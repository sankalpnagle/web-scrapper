# Google Canonical Links — Pipeline Documentation

## System Overview

Two pipelines run **simultaneously** inside a Docker container, managed by `supervisor`.  
Both share the same PostgreSQL table `GOOGLE_SEARCH_LINK` and write to the same daily log file.

```
┌─────────────────── Docker Container ───────────────────────┐
│                                                             │
│   supervisor                                                │
│   ├── fast_pipeline.py   ← Pipeline A (always running)     │
│   └── retry_pipeline.py  ← Pipeline B (always running)     │
│                                                             │
│   Both read/write:  PostgreSQL → GOOGLE_SEARCH_LINK         │
│   On success write: PostgreSQL → ALL_SEARCH_LINK            │
│   Both log to:      ~/log/Google2C_YYYYMMDD.log             │
└─────────────────────────────────────────────────────────────┘
```

---

## Configuration

### Pipeline A — Fast Pipeline

| Env Variable        | Default | Description                             |
| ------------------- | ------- | --------------------------------------- |
| `FAST_TIMEOUT_MS`   | `3000`  | Playwright `page.goto()` timeout (ms)   |
| `FAST_NUM_WORKERS`  | `15`    | Concurrent worker processes             |
| `FAST_BATCH_SIZE`   | `25`    | URLs locked per worker per cycle        |
| `FAST_STAGGER_S`    | `0.5`   | Delay between worker launches (seconds) |
| `FAST_POST_LOAD_MS` | `0`     | Extra wait after page load (ms)         |

> **Effective timeout:** `page.goto = 3000ms` + `asyncio.wait_for wrapper = 6000ms` (3s overhead for Chromium launch)

### Pipeline B — Retry Pipeline

| Env Variable                 | Default  | Description                                  |
| ---------------------------- | -------- | -------------------------------------------- |
| `RETRY_TIMEOUT_MS`           | `25000`  | asyncio.wait_for timeout per URL (ms)        |
| `RETRY_NUM_WORKERS`          | `5`      | Concurrent worker processes                  |
| `RETRY_BATCH_SIZE`           | `10`     | URLs locked per worker per cycle             |
| `RETRY_STAGGER_S`            | `1.0`    | Delay between worker launches (seconds)      |
| `RETRY_PROXY`                | _(none)_ | Optional HTTP proxy for decoder              |
| `RETRY_DECODER_INTERVAL`     | `0`      | Interval between decoder calls (seconds)     |
| `RETRY_RATE_LIMIT_THRESHOLD` | `5`      | Consecutive failures before rate-limit sleep |
| `RETRY_RATE_LIMIT_SLEEP_S`   | `7200`   | Sleep duration when rate-limited (2 hours)   |

---

## Database Tables

### `GOOGLE_SEARCH_LINK` — Input / Status

| Column            | Type | Meaning                                               |
| ----------------- | ---- | ----------------------------------------------------- |
| `LINK`            | text | Google News RSS URL (primary input)                   |
| `PUBLICATION`     | text | e.g. `indiatoday.in`                                  |
| `KEYWORD`         | text | Search term                                           |
| `ARTICLEDATE`     | date | Article publish date                                  |
| `CANONICAL_LINK`  | text | Resolved real article URL (output)                    |
| `COUNTER_EXTRACT` | int  | Unix timestamp = distributed lock (`NULL` = free)     |
| `RETRY`           | bit  | `NULL`=fresh, `1`=failed once (queued for Pipeline B) |
| `ISERROR`         | bit  | `0`=ok, `1`=error/exhausted                           |

### `ALL_SEARCH_LINK` — Final Output

Receives a row on every **successful** canonical resolution:

| Column          | Value written   |
| --------------- | --------------- |
| `LINK`          | Canonical URL   |
| `SOURCE`        | `GoogleInsert`  |
| `PUBLICATION`   | From source row |
| `ARTICLEDATE`   | From source row |
| `KEYWORD`       | From source row |
| `PROCESSSTATUS` | `New`           |

---

## Record Lifecycle

```
DB row created
(RETRY=NULL, ISERROR=NULL, COUNTER_EXTRACT=NULL)
        │
        ▼
  Pipeline A picks it up
  (COUNTER_EXTRACT = lock timestamp)
        │
   ┌────┴────────────────────┐
SUCCESS                   FAILURE / TIMEOUT
   │                          │
CANONICAL_LINK set         ISERROR=1, RETRY=1
ISERROR=0                  COUNTER_EXTRACT=NULL
RETRY=NULL                      │
COUNTER_EXTRACT=NULL             ▼
   │                    Pipeline B picks it up
   │                    (COUNTER_EXTRACT = lock timestamp)
   │                         │
   │                    ┌────┴────────────────┐
   │                  SUCCESS             FAILURE
   │                     │                   │
   │               CANONICAL_LINK set    ISERROR=1
   │               ISERROR=0             RETRY=1 (stays, for inspection)
   │               RETRY=NULL            COUNTER_EXTRACT=NULL
   │               COUNTER_EXTRACT=NULL  (dead end — not re-queued)
   │
   ▼
ALL_SEARCH_LINK ← final destination for all successful canonical URLs
```

---

## Pipeline A — Fast Pipeline (`fast_pipeline.py`)

**Purpose:** Process all fresh unprocessed records as fast as possible.  
**Fetcher:** `fetchcanocaialLink.py` — Playwright Chromium browser, intercepts network redirects.

### Cycle (every 2 seconds)

```
run_pipeline()
  └── ProcessPoolExecutor (15 workers, staggered 0.5s apart)
        └── process_batch()  ← each worker runs this
              └── Loop:
                    1. Lock up to 25 rows (FOR UPDATE SKIP LOCKED)
                    2. Process each URL → _process_single_url()
                    3. If no rows → exit worker
```

### Lock Query Filter

```sql
WHERE  "COUNTER_EXTRACT" IS NULL
AND    ("ISERROR" = B'0' OR "ISERROR" IS NULL)
AND    "RETRY" IS NULL
AND    "PUBLICATION" <> 'msn.com'
ORDER  BY "ARTICLEDATE" DESC
FOR UPDATE SKIP LOCKED
LIMIT  25
```

### Per-URL Processing (`_process_single_url`)

1. `SELECT` full row from `GOOGLE_SEARCH_LINK`
2. Log: `PROCESSING | pub=... | keyword=... | date=... | url=...`
3. Launch Playwright Chromium (headless, `--no-sandbox`)
4. `page.goto(url, timeout=3000ms)` — intercept first non-google.com redirect
5. Wrap in `asyncio.wait_for(timeout=6s)`

**On SUCCESS:**

- Validate canonical: `len ≤ 255`, not bare `msn.com`, domain matches publication
- `INSERT` into `ALL_SEARCH_LINK`
- `UPDATE GOOGLE_SEARCH_LINK`: `CANONICAL_LINK`, `ISERROR=0`, `RETRY=NULL`, `COUNTER_EXTRACT=NULL`
- Log: `PROCESS SUCCESS | pub=... | rss=... | canonical=... | canonical_len=...`

**On FAILURE / TIMEOUT:**

- `UPDATE GOOGLE_SEARCH_LINK`: `ISERROR=1`, `RETRY=1`, `COUNTER_EXTRACT=NULL`
- Log: `PROCESS FAILURE (reason) | pub=... | rss=... | decoded=... | decoded_len=...`
- Log: `RETRY TRIGGERED (reason) | rss=...`

**On EXCEPTION:**

- Rollback, then `_safe_release()` on a **fresh** DB connection
- Sets `ISERROR=1`, `RETRY=1`, `COUNTER_EXTRACT=NULL`
- Log: `PROCESS FAILURE (exception) | ...` + `RETRY TRIGGERED (exception) | ...`

---

## Pipeline B — Retry Pipeline (`retry_pipeline.py`)

**Purpose:** Second chance for URLs Pipeline A couldn't resolve (`RETRY=1`).  
**Fetcher:** `fetchCanonicalLinkWithConsent.py` — HTTP-based `googlenewsdecoder` (no browser).

### Key Differences from Pipeline A

|                       | Pipeline A                    | Pipeline B                                  |
| --------------------- | ----------------------------- | ------------------------------------------- |
| Lock filter           | `RETRY IS NULL`               | `RETRY = 1`                                 |
| Timeout               | 3000ms goto / 6000ms wrapper  | 25000ms                                     |
| Workers               | 15                            | 5                                           |
| Batch size            | 25                            | 10                                          |
| On failure            | Sets `RETRY=1` (queues for B) | Does **NOT** set `RETRY=1` again            |
| Rate-limit protection | No                            | Yes — sleep 2h after 5 consecutive failures |

### Rate-Limit Protection

After `RETRY_RATE_LIMIT_THRESHOLD` (default 5) consecutive failures:

1. Release locks on all remaining unprocessed URLs in the batch (`COUNTER_EXTRACT=NULL`)  
   so other workers / next cycle can pick them up immediately
2. Sleep `RETRY_RATE_LIMIT_SLEEP_S` (default 7200s = 2 hours)
3. Resume — the `while True` outer loop re-locks a fresh batch

---

## Safety Mechanisms

| Mechanism                   | Purpose                                                                                                 |
| --------------------------- | ------------------------------------------------------------------------------------------------------- |
| `FOR UPDATE SKIP LOCKED`    | Prevents two workers ever locking the same row                                                          |
| `COUNTER_EXTRACT` (unix ts) | Distributed lock — cleared on every exit path without exception                                         |
| `_safe_release()`           | Opens a **fresh** DB connection to release lock even if main connection is in aborted transaction state |
| Hourly reset thread         | Every 60 min: clears `COUNTER_EXTRACT` for locks older than 1 hour (crashed worker cleanup)             |
| Bulk release on crash       | If batch loop crashes, all unprocessed URLs are immediately freed                                       |
| `--no-sandbox`              | Required for Chromium to run inside Docker                                                              |
| Atomic commit               | `INSERT ALL_SEARCH_LINK` + `UPDATE GOOGLE_SEARCH_LINK` committed together — no partial writes           |

---

## Logging

### Log Location

```
~/log/Google2C_YYYYMMDD.log
```

Inside Docker: `/root/log/Google2C_YYYYMMDD.log`  
Mounted to host: `C:\logs\rss2cl\Google2C_YYYYMMDD.log`

### Log Format

```
[YYYY-MM-DD HH:MM:SS] [PIPELINE-A|PIPELINE-B] [LEVEL] message
```

### Log Levels

| Level             | When emitted                                             |
| ----------------- | -------------------------------------------------------- |
| `INFO`            | General events — cycle start, lock acquired, worker exit |
| `SUCCESS`         | `PROCESS SUCCESS` — canonical resolved and inserted      |
| `FAILURE`         | `PROCESS FAILURE` — all error/timeout/exception paths    |
| `TIMEOUT`         | URL timed out specifically                               |
| `RETRY TRIGGERED` | Row queued for Pipeline B                                |

### Example Log Lines

```
[2026-03-12 11:33:20] [PIPELINE-A] [INFO] Pipeline A starting | workers=15 | batch=25 | goto_timeout=3000ms | pipeline_timeout=6000ms
[2026-03-12 11:33:21] [PIPELINE-A] [INFO] Locked 25 row(s) | lock=1741779201 | pid=42
[2026-03-12 11:33:21] [PIPELINE-A] [INFO] PROCESSING | pub=indiatoday.in | keyword=india news | date=2026-03-12 | url=https://news.google.com/...
[2026-03-12 11:33:22] [PIPELINE-A] [SUCCESS] PROCESS SUCCESS | pub=indiatoday.in | rss=https://news.google.com/... | canonical=https://www.indiatoday.in/... | canonical_len=87
[2026-03-12 11:33:23] [PIPELINE-A] [TIMEOUT] TIMEOUT after 3000ms | pub=bbc.com | rss=https://news.google.com/...
[2026-03-12 11:33:23] [PIPELINE-A] [FAILURE] PROCESS FAILURE (TIMEOUT) | pub=bbc.com | rss=https://... | decoded=None
[2026-03-12 11:33:23] [PIPELINE-A] [RETRY TRIGGERED] RETRY TRIGGERED (TIMEOUT) | rss=https://...
[2026-03-12 11:33:25] [PIPELINE-B] [SUCCESS] PROCESS SUCCESS (retry resolved) | pub=bbc.com | rss=https://... | canonical=https://www.bbc.com/... | canonical_len=72
[2026-03-12 11:33:30] [PIPELINE-A] [INFO] Pipeline A: all workers finished
[2026-03-12 11:33:30] [PIPELINE-A] [INFO] Pipeline A cycle complete — restarting in 2s
```

---

## Docker Setup

### Build & Run

```powershell
# First run / rebuild after code changes
docker compose up -d --build

# Stop
docker compose down

# View live logs
Get-Content C:\logs\rss2cl\Google2C_20260312.log -Wait -Tail 50
```

### `docker-compose.yml` Summary

```yaml
services:
  rss2cl:
    build: .
    container_name: rss2cl
    restart: unless-stopped
    volumes:
      - C:/logs/rss2cl:/root/log
    environment:
      FAST_NUM_WORKERS: "15"
      FAST_BATCH_SIZE: "25"
      FAST_TIMEOUT_MS: "3000"
      FAST_STAGGER_S: "0.5"
      FAST_POST_LOAD_MS: "0"
      RETRY_NUM_WORKERS: "5"
      RETRY_BATCH_SIZE: "10"
      RETRY_TIMEOUT_MS: "25000"
      RETRY_STAGGER_S: "1.0"
      RETRY_RATE_LIMIT_THRESHOLD: "5"
      RETRY_RATE_LIMIT_SLEEP_S: "7200"
    healthcheck:
      test: supervisorctl status fast_pipeline retry_pipeline
      interval: 60s
      timeout: 10s
      retries: 3
```

### Check Container Status

```powershell
docker ps
docker logs rss2cl --tail 50
docker exec rss2cl supervisorctl status
```

---

## Time Estimates

Based on batch/worker/timeout configuration:

| Pipeline  | Records | Workers | Batch | Timeout | Est. Duration  |
| --------- | ------- | ------- | ----- | ------- | -------------- |
| A (fast)  | 697     | 15      | 25    | 3s      | ~4–5 minutes   |
| B (retry) | 609     | 5       | 10    | 25s     | ~55–65 minutes |

Both run in parallel — total wall-clock time is driven by Pipeline B (~1 hour).

---

## Useful DB Queries

```sql
-- How many records are pending for Pipeline A?
SELECT COUNT(*) FROM "GOOGLE_SEARCH_LINK"
WHERE "COUNTER_EXTRACT" IS NULL
  AND ("ISERROR" = B'0' OR "ISERROR" IS NULL)
  AND "RETRY" IS NULL;

-- How many records are queued for Pipeline B?
SELECT COUNT(*) FROM "GOOGLE_SEARCH_LINK"
WHERE "RETRY" = B'1';

-- How many are permanently failed?
SELECT COUNT(*) FROM "GOOGLE_SEARCH_LINK"
WHERE "ISERROR" = B'1' AND "RETRY" = B'1'
  AND "COUNTER_EXTRACT" IS NULL;

-- How many succeeded?
SELECT COUNT(*) FROM "GOOGLE_SEARCH_LINK"
WHERE "CANONICAL_LINK" IS NOT NULL AND "ISERROR" = B'0';

-- Any currently locked (in-progress)?
SELECT COUNT(*) FROM "GOOGLE_SEARCH_LINK"
WHERE "COUNTER_EXTRACT" IS NOT NULL;

-- Stuck locks (older than 1 hour — should be auto-cleared by hourly reset):
SELECT COUNT(*) FROM "GOOGLE_SEARCH_LINK"
WHERE "COUNTER_EXTRACT" IS NOT NULL
  AND now() - to_timestamp("COUNTER_EXTRACT") > interval '1 hour';
```
