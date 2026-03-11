"""
PIPELINE B — RETRY PIPELINE
Purpose  : Second-chance processing for records where RETRY = 1
Timeout  : 15 000 ms per URL
Workers  : 5   (env: RETRY_NUM_WORKERS)
Batch    : 10  (env: RETRY_BATCH_SIZE)

Bugs fixed vs previous version:
  BUG-2  8 except blocks but only 4 COUNTER_EXTRACT resets → every path now resets
  BUG-4  retry pipeline set RETRY=1 on its OWN failures, re-queuing endlessly → removed
  BUG-5  batch-level except didn't bulk-release committed locks → bulk release added
  BUG-7  row-not-found returned without releasing lock → now resets
  BUG-8  (same as BUG-7 in retry context)
  BUG-9  single cursor shared across URLs → each URL gets its own connection+cursor
  BUG-13 hourly reset used ARTICLEDATE → uses to_timestamp(COUNTER_EXTRACT)
  WARN-2 stagger sleep was after all submits → moved inside loop
  WARN-3 fetchCanonicalLinkWithConsent had no timeout on page.goto → fixed in that file
"""

import concurrent.futures
import threading
import time
import psycopg2
from psycopg2 import sql
import psycopg2.extras
import sys
import os
import re
import asyncio
import tldextract
from datetime import datetime, timezone
from database import localConnection
from fetchCanonicalLinkWithConsent import process_urls_in_batches

# ─────────────────────────────────────────────────────────────
# LOGGING  (REQ-9)
# ─────────────────────────────────────────────────────────────

class DualLogger:
    def __init__(self, pipeline_tag: str = "PIPELINE-B"):
        self._tag  = pipeline_tag
        self._lock = threading.Lock()
        logs_dir   = os.path.join(os.path.expanduser("~"), "log")
        os.makedirs(logs_dir, exist_ok=True)
        self.file_name = os.path.join(
            logs_dir, f"Google2C_{datetime.now().strftime('%Y%m%d')}.log"
        )

    def _log(self, level: str, message: str):
        ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{self._tag}] [{level}] {message}\n"
        with self._lock:
            with open(self.file_name, "a", encoding="utf-8") as f:
                f.write(line)
        sys.__stdout__.write(line)
        sys.__stdout__.flush()

    def write(self, message: str):
        if message.strip():
            self._log("PRINT", message.rstrip())
    def flush(self):
        sys.__stdout__.flush()

    def info(self,    msg): self._log("INFO",            msg)
    def success(self, msg): self._log("SUCCESS",         msg)
    def failure(self, msg): self._log("FAILURE",         msg)
    def timeout(self, msg): self._log("TIMEOUT",         msg)
    def retry(self,   msg): self._log("RETRY TRIGGERED", msg)


logger = DualLogger("PIPELINE-B")
sys.stdout = logger

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

PIPELINE_TIMEOUT_S    = float(os.getenv("RETRY_TIMEOUT_MS",  "15000")) / 1000.0
NUM_WORKERS           = int(os.getenv("RETRY_NUM_WORKERS",   "5"))
BATCH_SIZE            = int(os.getenv("RETRY_BATCH_SIZE",    "10"))
WORKER_STAGGER_S      = float(os.getenv("RETRY_STAGGER_S",  "1.0"))
HOURLY_RESET_INTERVAL = 3600

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def get_lock_value() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def get_main_domain(url: str) -> str:
    e = tldextract.extract(url)
    return f"{e.domain}.{e.suffix}"


def _safe_release(rss_url: str, extra_cols: str = "") -> None:
    """
    REQ-10: Fresh connection to release lock, bypassing any aborted transaction.
    Pipeline B on failure does NOT set RETRY=1 again (REQ-5).
    """
    try:
        c = localConnection()
        try:
            extra = f", {extra_cols}" if extra_cols else ""
            with c.cursor() as cur:
                cur.execute(
                    f"""UPDATE public."GOOGLE_SEARCH_LINK"
                        SET    "ISERROR"         = B'1',
                               "COUNTER_EXTRACT" = NULL{extra}
                        WHERE  "LINK" = %s""",
                    [rss_url]
                )
                c.commit()
        finally:
            c.close()
    except Exception as err:
        logger.failure(f"_safe_release failed for {rss_url}: {err}")


# ─────────────────────────────────────────────────────────────
# HOURLY STALE-LOCK RESET  (REQ-6)
# ─────────────────────────────────────────────────────────────

def hourly_reset() -> None:
    """
    BUG-13 FIX: compare lock acquisition time via to_timestamp(COUNTER_EXTRACT),
    NOT publish date via ARTICLEDATE.
    """
    try:
        conn = localConnection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE "GOOGLE_SEARCH_LINK"
                    SET    "COUNTER_EXTRACT" = NULL
                    WHERE  "COUNTER_EXTRACT" IS NOT NULL
                    AND    now() - to_timestamp("COUNTER_EXTRACT") > interval '1 hour'
                """)
                n = cur.rowcount
                conn.commit()
            logger.info(f"Hourly reset: released {n} stale lock(s)")
        finally:
            conn.close()
    except Exception as e:
        logger.failure(f"Hourly reset error: {e}")


def _hourly_reset_loop():
    while True:
        time.sleep(HOURLY_RESET_INTERVAL)
        hourly_reset()


# ─────────────────────────────────────────────────────────────
# PER-URL PROCESSOR  (REQ-2, REQ-5, REQ-9, REQ-10)
# ─────────────────────────────────────────────────────────────

def _process_single_url(rss_url: str) -> None:
    """
    BUG-9 FIX: own connection + cursor per URL.

    REQ-5: Pipeline B NEVER sets RETRY=1 again on failure.
    On failure it sets ISERROR=1, COUNTER_EXTRACT=NULL and leaves RETRY=1
    (already set by Pipeline A) so operations can manually inspect.

    REQ-10: Every exit path commits COUNTER_EXTRACT=NULL.
    """
    conn   = None
    cursor = None
    try:
        conn   = localConnection()
        cursor = conn.cursor()

        # ── 1. Fetch row ──────────────────────────────────────
        cursor.execute(
            """SELECT "LINK", "PUBLICATION", "ARTICLEDATE", "KEYWORD"
               FROM   public."GOOGLE_SEARCH_LINK"
               WHERE  "LINK" = %s
               ORDER  BY "ARTICLEDATE" DESC
               LIMIT  1""",
            [rss_url]
        )
        row = cursor.fetchone()

        # BUG-8 FIX: row-not-found releases lock (previously just returned)
        if not row:
            cursor.execute(
                """UPDATE public."GOOGLE_SEARCH_LINK"
                   SET    "ISERROR"         = B'1',
                          "COUNTER_EXTRACT" = NULL
                   WHERE  "LINK" = %s""",
                [rss_url]
            )
            conn.commit()
            logger.failure(f"Row not found — lock released | {rss_url}")
            return

        _, publication, article_date, keyword = row
        if not keyword:
            keyword = " "

        # ── 2. Fetch with longer timeout (consent-aware browser) ──
        original_link = None
        timed_out     = False
        try:
            original_link = asyncio.run(
                asyncio.wait_for(
                    process_urls_in_batches([{"rss": rss_url, "publication": publication}]),
                    timeout=PIPELINE_TIMEOUT_S
                )
            )
        except asyncio.TimeoutError:
            timed_out = True
            logger.timeout(
                f"TIMEOUT after {PIPELINE_TIMEOUT_S*1000:.0f}ms | {rss_url}"
            )
        except Exception as fetch_err:
            logger.failure(f"Fetch exception | {rss_url} | {fetch_err}")

        # ── 3a. SUCCESS ───────────────────────────────────────
        if (not timed_out) and original_link and _is_valid_canonical(original_link, publication):
            _insert_canonical(cursor, conn, original_link, publication, article_date, keyword)
            cursor.execute(
                """UPDATE public."GOOGLE_SEARCH_LINK"
                   SET    "CANONICAL_LINK"  = %s,
                          "ISERROR"         = B'0',
                          "RETRY"           = NULL,
                          "COUNTER_EXTRACT" = NULL
                   WHERE  "LINK" = %s""",
                [original_link, rss_url]
            )
            conn.commit()
            logger.success(f"PROCESS SUCCESS (retry resolved) | {rss_url} → {original_link}")

        # ── 3b. FINAL FAILURE  (REQ-5: do NOT set RETRY=1 again) ──
        else:
            reason = "TIMEOUT" if timed_out else "no valid canonical link"
            cursor.execute(
                """UPDATE public."GOOGLE_SEARCH_LINK"
                   SET    "ISERROR"         = B'1',
                          "COUNTER_EXTRACT" = NULL
                   WHERE  "LINK" = %s""",
                [rss_url]
            )
            conn.commit()
            logger.failure(
                f"PROCESS FAILURE (retry exhausted — {reason}) | {rss_url}"
            )

    except Exception as e:
        logger.failure(f"PROCESS FAILURE (exception) | {rss_url} | {e}")
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        # REQ-5: do NOT add RETRY=1 here either
        _safe_release(rss_url)

    finally:
        for obj in (cursor, conn):
            try:
                if obj:
                    obj.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
# BATCH LOCK + DISPATCH
# ─────────────────────────────────────────────────────────────

def process_batch(batch_size: int) -> None:
    while True:
        conn   = localConnection()
        cursor = conn.cursor()
        locked_urls: list = []

        try:
            lock_val = get_lock_value()
            cursor.execute(
                sql.SQL("""
                    UPDATE "GOOGLE_SEARCH_LINK"
                    SET    "COUNTER_EXTRACT" = %s
                    WHERE  ctid IN (
                        SELECT ctid
                        FROM   "GOOGLE_SEARCH_LINK"
                        WHERE  "COUNTER_EXTRACT" IS NULL
                        AND    "RETRY"       = B'1'
                        AND    "PUBLICATION" <> 'msn.com'
                        ORDER  BY "ARTICLEDATE" DESC
                        FOR UPDATE SKIP LOCKED
                        LIMIT  %s
                    )
                    RETURNING "LINK"
                """),
                [lock_val, batch_size]
            )
            locked_urls = [r[0] for r in cursor.fetchall()]
            conn.commit()
        except Exception as lock_err:
            logger.failure(f"Lock acquisition failed: {lock_err}")
            try:
                conn.rollback()
            except Exception:
                pass
            break
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        if not locked_urls:
            logger.info("No retry records — worker exiting")
            break

        logger.info(
            f"Locked {len(locked_urls)} retry row(s) | lock={lock_val} | pid={os.getpid()}"
        )

        processed = set()
        try:
            for rss_url in locked_urls:
                _process_single_url(rss_url)
                processed.add(rss_url)
        except Exception as loop_err:
            logger.failure(f"Unhandled loop error: {loop_err}")
            remaining = [u for u in locked_urls if u not in processed]
            if remaining:
                logger.failure(f"Bulk-releasing {len(remaining)} unprocessed locked rows")
                try:
                    fb = localConnection()
                    with fb.cursor() as cur:
                        cur.execute(
                            """UPDATE public."GOOGLE_SEARCH_LINK"
                               SET    "ISERROR"         = B'1',
                                      "COUNTER_EXTRACT" = NULL
                               WHERE  "LINK" = ANY(%s)
                               AND    "COUNTER_EXTRACT" IS NOT NULL""",
                            [remaining]
                        )
                        fb.commit()
                    fb.close()
                    logger.failure(f"Bulk released {len(remaining)} rows (retry exhausted)")
                except Exception as bulk_err:
                    logger.failure(f"Bulk release failed: {bulk_err}")


# ─────────────────────────────────────────────────────────────
# CANONICAL VALIDATION + INSERT
# ─────────────────────────────────────────────────────────────

def _is_valid_canonical(link: str, publication: str) -> bool:
    if not link:
        return False
    if link.rstrip("/") in ("https://www.msn.com/en-ca", "https://www.msn.com"):
        return False
    return bool(re.search(re.escape(get_main_domain(link)), publication))


def _insert_canonical(cursor, conn, original_link, publication, article_date, keyword):
    psycopg2.extras.execute_values(
        cursor,
        """INSERT INTO public."ALL_SEARCH_LINK" (
               "LINK", "SOURCE", "PUBLICATION", "ARTICLEDATE",
               "KEYWORD", "PROCESSED", "PROCESSSTATUS", "PROCESSBATCH"
           ) VALUES %s
           ON CONFLICT ("LINK", "SOURCE") DO NOTHING""",
        [(original_link, "GoogleInsert", publication, article_date, keyword, None, "New", None)]
    )
    conn.commit()


# ─────────────────────────────────────────────────────────────
# PIPELINE RUNNER  (REQ-7, REQ-8)
# ─────────────────────────────────────────────────────────────

def run_pipeline(num_workers: int, batch_size: int) -> None:
    logger.info(
        f"Pipeline B starting | workers={num_workers} | "
        f"batch={batch_size} | timeout={PIPELINE_TIMEOUT_S*1000:.0f}ms"
    )

    threading.Thread(
        target=_hourly_reset_loop, daemon=True, name="hourly-reset-B"
    ).start()

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as ex:
        futures = []
        for i in range(num_workers):
            futures.append(ex.submit(process_batch, batch_size))
            if i < num_workers - 1:
                time.sleep(WORKER_STAGGER_S)

        for fut in concurrent.futures.as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                logger.failure(f"Worker process raised: {e}")

    logger.info("Pipeline B: all workers finished")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    while True:
        try:
            run_pipeline(num_workers=NUM_WORKERS, batch_size=BATCH_SIZE)
        except Exception as e:
            logger.failure(f"Pipeline B top-level crash: {e}")
        logger.info("Pipeline B cycle complete — restarting in 2s")
        time.sleep(2)
