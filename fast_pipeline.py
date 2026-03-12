"""
PIPELINE A — FAST PIPELINE
Purpose  : Primary processing of fresh RSS records
Timeout  : 3 000 ms per URL (env: FAST_TIMEOUT_MS)
Workers  : 15  (env: FAST_NUM_WORKERS)
Batch    : 25  (env: FAST_BATCH_SIZE)

Bugs fixed vs previous version:
  BUG-1  datetime.utcnow() deprecated → datetime.now(timezone.utc)
  BUG-2  8 except blocks but only 4 COUNTER_EXTRACT resets → every path now resets
  BUG-5  batch-level except didn't release already-committed locks → bulk release added
  BUG-7  row-not-found returned without releasing lock → now resets + sets RETRY=1
  BUG-9  single cursor shared across URLs → each URL gets its own connection+cursor
  BUG-12 hourly reset used ARTICLEDATE (publish date) not lock time → uses to_timestamp(COUNTER_EXTRACT)
  WARN-1 stagger sleep was after all submits → moved inside loop
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
from fetchcanocaialLink import process_urls_in_batches

# ─────────────────────────────────────────────────────────────
# LOGGING  (REQ-9)
# ─────────────────────────────────────────────────────────────

class DualLogger:
    """Thread-safe logger — file (append) + stdout, with typed event levels."""

    def __init__(self, pipeline_tag: str = "PIPELINE-A"):
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

    # sys.stdout interface
    def write(self, message: str):
        if message.strip():
            self._log("PRINT", message.rstrip())
    def flush(self):
        sys.__stdout__.flush()

    # Semantic log methods  (REQ-9 explicit tokens)
    def info(self,    msg): self._log("INFO",            msg)
    def success(self, msg): self._log("SUCCESS",         msg)
    def failure(self, msg): self._log("FAILURE",         msg)
    def timeout(self, msg): self._log("TIMEOUT",         msg)   # distinct from FAILURE
    def retry(self,   msg): self._log("RETRY TRIGGERED", msg)


logger = DualLogger("PIPELINE-A")
sys.stdout = logger

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

PIPELINE_TIMEOUT_S    = float(os.getenv("FAST_TIMEOUT_MS",  "3000")) / 1000.0
NUM_WORKERS           = int(os.getenv("FAST_NUM_WORKERS",   "15"))
BATCH_SIZE            = int(os.getenv("FAST_BATCH_SIZE",    "25"))
WORKER_STAGGER_S      = float(os.getenv("FAST_STAGGER_S",  "0.5"))
HOURLY_RESET_INTERVAL = 3600

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def get_lock_value() -> int:
    """REQ-1: Unix timestamp integer — also encodes when the lock was set."""
    return int(datetime.now(timezone.utc).timestamp())


def get_main_domain(url: str) -> str:
    e = tldextract.extract(url)
    return f"{e.domain}.{e.suffix}"


def _safe_release(rss_url: str, extra_cols: str = "") -> None:
    """
    REQ-10: Open a BRAND-NEW connection to release the lock.
    The caller's connection may be in an aborted transaction state
    (psycopg2 InFailedSqlTransaction) and cannot run more SQL.
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
    REQ-6 BUG-12 FIX:
    Previous code used  now() - ARTICLEDATE > '1 hour'
    That compares the *publish* date, not the lock time.
    An article published yesterday (ARTICLEDATE = 24 hours ago) would have
    its lock released immediately, even if it was locked only 5 seconds ago.

    Fix: COUNTER_EXTRACT IS a Unix timestamp (set by get_lock_value()).
    Use to_timestamp(COUNTER_EXTRACT) to get the actual lock acquisition time.
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
# PER-URL PROCESSOR  (REQ-2, REQ-4, REQ-9, REQ-10)
# ─────────────────────────────────────────────────────────────

def _process_single_url(rss_url: str) -> None:
    """
    BUG-9 FIX: Each URL gets its own dedicated connection + cursor.
    A psycopg2 InFailedSqlTransaction on URL #3 can no longer poison
    URLs #4-25 in the same batch.

    REQ-10: Every exit path (success, failure, timeout, row-not-found,
    any unhandled exception) commits COUNTER_EXTRACT = NULL.
    """
    conn   = None
    cursor = None
    try:
        conn   = localConnection()
        cursor = conn.cursor()

        # ── 1. Fetch the full row ─────────────────────────────
        cursor.execute(
            """SELECT "LINK", "PUBLICATION", "ARTICLEDATE", "KEYWORD"
               FROM   public."GOOGLE_SEARCH_LINK"
               WHERE  "LINK" = %s
               ORDER  BY "ARTICLEDATE" DESC
               LIMIT  1""",
            [rss_url]
        )
        row = cursor.fetchone()

        # BUG-7 FIX: row-not-found MUST release the lock (previously just returned)
        if not row:
            cursor.execute(
                """UPDATE public."GOOGLE_SEARCH_LINK"
                   SET    "ISERROR"         = B'1',
                          "RETRY"           = B'1',
                          "COUNTER_EXTRACT" = NULL
                   WHERE  "LINK" = %s""",
                [rss_url]
            )
            conn.commit()
            logger.failure(f"Row not found — lock released, RETRY=1 | {rss_url}")
            logger.retry(f"RETRY TRIGGERED (row not found) | {rss_url}")
            return

        _, publication, article_date, keyword = row
        if not keyword:
            keyword = " "

        logger.info(
            f"PROCESSING | pub={publication} | keyword={keyword.strip()[:60]} | "
            f"date={article_date} | url={rss_url}"
        )

        # ── 2. Fetch canonical link with hard timeout ─────────
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
            logger.success(
                f"PROCESS SUCCESS | pub={publication} | "
                f"rss={rss_url} | canonical={original_link} | "
                f"canonical_len={len(original_link)}"
            )

        # ── 3b. FAILURE / TIMEOUT  (REQ-4) ───────────────────
        else:
            reason = "TIMEOUT" if timed_out else (
                "link too long" if (original_link and len(original_link) > MAX_LINK_LENGTH)
                else "no valid canonical link"
            )
            cursor.execute(
                """UPDATE public."GOOGLE_SEARCH_LINK"
                   SET    "ISERROR"         = B'1',
                          "RETRY"           = B'1',
                          "COUNTER_EXTRACT" = NULL
                   WHERE  "LINK" = %s""",
                [rss_url]
            )
            conn.commit()
            decoded_info = (
                f" | decoded={original_link} | decoded_len={len(original_link)}"
                if original_link else " | decoded=None"
            )
            logger.failure(
                f"PROCESS FAILURE ({reason}) | pub={publication} | "
                f"rss={rss_url}{decoded_info}"
            )
            logger.retry(f"RETRY TRIGGERED ({reason}) | {rss_url}")

    except Exception as e:
        # ── 3c. UNEXPECTED EXCEPTION  (REQ-2, REQ-10) ────────
        logger.failure(f"PROCESS FAILURE (exception) | {rss_url} | {e}")
        # Rollback the aborted txn first, then use a fresh connection to release
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        _safe_release(rss_url, "\"RETRY\" = B'1'")
        logger.retry(f"RETRY TRIGGERED (exception) | {rss_url}")

    finally:
        for obj in (cursor, conn):
            try:
                if obj:
                    obj.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
# BATCH LOCK + DISPATCH  (REQ-2 BUG-5 fix)
# ─────────────────────────────────────────────────────────────

def process_batch(batch_size: int) -> None:
    """
    BUG-5 FIX:
    The lock UPDATE is committed before per-URL processing begins.
    If the process crashes between the commit and the processing loop,
    locked rows would be stuck until the hourly reset.
    Fix: on any unhandled exception in the URL loop, do an immediate
    bulk COUNTER_EXTRACT=NULL reset for all rows in the batch.
    """
    while True:
        conn   = localConnection()
        cursor = conn.cursor()
        locked_urls: list = []

        # ── Step 1: acquire batch lock ────────────────────────
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
                        AND    ("ISERROR" = B'0' OR "ISERROR" IS NULL)
                        AND    "RETRY"       IS NULL
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
            break   # nothing was committed — safe to just exit
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
            logger.info("No unprocessed records — worker exiting")
            break

        logger.info(
            f"Locked {len(locked_urls)} row(s) | lock={lock_val} | pid={os.getpid()}"
        )

        # ── Step 2: process each URL (own conn per URL) ───────
        processed = set()
        try:
            for rss_url in locked_urls:
                _process_single_url(rss_url)
                processed.add(rss_url)
        except Exception as loop_err:
            # Should never reach here (each URL has its own try/except)
            # but as an absolute safety net, bulk-release anything not yet processed
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
                                      "RETRY"           = B'1',
                                      "COUNTER_EXTRACT" = NULL
                               WHERE  "LINK" = ANY(%s)
                               AND    "COUNTER_EXTRACT" IS NOT NULL""",
                            [remaining]
                        )
                        fb.commit()
                    fb.close()
                    logger.retry(f"Bulk RETRY TRIGGERED for {len(remaining)} rows")
                except Exception as bulk_err:
                    logger.failure(f"Bulk release failed: {bulk_err}")


# ─────────────────────────────────────────────────────────────
# CANONICAL VALIDATION + DB INSERT
# ─────────────────────────────────────────────────────────────

MAX_LINK_LENGTH = 255  # ALL_SEARCH_LINK.LINK, GOOGLE_SEARCH_LINK.CANONICAL_LINK

def _is_valid_canonical(link: str, publication: str) -> bool:
    if not link:
        return False
    if len(link) > MAX_LINK_LENGTH:
        return False
    if link.rstrip("/") in (
        "https://www.msn.com/en-ca", "https://www.msn.com",
        "http://www.msn.com/en-ca",  "http://www.msn.com",
    ):
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
    # No commit here — caller commits INSERT + UPDATE GOOGLE_SEARCH_LINK atomically


# ─────────────────────────────────────────────────────────────
# PIPELINE RUNNER  (REQ-7, REQ-8)
# ─────────────────────────────────────────────────────────────

def run_pipeline(num_workers: int, batch_size: int) -> None:
    logger.info(
        f"Pipeline A starting | workers={num_workers} | "
        f"batch={batch_size} | timeout={PIPELINE_TIMEOUT_S*1000:.0f}ms"
    )

    # Start hourly stale-lock reset in background daemon thread
    threading.Thread(
        target=_hourly_reset_loop, daemon=True, name="hourly-reset-A"
    ).start()

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as ex:
        futures = []
        for i in range(num_workers):
            futures.append(ex.submit(process_batch, batch_size))
            # REQ-7 WARN-1 FIX: sleep INSIDE the loop to actually stagger workers
            if i < num_workers - 1:
                time.sleep(WORKER_STAGGER_S)

        for fut in concurrent.futures.as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                logger.failure(f"Worker process raised: {e}")

    logger.info("Pipeline A: all workers finished")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    while True:
        try:
            run_pipeline(num_workers=NUM_WORKERS, batch_size=BATCH_SIZE)
        except Exception as e:
            logger.failure(f"Pipeline A top-level crash: {e}")
        logger.info("Pipeline A cycle complete — restarting in 2s")
        time.sleep(2)
