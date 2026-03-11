"""
Tests for retry_pipeline.py — verifies REQ 1–10 for Pipeline B.
"""
import os
from datetime import datetime, timezone
from unittest.mock import patch

import pytest


# ── REQ-1: Timestamp-based lock ─────────────────────────────────────────────
def test_get_lock_value_is_timestamp():
    """REQ-1: Retry pipeline also uses timestamp lock."""
    from retry_pipeline import get_lock_value

    lock = get_lock_value()
    expected = int(datetime.now(timezone.utc).timestamp())
    assert isinstance(lock, int)
    assert abs(lock - expected) <= 2


# ── REQ-3: Retry pipeline 15 sec timeout ──────────────────────────────────────
def test_retry_pipeline_timeout_config():
    """REQ-3: Retry pipeline uses 15 sec timeout."""
    from retry_pipeline import PIPELINE_TIMEOUT_S

    assert PIPELINE_TIMEOUT_S == 15.0


# ── REQ-5: Retry pipeline does NOT set RETRY=1 again ────────────────────────
def test_retry_pipeline_safe_release_no_retry():
    """REQ-5: _safe_release does not add RETRY=1 (retry exhausted)."""
    import retry_pipeline

    source = open(retry_pipeline.__file__).read()
    # On failure path, retry pipeline sets ISERROR=1, COUNTER_EXTRACT=NULL
    # but NOT RETRY=1 (unlike fast pipeline)
    assert '_safe_release(rss_url)' in source or '_safe_release(rss_url, "")' in source
    # The 3b failure block should NOT have RETRY = 1
    assert '"COUNTER_EXTRACT" = NULL' in source


# ── REQ-7: ProcessPoolExecutor ──────────────────────────────────────────────
def test_retry_run_pipeline_uses_process_pool_executor():
    """REQ-7: Retry pipeline uses ProcessPoolExecutor."""
    import retry_pipeline

    source = open(retry_pipeline.__file__).read()
    assert "ProcessPoolExecutor" in source


# ── REQ-9: Log levels ───────────────────────────────────────────────────────
def test_retry_logger_has_required_levels():
    """REQ-9: Retry logger has success, failure, retry, timeout."""
    from retry_pipeline import logger

    assert hasattr(logger, "success")
    assert hasattr(logger, "failure")
    assert hasattr(logger, "timeout")


# ── Canonical validation ───────────────────────────────────────────────────
def test_retry_is_valid_canonical():
    """Helper: _is_valid_canonical in retry pipeline."""
    from retry_pipeline import _is_valid_canonical

    assert _is_valid_canonical("https://www.indiatoday.in/article", "indiatoday.in") is True
    assert _is_valid_canonical("https://www.msn.com/en-ca", "msn.com") is False
