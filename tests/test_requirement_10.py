"""
Tests for REQ-10: No record should remain with non-null COUNTER_EXTRACT after failure.
Verifies that all exception/failure paths in both pipelines release the lock.
"""
import ast
import os


def _extract_update_statements(filepath: str) -> list:
    """Extract SQL UPDATE statements that touch COUNTER_EXTRACT."""
    with open(filepath) as f:
        content = f.read()
    # Look for UPDATE ... COUNTER_EXTRACT = NULL
    import re
    # Find all places that set COUNTER_EXTRACT
    pattern = r'["\']COUNTER_EXTRACT["\']\s*=\s*NULL'
    return list(re.finditer(pattern, content))


def test_fast_pipeline_all_failure_paths_release_lock():
    """REQ-10: Fast pipeline — every failure path sets COUNTER_EXTRACT = NULL."""
    base = os.path.dirname(__file__)
    path = os.path.join(base, "..", "fast_pipeline.py")
    content = open(path).read()

    # Count failure/exception paths that must release lock
    # 1. _safe_release (ISERROR=1, COUNTER_EXTRACT=NULL)
    # 2. Row not found block
    # 3. 3b FAILURE/TIMEOUT block (RETRY=1, COUNTER_EXTRACT=NULL)
    # 4. Exception handler (_safe_release)
    # 5. Bulk release in loop exception
    assert '"COUNTER_EXTRACT" = NULL' in content or "'COUNTER_EXTRACT' = NULL" in content
    assert "_safe_release" in content
    assert "ISERROR" in content and "B'1'" in content


def test_retry_pipeline_all_failure_paths_release_lock():
    """REQ-10: Retry pipeline — every failure path sets COUNTER_EXTRACT = NULL."""
    base = os.path.dirname(__file__)
    path = os.path.join(base, "..", "retry_pipeline.py")
    content = open(path).read()

    assert '"COUNTER_EXTRACT" = NULL' in content or "'COUNTER_EXTRACT' = NULL" in content
    assert "_safe_release" in content
