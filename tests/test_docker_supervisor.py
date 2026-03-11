"""
Tests for Docker + Supervisor packaging (REQ-8).
"""
import os


def test_supervisor_conf_exists():
    """REQ-8: Supervisor config must exist and define both pipelines."""
    path = os.path.join(os.path.dirname(__file__), "..", "supervisor.conf")
    assert os.path.exists(path), "supervisor.conf not found"
    content = open(path).read()
    assert "[program:fast_pipeline]" in content
    assert "[program:retry_pipeline]" in content
    assert "fast_pipeline.py" in content
    assert "retry_pipeline.py" in content


def test_dockerfile_exists_and_copies_required_files():
    """REQ-8: Dockerfile must copy both pipeline scripts and supervisor config."""
    path = os.path.join(os.path.dirname(__file__), "..", "Dockerfile")
    assert os.path.exists(path), "Dockerfile not found"
    content = open(path).read()
    assert "fast_pipeline.py" in content
    assert "retry_pipeline.py" in content
    assert "supervisor.conf" in content
    assert "supervisord" in content or "supervisor" in content


def test_dockerfile_runs_supervisor_as_entrypoint():
    """REQ-8: CMD must run supervisord to start both pipelines."""
    path = os.path.join(os.path.dirname(__file__), "..", "Dockerfile")
    content = open(path).read()
    assert "supervisord" in content.lower()
    assert "CMD" in content
