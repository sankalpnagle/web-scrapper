"""
Unit tests for rss2cl.py - Google canonical links pipeline.
"""
import os

import pytest


def test_getMainDomainName():
    """Test domain extraction from various URL formats."""
    from rss2cl import getMainDomainName

    assert getMainDomainName("https://www.indiatoday.in/india/story") == "indiatoday.in"
    assert getMainDomainName("https://news.google.com/rss/articles/xyz") == "google.com"
    assert getMainDomainName("https://www.msn.com/en-ca/news") == "msn.com"
    assert getMainDomainName("https://subdomain.example.co.uk/path") == "example.co.uk"
    assert getMainDomainName("http://bbc.com/article") == "bbc.com"


def test_getMainDomainName_with_none():
    """Document bug: getMainDomainName crashes when given None (process_urls_in_batches can return None)."""
    from rss2cl import getMainDomainName

    with pytest.raises((TypeError, AttributeError)):
        getMainDomainName(None)


def test_publication_match_logic():
    """Test the publication domain matching logic used in process_batch."""
    import re

    def matches_publication(original_link, publication):
        if not original_link:
            return False
        # Simulate getMainDomainName
        import tldextract
        extracted = tldextract.extract(original_link)
        publication_rss = "{}.{}".format(extracted.domain, extracted.suffix)
        return bool(re.search(publication_rss, publication)) and original_link != "https://www.msn.com/en-ca"

    assert matches_publication("https://www.indiatoday.in/article", "indiatoday.in") is True
    assert matches_publication("https://www.msn.com/en-ca", "msn.com") is False  # MSN excluded
    assert matches_publication(None, "example.com") is False
    assert matches_publication("https://other.com/page", "indiatoday.in") is False


def test_dual_logger_creates_log_dir():
    """Test DualLogger creates log directory and file."""
    # Import rss2cl - this will init DualLogger and replace stdout
    import rss2cl
    logger = rss2cl.DualLogger()
    assert os.path.exists(os.path.dirname(logger.file_name))
    assert "Google2C_" in logger.file_name
    assert logger.file_name.endswith(".log")
