"""
Unit tests for fetchcanocaialLink.py - Canonical link extraction via Playwright.
"""
import pytest


def test_process_urls_returns_none_when_no_match():
    """process_urls_in_batches returns None when all collected URLs are from google.com."""
    from unittest.mock import AsyncMock, patch, MagicMock

    async def mock_get_urls(url):
        # Simulate only Google-internal redirects — nothing usable
        return ["https://news.google.com/redirect", "https://google.com/other"]

    with patch("fetchcanocaialLink.get_urls", side_effect=mock_get_urls):
        from fetchcanocaialLink import process_urls_in_batches

        urls = [{"rss": "https://example.com/rss", "publication": "indiatoday.in"}]
        result = __import__("asyncio").run(process_urls_in_batches(urls))
        assert result is None


def test_process_urls_returns_match():
    """process_urls_in_batches returns URL when publication matches."""
    from unittest.mock import patch

    async def mock_get_urls(url):
        return [
            "https://google.com/redirect",
            "https://www.indiatoday.in/india/story-123",
            "https://other.com/page",
        ]

    with patch("fetchcanocaialLink.get_urls", side_effect=mock_get_urls):
        from fetchcanocaialLink import process_urls_in_batches

        urls = [{"rss": "https://example.com/rss", "publication": "indiatoday.in"}]
        result = __import__("asyncio").run(process_urls_in_batches(urls))
        assert result == "https://www.indiatoday.in/india/story-123"
