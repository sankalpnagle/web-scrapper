"""
fetchCanonicalLinkWithConsent.py  —  Pipeline B fetcher

Uses googlenewsdecoder library instead of Playwright.
Decodes Google News URLs to their original article URLs via HTTP (no browser).
Publication validation is handled by the pipeline's _is_valid_canonical.
"""

import asyncio
import os
from googlenewsdecoder import gnewsdecoder

# Optional: proxy for rate limiting (env: RETRY_PROXY)
_PROXY = os.getenv("RETRY_PROXY", None)
_INTERVAL = float(os.getenv("RETRY_DECODER_INTERVAL", "0"))


def _decode_url(rss_url: str) -> str | None:
    """Decode Google News URL to original article URL. Returns None on failure."""
    try:
        result = gnewsdecoder(rss_url, interval=_INTERVAL, proxy=_PROXY)
        if result.get("status") and result.get("decoded_url"):
            return result["decoded_url"]
    except Exception as e:
        print(f"Error decoding URL {rss_url}: {e}")
    return None


async def process_urls_in_batches(urls: list):
    """
    Decode Google News URLs using googlenewsdecoder.
    Returns the decoded canonical URL (publication check done by pipeline).
    """
    for item in urls:
        rss_url = item["rss"]

        # Run blocking gnewsdecoder in thread pool (async-friendly)
        decoded = await asyncio.to_thread(_decode_url, rss_url)

        if decoded:
            return decoded

    return None
