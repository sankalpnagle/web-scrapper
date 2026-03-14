"""
fetchCanonicalLinkWithConsent.py  —  Pipeline B fetcher

Primary:  googlenewsdecoder (HTTP, no browser)
Fallback: Playwright with 20 s timeout — mirrors Pipeline A intercept logic.
Publication validation is handled by the pipeline's _is_valid_canonical.
"""

import asyncio
import os
from googlenewsdecoder import gnewsdecoder
from playwright.async_api import async_playwright

# ── gnewsdecoder settings ────────────────────────────────────────────────────
# Optional: proxy for rate limiting (env: RETRY_PROXY)
_PROXY    = os.getenv("RETRY_PROXY", None)
_INTERVAL = float(os.getenv("RETRY_DECODER_INTERVAL", "0"))

# ── Playwright fallback settings ─────────────────────────────────────────────
_PW_TIMEOUT_MS = int(os.getenv("RETRY_PW_TIMEOUT_MS", "20000"))

# Rate-limit: max concurrent Playwright browser sessions
_PW_CONCURRENCY     = int(os.getenv("RETRY_PW_CONCURRENCY", "1"))
_PW_DELAY_BETWEEN_S = float(os.getenv("RETRY_PW_DELAY_S", "0"))

_pw_semaphore: asyncio.Semaphore | None = None  # initialised lazily


def _get_semaphore() -> asyncio.Semaphore:
    global _pw_semaphore
    if _pw_semaphore is None:
        _pw_semaphore = asyncio.Semaphore(_PW_CONCURRENCY)
    return _pw_semaphore


# ── gnewsdecoder ─────────────────────────────────────────────────────────────

def _decode_url(rss_url: str) -> str | None:
    """Decode Google News URL to original article URL. Returns None on failure."""
    try:
        result = gnewsdecoder(rss_url, interval=_INTERVAL, proxy=_PROXY)
        if result.get("status") and result.get("decoded_url"):
            return result["decoded_url"]
    except Exception as e:
        print(f"[Pipeline B] gnewsdecoder error for {rss_url}: {e}")
    return None


# ── Playwright fallback ───────────────────────────────────────────────────────

async def _get_urls_playwright(url: str) -> list[str]:
    """Intercept navigated requests for *url* and return non-Google ones."""
    collected_urls: list[str] = []

    async with _get_semaphore():          # enforce concurrency rate limit
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            # Pre-set consent cookie to skip Google consent wall
            await context.add_cookies([{
                "name": "CONSENT",
                "value": "YES+",
                "domain": ".google.com",
                "path": "/",
            }])
            page = await context.new_page()

            def _intercept(request):
                if request.resource_type in ("document", "xhr"):
                    collected_urls.append(request.url)

            page.on("request", _intercept)

            try:
                await page.goto(url, timeout=_PW_TIMEOUT_MS)
            except Exception as e:
                print(f"[Pipeline B] Playwright error for {url}: {e}")
            finally:
                await browser.close()

        # Inter-request delay (rate limiting)
        if _PW_DELAY_BETWEEN_S > 0:
            await asyncio.sleep(_PW_DELAY_BETWEEN_S)

    return collected_urls


# ── Public entry-point ────────────────────────────────────────────────────────

async def process_urls_in_batches(urls: list):
    """
    1. Try googlenewsdecoder (fast, no browser).
    2. If that fails, fall back to Playwright intercept (20 s timeout).
    Returns the canonical URL (publication check done by pipeline).
    """
    for item in urls:
        rss_url = item["rss"]

        # Primary: gnewsdecoder
        decoded = await asyncio.to_thread(_decode_url, rss_url)
        if decoded:
            return decoded

        # Fallback: Playwright intercept
        print(f"[Pipeline B] gnewsdecoder failed, trying Playwright for {rss_url}")
        collected = await _get_urls_playwright(rss_url)
        for collected_url in collected:
            if "google.com" not in collected_url:
                return collected_url

    return None
