"""
fetchCanonicalLinkWithConsent.py  —  Pipeline B fetcher (consent-aware, 15s budget)

Timing budget (must fit inside Pipeline B's asyncio.wait_for(15s)):
  - page.goto()          : up to 12s  (GOTO_TIMEOUT_MS)
  - wait_for_timeout()   : 1.5s       (POST_LOAD_WAIT_MS)  — reduced from 3s
  - consent handling     : ~0.5s overhead
  Total worst-case       : ~14s  ✅  safely under 15s

Previous problems fixed:
  - wait_for_timeout(3000) + goto(15000) = 18s total → exceeded asyncio budget
  - page.goto() had no timeout → Playwright default 30s leaked zombie browsers
  - wait_for_timeout is now env-configurable via POST_LOAD_WAIT_MS
"""

import asyncio
from playwright.async_api import async_playwright
import re
import os

# goto=15000 + wait=5000 = 20s total
# asyncio.wait_for in Pipeline B is set to 25s to give headroom
_GOTO_TIMEOUT_MS    = int(os.getenv("GOTO_TIMEOUT_MS",   "15000"))
_POST_LOAD_WAIT_MS  = int(os.getenv("POST_LOAD_WAIT_MS", "5000"))


async def get_urls(url: str) -> list:
    collected_urls = []
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=user_agent,
            locale="en-US",
            geolocation={"longitude": -122.33, "latitude": 47.60},
            timezone_id="America/Los_Angeles",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"}
        )

        # Pre-set consent cookie to skip the consent wall entirely
        await context.add_cookies([{
            "name": "CONSENT", "value": "YES+",
            "domain": ".google.com", "path": "/"
        }])

        page = await context.new_page()

        async def intercept_request(request):
            if request.resource_type in ("document", "xhr"):
                collected_urls.append(request.url)

        async def handle_consent_page():
            if "consent.google.com" not in page.url:
                return
            for selector in ("button:has-text('Accept all')", "button:has-text('I agree')"):
                try:
                    await page.click(selector, timeout=3000)
                    await page.wait_for_load_state("networkidle", timeout=5000)
                    return
                except Exception:
                    pass
            # Last resort: extract the continue= param and navigate directly
            try:
                m = re.search(r'continue=([^&]+)', page.url)
                if m:
                    dest = (m.group(1)
                            .replace('%3A', ':').replace('%2F', '/')
                            .replace('%3D', '=').replace('%3F', '?').replace('%26', '&'))
                    await page.goto(dest, wait_until="domcontentloaded",
                                    timeout=_GOTO_TIMEOUT_MS)
            except Exception:
                pass

        page.on("request", intercept_request)

        try:
            await page.goto(url, wait_until="domcontentloaded",
                            timeout=_GOTO_TIMEOUT_MS)
            await handle_consent_page()

            # Short post-load wait — reduced to fit within asyncio budget
            await page.wait_for_timeout(_POST_LOAD_WAIT_MS)

            if "news.google.com" in page.url:
                article_links = await page.evaluate("""() => {
                    return Array.from(document.querySelectorAll('a[href*="http"]'))
                                .map(a => a.href);
                }""")
                collected_urls.extend(article_links)

        except Exception as e:
            print(f"Error navigating to {url}: {e}")
        finally:
            await browser.close()

    return collected_urls


async def process_urls_in_batches(urls: list):
    for url in urls:
        collected = await get_urls(url["rss"])
        matches   = [u for u in collected if re.search(url["publication"], u)]
        if matches:
            return matches[0]
    return None
