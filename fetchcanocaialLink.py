import asyncio
import os
from playwright.async_api import async_playwright
import re

# Pipeline A timeout: ~6s typical, 8s budget (env: FAST_TIMEOUT_MS)
_GOTO_TIMEOUT_MS   = int(os.getenv("FAST_TIMEOUT_MS", "8000"))
_POST_LOAD_WAIT_MS = int(os.getenv("FAST_POST_LOAD_MS", "2000"))

async def get_urls(url):
    collected_urls = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        async def intercept_request(request):
            if request.resource_type == "document" or request.resource_type == "xhr":
                collected_urls.append(request.url)

        page.on("request", intercept_request)

        try:
            await page.goto(url, timeout=_GOTO_TIMEOUT_MS)
            await page.wait_for_timeout(_POST_LOAD_WAIT_MS)
        except Exception as e:
            print(f"Error navigating to URL {url}: {e}")
        finally:
            await browser.close()

    return collected_urls

async def process_urls_in_batches(urls):
    all_collected_urls = []
    
    for url in urls:
        collected_urls = await get_urls(url['rss'])  # Ensure we await the coroutine
        all_collected_urls.extend(collected_urls)  # Collect all URLs from the batch

        for collected_url in collected_urls:
            # print(collected_url)
            if re.search(url['publication'], collected_url):
                # print(collected_url)
                return collected_url

    return None

# Example usage
# rss = 'https://news.google.com/rss/articles/CBMi6gFBVV95cUxPQTQyaE5oSkpkYW1oWDB1ZE9NSmNwdkRVV29tcUhLbzVZMkJtWExZV1R1Uk14akVVeG9RaU5EVTAyRHRqak9hMXpmeUlSdHJzdktKNmx5R21hbGdoSGFkZWFQRkpDbjhxN2x0b3pzdE5VRHRvQVBBRTF0SEhaTk9fampaR0ZSWjQxTkdNcmtxZ0JIOWl1REpiZTVfallOWUZ4SnVlZ3d5QUhlYTl6RzlhcW40YVZEb0Z3cDB0UkstZTNJWkR1TEI1SGdCZGdac1ZCM3VJRDZwU3BMbkJXWGJkbmpIRHdtY3BxUFHSAeoBQVVfeXFMT0E0MmhOaEpKZGFtaFgwdWRPTUpjcHZEVVdvbXFIS281WTJCbVhMWVdUdVJNeGpFVXhvUWlORFUwMkR0ampPYTF6ZnlJUnRyc3ZLSjZseUdtYWxnaEhhZGVhUEZKQ244cTdsdG96c3ROVUR0b0FQQUUxdEhIWk5PX2pqWkdGUlo0MU5HTXJrcWdCSDlpdURKYmU1X2pZTllGeEp1ZWd3eUFIZWE5ekc5YXFuNGFWRG9Gd3AwdFJLLWUzSVpEdUxCNUhnQmRnWnNWQjN1SUQ2cFNwTG5CV1hiZG5qSER3bWNwcVBR?oc=5&hl=en-US&gl=US&ceid=US:en'
# urls = [{"rss": rss, "publication": 'indiatoday.in'}]

# asyncio.run(process_urls_in_batches(urls))
