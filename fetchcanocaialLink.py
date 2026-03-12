import asyncio
import os
from playwright.async_api import async_playwright

# Pipeline A timeout: 3s per spec (env: FAST_TIMEOUT_MS)
_GOTO_TIMEOUT_MS   = int(os.getenv("FAST_TIMEOUT_MS", "3000"))
_POST_LOAD_WAIT_MS = int(os.getenv("FAST_POST_LOAD_MS", "0"))  # 0 = rely on goto timeout only

async def get_urls(url):
    collected_urls = []

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
    """
    Intercept requests made by the browser when navigating the Google News URL.
    Returns the first non-Google URL found (publication check done by pipeline).
    """
    for url in urls:
        collected_urls = await get_urls(url['rss'])
        for collected_url in collected_urls:
            if 'google.com' not in collected_url:
                return collected_url

    return None

# Example usage
# rss = 'https://news.google.com/rss/articles/CBMi6gFBVV95cUxPQTQyaE5oSkpkYW1oWDB1ZE9NSmNwdkRVV29tcUhLbzVZMkJtWExZV1R1Uk14akVVeG9RaU5EVTAyRHRqak9hMXpmeUlSdHJzdktKNmx5R21hbGdoSGFkZWFQRkpDbjhxN2x0b3pzdE5VRHRvQVBBRTF0SEhaTk9fampaR0ZSWjQxTkdNcmtxZ0JIOWl1REpiZTVfallOWUZ4SnVlZ3d5QUhlYTl6RzlhcW40YVZEb0Z3cDB0UkstZTNJWkR1TEI1SGdCZGdac1ZCM3VJRDZwU3BMbkJXWGJkbmpIRHdtY3BxUFHSAeoBQVVfeXFMT0E0MmhOaEpKZGFtaFgwdWRPTUpjcHZEVVdvbXFIS281WTJCbVhMWVdUdVJNeGpFVXhvUWlORFUwMkR0ampPYTF6ZnlJUnRyc3ZLSjZseUdtYWxnaEhhZGVhUEZKQ244cTdsdG96c3ROVUR0b0FQQUUxdEhIWk5PX2pqWkdGUlo0MU5HTXJrcWdCSDlpdURKYmU1X2pZTllGeEp1ZWd3eUFIZWE5ekc5YXFuNGFWRG9Gd3AwdFJLLWUzSVpEdUxCNUhnQmRnWnNWQjN1SUQ2cFNwTG5CV1hiZG5qSER3bWNwcVBR?oc=5&hl=en-US&gl=US&ceid=US:en'
# urls = [{"rss": rss, "publication": 'indiatoday.in'}]

# asyncio.run(process_urls_in_batches(urls))
