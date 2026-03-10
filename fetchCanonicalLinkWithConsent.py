import asyncio
from playwright.async_api import async_playwright
import re

async def get_urls(url):
    collected_urls = []
    # Use a US-based user agent to reduce chances of consent screens
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Set geolocation to United States and appropriate locale
        context = await browser.new_context(
            user_agent=user_agent,
            locale="en-US",
            geolocation={"longitude": -122.33, "latitude": 47.60},  # Seattle coordinates
            timezone_id="America/Los_Angeles",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"}
        )
        
        page = await context.new_page()
        
        async def intercept_request(request):
            if request.resource_type == "document" or request.resource_type == "xhr":
                collected_urls.append(request.url)
            
        
        # Function to handle consent pages
        async def handle_consent_page():
            # Check if we're on a consent page
            if "consent.google.com" in page.url:
                try:
                    # Try to click the "Agree to all" button (might have different selectors)
                    await page.click("button:has-text('Accept all')", timeout=3000)
                    await page.wait_for_load_state("networkidle")
                except Exception:
                    try:
                        # Try alternative buttons
                        await page.click("button:has-text('I agree')", timeout=3000)
                        await page.wait_for_load_state("networkidle")
                    except Exception as e:
                        print(f"Could not bypass consent page: {e}")
                        print("pageurl-->",page.url)
                        # Try to extract the continue URL and navigate directly
                        try:
                            continue_url = re.search(r'continue=([^&]+)', page.url)
                            if continue_url:
                                continue_url = continue_url.group(1)
                                continue_url = continue_url.replace('%3A', ':').replace('%2F', '/').replace('%3D', '=').replace('%3F', '?').replace('%26', '&')
                                await page.goto(continue_url, wait_until="domcontentloaded")
                        except Exception as e2:
                            print(f"Failed to extract continue URL: {e2}")
        
        page.on("request", intercept_request)
        
        try:
            # Set cookies to skip consent
            await context.add_cookies([
                {
                    "name": "CONSENT",
                    "value": "YES+",
                    "domain": ".google.com",
                    "path": "/"
                }
            ])
            
            await page.goto(url, wait_until="domcontentloaded")
            
            # Handle consent page if we land on one
            await handle_consent_page()
            
            # Additional wait to ensure page loads fully
            await page.wait_for_timeout(3000)
            
            # Try to find visible links that might contain the target URL
            if "news.google.com" in page.url:
                print("On Google News, checking for article links...")
                article_links = await page.evaluate('''() => {
                    const links = Array.from(document.querySelectorAll('a[href*="http"]'));
                    return links.map(link => link.href);
                }''')
                collected_urls.extend(article_links)
            
        except Exception as e:
            print(f"Error navigating to URL {url}: {e}")
        finally:
            await browser.close()
            
    return collected_urls

async def process_urls_in_batches(urls):
    all_collected_urls = []
    
    for url in urls:
        collected_urls = await get_urls(url['rss'])
        all_collected_urls.extend(collected_urls)
        
        # Filter URLs based on the publication domain
        matching_urls = [u for u in collected_urls if re.search(url['publication'], u)]
        
        if matching_urls:
            return matching_urls[0]
            
    return None

# Example usage
#rss = 'https://news.google.com/rss/articles/CBMi6gFBVV95cUxPQTQyaE5oSkpkYW1oWDB1ZE9NSmNwdkRVV29tcUhLbzVZMkJtWExZV1R1Uk14akVVeG9RaU5EVTAyRHRqak9hMXpmeUlSdHJzdktKNmx5R21hbGdoSGFkZWFQRkpDbjhxN2x0b3pzdE5VRHRvQVBBRTF0SEhaTk9fampaR0ZSWjQxTkdNcmtxZ0JIOWl1REpiZTVfallOWUZ4SnVlZ3d5QUhlYTl6RzlhcW40YVZEb0Z3cDB0UkstZTNJWkR1TEI1SGdCZGdac1ZCM3VJRDZwU3BMbkJXWGJkbmpIRHdtY3BxUFHSAeoBQVVfeXFMT0E0MmhOaEpKZGFtaFgwdWRPTUpjcHZEVVdvbXFIS281WTJCbVhMWVdUdVJNeGpFVXhvUWlORFUwMkR0ampPYTF6ZnlJUnRyc3ZLSjZseUdtYWxnaEhhZGVhUEZKQ244cTdsdG96c3ROVUR0b0FQQUUxdEhIWk5PX2pqWkdGUlo0MU5HTXJrcWdCSDlpdURKYmU1X2pZTllGeEp1ZWd3eUFIZWE5ekc5YXFuNGFWRG9Gd3AwdFJLLWUzSVpEdUxCNUhnQmRnWnNWQjN1SUQ2cFNwTG5CV1hiZG5qSER3bWNwcVBR?oc=5&hl=en-US&gl=US&ceid=US:en'
#urls = [{"rss": rss, "publication": 'indiatoday.in'}]
#result = asyncio.run(process_urls_in_batches(urls))
#print(result)