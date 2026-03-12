"""Live test: decode Google News URLs from CSV and validate against publication."""
import csv
import asyncio
import re
import tldextract
from fetchCanonicalLinkWithConsent import process_urls_in_batches

MAX_LINK_LENGTH = 255

MSN_BARE = {
    "https://www.msn.com/en-ca", "https://www.msn.com",
    "http://www.msn.com/en-ca",  "http://www.msn.com",
}

def get_main_domain(url: str) -> str:
    ext = tldextract.extract(url)
    return f"{ext.domain}.{ext.suffix}" if ext.domain and ext.suffix else ""

def is_valid_canonical(link: str, publication: str) -> bool:
    if not link:
        return False
    if len(link) > MAX_LINK_LENGTH:
        return False
    if link.rstrip("/") in MSN_BARE:
        return False
    return bool(re.search(re.escape(get_main_domain(link)), publication))

async def main():
    rows = []
    with open(r"c:\Users\nagle\Downloads\data-1773305780482.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    print(f"Total rows: {len(rows)}\n")
    sep = "-" * 200

    ok = fail = skip = 0
    for i, row in enumerate(rows, 1):
        pub      = row["PUBLICATION"]
        link     = row["LINK"]
        retry    = row["RETRY"]
        iserr    = row["ISERROR"]
        keyword  = row.get("KEYWORD", "").strip()
        artdate  = row.get("ARTICLEDATE", "").strip()
        counter  = row.get("COUNTER_EXTRACT", "NULL")
        existing = row.get("CANONICAL_LINK", "").strip()

        print(sep)
        print(f"#{i}")
        print(f"  PUBLICATION    : {pub}")
        print(f"  KEYWORD        : {keyword}")
        print(f"  ARTICLE DATE   : {artdate}")
        print(f"  RETRY          : {retry}  |  ISERROR: {iserr}  |  COUNTER_EXTRACT: {counter}")
        print(f"  RSS LINK       : {link}")

        # Skip rows that are permanently failed (news.google.com publication)
        if pub == "news.google.com":
            print(f"  DECODED        : —")
            print(f"  DECODED LEN    : —")
            print(f"  DOMAIN MATCH   : —")
            print(f"  RESULT         : SKIP (publication=news.google.com will never match)")
            skip += 1
            continue

        # Skip rows already with ISERROR=1 and no canonical
        if iserr == "1" and not existing:
            print(f"  DECODED        : —")
            print(f"  DECODED LEN    : —")
            print(f"  DOMAIN MATCH   : —")
            print(f"  RESULT         : SKIP (ISERROR=1, no canonical — retry exhausted)")
            skip += 1
            continue

        decoded = await process_urls_in_batches([{"rss": link}])
        if decoded is None:
            print(f"  DECODED        : —")
            print(f"  DECODED LEN    : —")
            print(f"  DOMAIN MATCH   : —")
            print(f"  RESULT         : DECODE_FAIL")
            fail += 1
            continue

        decoded_domain = get_main_domain(decoded)
        domain_match   = bool(re.search(re.escape(decoded_domain), pub)) if decoded_domain else False
        too_long       = len(decoded) > MAX_LINK_LENGTH
        bare_msn       = decoded.rstrip("/") in MSN_BARE

        if not decoded:
            reason = "empty decoded URL"
        elif too_long:
            reason = f"URL too long ({len(decoded)} > {MAX_LINK_LENGTH} chars)"
        elif bare_msn:
            reason = "bare MSN root URL rejected"
        elif not domain_match:
            reason = f"domain mismatch: '{decoded_domain}' not in '{pub}'"
        else:
            reason = "OK"

        valid  = (decoded and not too_long and not bare_msn and domain_match)
        status = "VALID" if valid else f"FAIL — {reason}"
        if valid:
            ok += 1
        else:
            fail += 1

        print(f"  DECODED        : {decoded}")
        print(f"  DECODED LEN    : {len(decoded)} chars")
        print(f"  DECODED DOMAIN : {decoded_domain}")
        print(f"  DOMAIN MATCH   : {'YES' if domain_match else 'NO'}")
        print(f"  RESULT         : {status}")

    print(sep)
    print(f"\nVALID: {ok} | FAIL/MISMATCH: {fail} | SKIPPED: {skip} | TOTAL: {len(rows)}")

asyncio.run(main())
