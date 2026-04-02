"""
diagnose_salesql.py
Run this FIRST to find the correct SalesQL endpoint for name+org search.
Usage: python3 diagnose_salesql.py

It will print which endpoints respond and what fields they return.
"""
import requests
import json

API_KEY = "AlvJvWRLBGQ4GvfY1EslG6r2XUF2xHAr"

HEADERS = {
    "accept":        "application/json",
    "x-api-key":     API_KEY,
    "Authorization": f"Bearer {API_KEY}",
}

# Test person — well-known, high chance of being in SalesQL
TEST_FIRST = "Tim"
TEST_LAST  = "Cook"
TEST_COMPANY = "Apple"
TEST_LINKEDIN = "https://www.linkedin.com/in/tim-cook"

print("=" * 65)
print("  SalesQL API Diagnostic")
print("=" * 65)

# ── 1. Test the URL-based enrich (known working endpoint) ────────────────────
print("\n[1] Testing URL-based enrich (baseline) …")
r = requests.get(
    "https://api-public.salesql.com/v1/persons/enrich/",
    params={"linkedin_url": TEST_LINKEDIN},
    headers=HEADERS, timeout=15
)
print(f"    Status: {r.status_code}")
if r.status_code == 200:
    d = r.json()
    emails = [e.get("email") for e in (d.get("emails") or [])]
    print(f"    Name  : {d.get('full_name','?')}")
    print(f"    Emails: {emails}")
    print("    ✅ URL enrich works!")
else:
    print(f"    ❌ Response: {r.text[:200]}")

# ── 2. Candidate name+org endpoints ──────────────────────────────────────────
CANDIDATES = [
    # (label, url, params)
    ("GET /v1/persons/ (first+last+org_name)",
     "https://api-public.salesql.com/v1/persons/",
     {"first_name": TEST_FIRST, "last_name": TEST_LAST, "organization_name": TEST_COMPANY}),

    ("GET /v1/persons/ (full_name+company)",
     "https://api-public.salesql.com/v1/persons/",
     {"full_name": f"{TEST_FIRST} {TEST_LAST}", "company_name": TEST_COMPANY}),

    ("GET /v1/persons/search/ (first+last+org)",
     "https://api-public.salesql.com/v1/persons/search/",
     {"first_name": TEST_FIRST, "last_name": TEST_LAST, "organization_name": TEST_COMPANY}),

    ("GET /v1/persons/find/ (first+last+org)",
     "https://api-public.salesql.com/v1/persons/find/",
     {"first_name": TEST_FIRST, "last_name": TEST_LAST, "organization_name": TEST_COMPANY}),

    ("GET /v1/persons/lookup/ (first+last+org)",
     "https://api-public.salesql.com/v1/persons/lookup/",
     {"first_name": TEST_FIRST, "last_name": TEST_LAST, "organization_name": TEST_COMPANY}),

    ("GET /v1/persons/ (name+domain)",
     "https://api-public.salesql.com/v1/persons/",
     {"first_name": TEST_FIRST, "last_name": TEST_LAST, "organization_domain": "apple.com"}),

    ("GET /v2/persons/enrich/ (name+org)",
     "https://api-public.salesql.com/v2/persons/enrich/",
     {"first_name": TEST_FIRST, "last_name": TEST_LAST, "organization_name": TEST_COMPANY}),

    ("GET /v1/persons/enrich/ (name only, no URL)",
     "https://api-public.salesql.com/v1/persons/enrich/",
     {"first_name": TEST_FIRST, "last_name": TEST_LAST, "organization_name": TEST_COMPANY}),
]

print(f"\n[2] Testing {len(CANDIDATES)} name+org endpoint candidates …\n")

working = []
for label, url, params in CANDIDATES:
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=12)
        status = r.status_code
        note = ""
        if status == 200:
            try:
                d = r.json()
                if isinstance(d, list): d = d[0] if d else {}
                emails = [e.get("email") for e in (d.get("emails") or [])]
                name   = d.get("full_name", "?")
                note   = f"✅ WORKS — name={name}, emails={emails}"
                working.append((label, url, params, emails))
            except Exception as e:
                note = f"⚠️  200 but bad JSON: {e}"
        elif status == 404:
            note = "endpoint not found"
        elif status == 422:
            note = f"422 unprocessable — wrong params? {r.text[:100]}"
        elif status == 401:
            note = "401 auth error"
        elif status == 429:
            note = "429 rate-limit"
        else:
            note = f"HTTP {status}: {r.text[:80]}"
        print(f"  {label}")
        print(f"    → {note}\n")
    except Exception as e:
        print(f"  {label}")
        print(f"    → ERROR: {e}\n")

print("=" * 65)
if working:
    print(f"\n✅ WORKING ENDPOINTS ({len(working)}):")
    for label, url, params, emails in working:
        print(f"\n  Label  : {label}")
        print(f"  URL    : {url}")
        print(f"  Params : {json.dumps(params)}")
        print(f"  Emails : {emails}")
    print(f"\n→ Copy the URL + param format above into salesql_enricher.py")
else:
    print("\n❌ No name+org endpoint found.")
    print("   The URL-based enrich (/v1/persons/enrich/?linkedin_url=...) still works.")
    print("   Consider using a different LinkedIn-finding strategy instead.")
