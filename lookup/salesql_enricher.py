"""
salesql_enricher.py

Correct SalesQL API format (per official docs):

1. By full name + organization domain  ← PRIMARY
   GET https://api-public.salesql.com/v1/persons/enrich
   Params: full_name, organization_domain, api_key

2. By LinkedIn URL  ← SECONDARY (richer data when URL is available)
   GET https://api-public.salesql.com/v1/persons/enrich
   Params: linkedin_url, api_key

Both return the same dict shape:
  {
    "direct_email":   str,
    "work_email":     str,
    "personal_email": str,
    "best_email":     str,          # work > direct > personal
    "direct_phone":   str,          # direct dial (highest priority)
    "work_phone":     str,          # company main / office line
    "mobile_phone":   str,          # mobile / personal phone
    "phone":          str,          # best phone: direct > work > mobile
    "phone_type":     str,          # "direct" | "work" | "mobile" | ""
    "linkedin_url":   str,
    "source":         str,          # "salesql_name" | "salesql_url" | "not_found" | ...
  }

Phone priority: direct > work > mobile
"""

from __future__ import annotations
import os
import re as _re
import time
import requests
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Config ────────────────────────────────────────────────────────────────────
SALESQL_API_KEY    = os.getenv("SALESQL_API_KEY", "AlvJvWRLBGQ4GvfY1EslG6r2XUF2xHAr")
SALESQL_ENRICH_URL = "https://api-public.salesql.com/v1/persons/enrich"

SESSION = requests.Session()


def _headers() -> dict:
    return {
        "accept":        "application/json",
        "x-api-key":     SALESQL_API_KEY,
        "Authorization": f"Bearer {SALESQL_API_KEY}",
    }


def _empty(source: str = "not_found") -> dict:
    return {
        "direct_email":   "",
        "work_email":     "",
        "personal_email": "",
        "best_email":     "",
        "direct_phone":   "",
        "work_phone":     "",
        "mobile_phone":   "",
        "phone":          "",
        "phone_type":     "",
        "linkedin_url":   "",
        "source":         source,
    }


def _parse_response(data: dict, source_label: str, prefer_domain: str = "") -> dict:
    """
    Parse a SalesQL person response into our standard dict.

    prefer_domain: if set (e.g. '1stdibs.com'), emails matching that domain are
    moved to the front before bucket-assignment so they become best_email.
    This fixes cases where SalesQL returns an old-employer email first.
    """
    result = _empty(source_label)

    # ── Emails — accept ALL that SalesQL returns ──────────────────────────────
    # Never filter by status. The SalesQL UI "error" = API "invalid" — same data.
    raw_emails = [
        ((em.get("email") or "").strip(), (em.get("type") or "").lower())
        for em in (data.get("emails") or [])
        if (em.get("email") or "").strip()
    ]

    # Move domain-matching emails to the front so they win the first-seen bucket
    if prefer_domain:
        pd = prefer_domain.lower()
        matched   = [(a, t) for a, t in raw_emails if "@" in a and a.split("@")[-1].lower() == pd]
        unmatched = [(a, t) for a, t in raw_emails if (a, t) not in matched]
        raw_emails = matched + unmatched

    for addr, etype in raw_emails:
        # Always treat .edu addresses as personal (university/school emails, not work)
        addr_domain = addr.split("@")[-1].lower() if "@" in addr else ""
        if addr_domain.endswith(".edu"):
            if not result["personal_email"]:
                result["personal_email"] = addr
            continue
        if "work" in etype and not result["work_email"]:
            result["work_email"] = addr
        elif "direct" in etype and not result["direct_email"]:
            result["direct_email"] = addr
        elif "personal" in etype and not result["personal_email"]:
            result["personal_email"] = addr
        elif not result["work_email"]:
            result["work_email"] = addr   # untyped → work bucket

    result["best_email"] = (
        result["work_email"]
        or result["direct_email"]
        or result["personal_email"]
        or ""
    )

    # ── Phones — priority: direct > work > mobile ─────────────────────────────
    phones = data.get("phones") or []
    for ph in phones:
        num   = (ph.get("phone") or "").strip()
        ptype = (ph.get("type")  or "").lower()
        valid = ph.get("is_valid", True)
        # Only skip if explicitly False — None/missing means unknown validity, keep it
        if not num or valid is False:
            continue
        if "direct" in ptype and not result["direct_phone"]:
            result["direct_phone"] = num
        elif "work" in ptype or "office" in ptype or "hq" in ptype:
            if not result["work_phone"]:
                result["work_phone"] = num
        elif "mobile" in ptype or "cell" in ptype or "personal" in ptype:
            if not result["mobile_phone"]:
                result["mobile_phone"] = num
        else:
            if not result["work_phone"]:
                result["work_phone"] = num
            elif not result["mobile_phone"]:
                result["mobile_phone"] = num

    if result["direct_phone"]:
        result["phone"]      = result["direct_phone"]
        result["phone_type"] = "direct"
    elif result["work_phone"]:
        result["phone"]      = result["work_phone"]
        result["phone_type"] = "work"
    elif result["mobile_phone"]:
        result["phone"]      = result["mobile_phone"]
        result["phone_type"] = "mobile"

    # ── Top-level phone fallback (some SalesQL responses use phone_number key) ──
    # If the phones array yielded nothing, check top-level fields
    if not result["phone"]:
        for key in ("phone_number", "phone", "mobile_phone", "work_phone", "direct_phone"):
            top_val = (data.get(key) or "").strip()
            if top_val and top_val != result["phone"]:
                # Infer type from key name
                if "direct" in key:
                    result["direct_phone"] = result["direct_phone"] or top_val
                    result["phone"]        = top_val
                    result["phone_type"]   = "direct"
                elif "mobile" in key:
                    result["mobile_phone"] = result["mobile_phone"] or top_val
                    result["phone"]        = top_val
                    result["phone_type"]   = "mobile"
                else:
                    result["work_phone"] = result["work_phone"] or top_val
                    result["phone"]      = top_val
                    result["phone_type"] = "work"
                break

    result["linkedin_url"] = (data.get("linkedin_url") or "").strip()
    return result


def _safe_get(url: str, params: dict, source_label: str) -> dict:
    """Make a GET request to SalesQL, handle all error codes, return parsed dict."""
    # Always include api_key as query param (docs require it)
    params = {**params, "api_key": SALESQL_API_KEY}
    try:
        resp = SESSION.get(url, params=params, headers=_headers(), timeout=20)

        if resp.status_code == 429:
            print("    [salesql] 429 rate-limit — backing off 10s …")
            time.sleep(10)
            resp = SESSION.get(url, params=params, headers=_headers(), timeout=20)

        if resp.status_code == 401:
            print("    [salesql] 401 Unauthorized — check SALESQL_API_KEY")
            return _empty("auth_error")

        if resp.status_code == 404:
            return _empty("not_found")

        if resp.status_code == 422:
            return _empty("bad_params")

        if resp.status_code == 402:
            print("    [salesql] 402 Payment Required — credits exhausted")
            return _empty("no_credits")

        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, list):
            data = data[0] if data else {}

        if not data:
            return _empty("not_found")

        # Pass organization_domain as prefer_domain so the parser front-loads
        # emails matching the queried company (fixes old-employer email priority)
        prefer_domain = params.get("organization_domain", "")
        return _parse_response(data, source_label, prefer_domain=prefer_domain)

    except requests.exceptions.Timeout:
        print("    [salesql] timeout — retrying …")
        try:
            time.sleep(3)
            resp = SESSION.get(url, params=params, headers=_headers(), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                data = data[0] if data else {}
            prefer_domain = params.get("organization_domain", "")
            return _parse_response(data, source_label, prefer_domain=prefer_domain) if data else _empty("not_found")
        except Exception as e:
            print(f"    [salesql] retry failed: {e}")
            return _empty("timeout")
    except Exception as e:
        print(f"    [salesql] error: {e}")
        return _empty("error")


# ── Domain helpers ─────────────────────────────────────────────────────────────

_LEGAL_SUFFIX_RE = _re.compile(
    r',?\s+(?:Inc\.?|Incorporated|Corp\.?|Corporation|LLC|L\.L\.C\.?|'
    r'Ltd\.?|Limited|L\.P\.?|PLC|Co\.?|Holdings?|Holding|'
    r'Technologies?|Technology|Solutions?|Services?|Enterprises?|'
    r'Communications?|Systems?|Networks?|Capital|Partners?|Group|'
    r'International|Global|Worldwide|Americas?)\s*[.,]?\s*$',
    _re.IGNORECASE,
)
_DOMAIN_SUFFIX_RE = _re.compile(
    r'\.(?:com|net|org|io|ca|co|us|biz)\s*$',
    _re.IGNORECASE,
)


def _clean_company(name: str) -> str:
    """
    Strip legal suffixes and embedded domain extensions from yfinance company names.

    '1stdibs.Com, Inc.'            → '1stdibs'
    'Aclarion, Inc.'               → 'Aclarion'
    'Altigen Communications, Inc.' → 'Altigen Communications'
    'Energous Corporation'         → 'Energous'
    """
    s = name.strip()
    for _ in range(3):
        prev = s
        s = _LEGAL_SUFFIX_RE.sub("", s).strip().rstrip(".,").strip()
        if s == prev:
            break
    s = _DOMAIN_SUFFIX_RE.sub("", s).strip().rstrip(".,").strip()
    return s or name.strip()


def _extract_domain(website: str) -> str:
    """
    Extract clean domain from a website URL.

    'https://www.1stdibs.com'  → '1stdibs.com'
    'https://aclarion.com'     → 'aclarion.com'
    'backblaze.com'            → 'backblaze.com'
    """
    if not website:
        return ""
    s = website.strip()
    if "://" not in s:
        s = "https://" + s
    try:
        domain = urlparse(s).netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def _domain_variants(company: str, website: str = "", is_spac: bool = False) -> list[str]:
    """
    Return organization_domain values to try, most specific first.

    1. Actual website domain extracted from yfinance (best)
    2. Derived from first word of cleaned company + .com
    3. Empty string — name-only lookup (SPAC mode only, last resort)
    """
    variants: list[str] = []

    def _add(v: str) -> None:
        v = v.strip().lower()
        if v and v not in variants:
            variants.append(v)

    # 1. Website domain (most reliable — comes from yfinance)
    if website:
        _add(_extract_domain(website))

    # 2. Derive domain from cleaned company name
    cleaned = _clean_company(company)
    if cleaned:
        first_word = _re.sub(r"[^a-z0-9]", "", cleaned.lower().split()[0])
        if first_word and len(first_word) > 2:
            _add(f"{first_word}.com")

    # 3. SPAC mode: add empty-domain fallback for name-only search.
    # SPACs are shell companies that typically have no website, so the
    # domain-based lookup always fails. Name-only search may still return
    # results if SalesQL has the person indexed by name alone.
    if is_spac and "" not in variants:
        variants.append("")

    return variants


def _prefer_company_domain_email(emails: list[str], company: str, website: str = "") -> str:
    """
    Return the email whose domain best matches the company.
    Prefers website domain > company keyword match > first email.

    Fixes stale-data cases where SalesQL returns an old employer email.
    """
    if not emails:
        return ""
    if len(emails) == 1:
        return emails[0]

    # Prefer domain matching the actual website
    site_domain = _extract_domain(website) if website else ""
    if site_domain:
        for addr in emails:
            if "@" in addr and addr.split("@")[-1].lower() == site_domain:
                return addr

    # Fall back to keyword match from company name
    co_keyword = _re.sub(r"[^a-z0-9]", "", _clean_company(company).lower().split()[0]) if company else ""
    if co_keyword and len(co_keyword) > 3:
        for addr in emails:
            domain = addr.split("@")[-1].lower() if "@" in addr else ""
            if co_keyword in domain:
                return addr

    return emails[0]


# ── Name variation helpers ─────────────────────────────────────────────────────

_SALESQL_PREFIX_RE = _re.compile(
    r'^\s*(Mr\.?|Ms\.?|Mrs\.?|Miss\.?|Dr\.?|Prof\.?|Sir)\s+',
    _re.IGNORECASE,
)
_SALESQL_SUFFIX_STRIP = _re.compile(
    r'\s+(jr\.?|sr\.?|ii|iii|iv|phd|ph\.d\.?|md|m\.d\.?|jd|j\.d\.?|'
    r'cpa\.?|c\.p\.a\.?|cfa\.?|mba|m\.b\.a\.?|llm|esq\.?)\s*$',
    _re.IGNORECASE,
)

# Common formal-name → nickname mappings
# Used to try "Dan Goldberger" when yfinance returns "Daniel Goldberger"
_NICKNAMES: dict[str, str] = {
    "william":    "bill",
    "michael":    "mike",
    "robert":     "rob",
    "james":      "jim",
    "richard":    "rick",
    "joseph":     "joe",
    "thomas":     "tom",
    "charles":    "chuck",
    "kenneth":    "ken",
    "steven":     "steve",
    "stephen":    "steve",
    "daniel":     "dan",
    "timothy":    "tim",
    "anthony":    "tony",
    "edward":     "ed",
    "christopher":"chris",
    "matthew":    "matt",
    "andrew":     "andy",
    "nicholas":   "nick",
    "jonathan":   "jon",
    "benjamin":   "ben",
    "alexander":  "alex",
    "joshua":     "josh",
    "nathaniel":  "nate",
    "jeffrey":    "jeff",
    "gregory":    "greg",
    "lawrence":   "larry",
    "raymond":    "ray",
    "gerald":     "jerry",
    "donald":     "don",
    "ronald":     "ron",
    "douglas":    "doug",
    "samuel":     "sam",
    "patrick":    "pat",
    "raymond":    "ray",
    "leonard":    "len",
    "gerald":     "jerry",
    "eugene":     "gene",
    "phillip":    "phil",
    "philip":     "phil",
    "frederick":  "fred",
}


def _strip_apostrophes(name: str) -> str:
    """Remove apostrophes from names: "O'Dowd" → "ODowd"."""
    return name.replace("'", "").replace("'", "")


def _name_variants(full_name: str) -> list[str]:
    """
    Return a list of full-name strings to try, most specific first.

    'Mr. David S. Rosenblatt, MBA' → ['David S. Rosenblatt', 'David Rosenblatt']
    'Thomas J. Etergino CPA'       → ['Thomas J. Etergino', 'Thomas Etergino']
    'William O\'Dowd IV'           → ['William O\'Dowd', 'Bill O\'Dowd', 'William ODowd', 'Bill ODowd']
    'Daniel S. Goldberger'         → ['Daniel S. Goldberger', 'Daniel Goldberger', 'Dan Goldberger']
    """
    clean = _SALESQL_PREFIX_RE.sub("", full_name).strip()
    if "," in clean:
        before = clean.split(",")[0].strip()
        if len(before.split()) >= 2:
            clean = before
    clean = _SALESQL_SUFFIX_STRIP.sub("", clean).strip()

    parts = clean.split()
    seen: list[str] = []

    def _add(v: str) -> None:
        v = _re.sub(r"\s+", " ", v).strip()
        if v and v not in seen:
            seen.append(v)

    def _add_with_apostrophe_variants(name: str) -> None:
        """Add name + apostrophe-stripped variant."""
        _add(name)
        stripped = _strip_apostrophes(name)
        if stripped != name:
            _add(stripped)

    if len(parts) >= 2:
        first = parts[0]
        last  = parts[-1]

        # 1. Full cleaned name (e.g. "Daniel S. Goldberger" or "William O'Dowd")
        _add_with_apostrophe_variants(clean)

        # 2. First + last only (drop middle initial) e.g. "Daniel Goldberger"
        if len(parts) >= 3:
            _add_with_apostrophe_variants(f"{first} {last}")

        # 3. Nickname + last (e.g. "Dan Goldberger", "Bill O'Dowd")
        nick = _NICKNAMES.get(first.lower())
        if nick:
            nick_cap = nick.capitalize()
            _add_with_apostrophe_variants(f"{nick_cap} {last}")

    return seen


# ── Public API ─────────────────────────────────────────────────────────────────

def search_by_name(full_name: str, domain: str = "") -> dict:
    """
    PRIMARY strategy — correct SalesQL API format:
      GET /v1/persons/enrich?full_name=David Rosenblatt&organization_domain=1stdibs.com&api_key=...

    domain should be the company's web domain (e.g. '1stdibs.com'), NOT the company name.
    """
    if not full_name.strip():
        return _empty("no_name")

    params: dict = {"full_name": full_name.strip()}
    if domain:
        params["organization_domain"] = domain.strip()

    return _safe_get(SALESQL_ENRICH_URL, params, "salesql_name")


def search_by_name_with_variations(
    first_name: str,
    last_name: str,
    full_name: str,
    company: str,
    website: str = "",
    is_spac: bool = False,
) -> dict:
    """
    Try SalesQL with multiple full_name × organization_domain combinations.

    Name variants:   full cleaned name → first+last only
    Domain variants: website domain → derived from company → no domain

    When is_spac=True, also tries name-only search (empty domain) as a
    fallback, since SPACs rarely have real websites.

    Returns first result with email OR phone; otherwise best partial; otherwise empty.
    """
    names   = _name_variants(full_name) or (
        [f"{first_name} {last_name}".strip()] if first_name or last_name else []
    )
    domains = _domain_variants(company, website, is_spac=is_spac)

    last_result = _empty("not_found")

    for name in names:
        for domain in domains:
            result = search_by_name(name, domain)
            domain_label = domain or "(no domain)"
            print(f"      → SalesQL try: '{name}' @ {domain_label} → "
                  f"email={result.get('best_email') or 'none'} "
                  f"phone={result.get('phone') or 'none'} "
                  f"src={result.get('source')}")

            if result["best_email"] or result["phone"]:
                # Prefer email matching current company domain
                all_emails = [e for e in [
                    result.get("work_email", ""),
                    result.get("direct_email", ""),
                    result.get("personal_email", ""),
                ] if e]
                preferred = _prefer_company_domain_email(all_emails, company, website)
                if preferred and preferred != result["best_email"]:
                    result["best_email"] = preferred
                    result["work_email"] = preferred
                return result

            if result.get("source") not in ("not_found", "bad_params", "no_name", ""):
                last_result = result

            time.sleep(0.5)

    return last_result


def search_by_name_and_company(full_name: str, company_name: str, title: str = "") -> dict:
    """
    STEP 3 strategy — search by name + company name string (no domain).
    Some SalesQL lookups work with just the company name instead of domain.

    Tries full_name + company_name, then full_name alone.
    """
    if not full_name.strip():
        return _empty("no_name")

    # Try with company name as organization_domain (SalesQL sometimes accepts this)
    params: dict = {"full_name": full_name.strip()}
    if company_name:
        params["company_name"] = company_name.strip()
    if title:
        params["title"] = title.strip()

    result = _safe_get(SALESQL_ENRICH_URL, params, "salesql_name_company")
    if result.get("best_email") or result.get("phone"):
        return result

    # Fallback: name-only (no domain, no company)
    params_bare = {"full_name": full_name.strip()}
    return _safe_get(SALESQL_ENRICH_URL, params_bare, "salesql_name_only")


def enrich_by_url(linkedin_url: str) -> dict:
    """
    SECONDARY strategy — enrich by LinkedIn URL:
      GET /v1/persons/enrich?linkedin_url=https://linkedin.com/in/...&api_key=...
    """
    if not linkedin_url or not linkedin_url.strip():
        return _empty("no_url")

    params = {"linkedin_url": linkedin_url.strip()}
    return _safe_get(SALESQL_ENRICH_URL, params, "salesql_url")


def enrich_contact(linkedin_url: str) -> dict:
    """Backwards-compatible wrapper."""
    return enrich_by_url(linkedin_url)
