"""
linkedin_finder.py
Find a LinkedIn /in/ profile URL for a given name + company.

Strategy: DuckDuckGo HTML search only.
Google is intentionally disabled — it rate-limits immediately (429) and adds
45s sleeps that stall the whole pipeline. DuckDuckGo has no rate-limit issues.

Name variation logic: tries the full (cleaned) name first, then falls back to
first+last only (dropping middle initials/names) for better recall.
"""

from __future__ import annotations
import re
import time
import random
from typing import Optional
import requests
from bs4 import BeautifulSoup

# ── Company name cleaner (mirrors salesql_enricher._clean_company) ───────────

_LI_LEGAL_RE = re.compile(
    r',?\s+(?:Inc\.?|Incorporated|Corp\.?|Corporation|LLC|L\.L\.C\.?|'
    r'Ltd\.?|Limited|L\.P\.?|PLC|Co\.?|Holdings?|Holding|'
    r'Technologies?|Technology|Solutions?|Services?|Enterprises?|'
    r'Communications?|Systems?|Networks?|Capital|Partners?|Group|'
    r'International|Global|Worldwide|Americas?)\s*[.,]?\s*$',
    re.IGNORECASE,
)
_LI_DOMAIN_RE = re.compile(r'\.(?:com|net|org|io|ca|co|us|biz)\s*$', re.IGNORECASE)

def _clean_company_for_search(name: str) -> str:
    s = name.strip()
    for _ in range(3):
        prev = s
        s = _LI_LEGAL_RE.sub("", s).strip().rstrip(".,").strip()
        if s == prev:
            break
    s = _LI_DOMAIN_RE.sub("", s).strip().rstrip(".,").strip()
    return s or name.strip()

# ── Config ────────────────────────────────────────────────────────────────────
GOOGLE_URL  = "https://www.google.com/search"
DDG_URL     = "https://html.duckduckgo.com/html/"

LINKEDIN_RE = re.compile(
    r'https?://(?:www\.)?linkedin\.com/in/[A-Za-z0-9\-_%]+'
)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

SESSION = requests.Session()


def _pick_ua() -> str:
    return random.choice(USER_AGENTS)


def _clean_url(url: str) -> str:
    """Strip Google redirect noise from a raw URL."""
    return url.split("&")[0].split("?")[0] if "&" in url or "?" in url else url


# ── Strategy 1: Google ────────────────────────────────────────────────────────

def _google_linkedin(name: str, company: str, title: str = "") -> str:
    title_word = title.split()[0] if title else ""
    query = f'site:linkedin.com/in "{name}" "{company}" {title_word}'.strip()
    headers = {"User-Agent": _pick_ua(), "Accept-Language": "en-US,en;q=0.9"}
    params  = {"q": query, "num": 5, "hl": "en"}

    try:
        r = SESSION.get(GOOGLE_URL, params=params, headers=headers, timeout=12)
        if r.status_code == 429:
            print(f"    [google] 429 rate-limit — sleeping 45s …")
            time.sleep(45)
            r = SESSION.get(GOOGLE_URL, params=params, headers=headers, timeout=12)

        if r.status_code != 200:
            return ""

        matches = LINKEDIN_RE.findall(r.text)
        if matches:
            return _clean_url(matches[0])
    except Exception:
        pass
    return ""


# ── Strategy 2: DuckDuckGo ────────────────────────────────────────────────────

def _ddg_linkedin(name: str, company: str) -> str:
    # Use cleaned company name for much better DDG results
    clean_co = _clean_company_for_search(company) if company else ""
    query = f'site:linkedin.com/in "{name}" "{clean_co}"' if clean_co else f'site:linkedin.com/in "{name}"'
    headers = {"User-Agent": _pick_ua()}
    data    = {"q": query}

    try:
        r = SESSION.post(DDG_URL, data=data, headers=headers, timeout=15)
        if r.status_code != 200:
            return ""

        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "linkedin.com/in/" in href:
                m = LINKEDIN_RE.search(href)
                if m:
                    return _clean_url(m.group(0))

        # Fallback: also check result text for linkedin URLs
        matches = LINKEDIN_RE.findall(r.text)
        if matches:
            return _clean_url(matches[0])
    except Exception:
        pass
    return ""


# ── Name variation helpers ────────────────────────────────────────────────────

_PREFIX_RE = re.compile(
    r'^\s*(Mr\.?|Ms\.?|Mrs\.?|Miss\.?|Dr\.?|Prof\.?|Sir)\s+',
    re.IGNORECASE,
)

# Cleaned credential set — same logic as ticker_resolver._SUFFIX_CLEAN
_LI_SUFFIX_CLEAN = {
    s.replace(".", "").replace(",", "") for s in {
        "jr", "jr.", "sr", "sr.", "ii", "iii", "iv",
        "phd", "ph.d", "ph.d.", "md", "m.d", "m.d.",
        "cfa", "c.f.a", "c.f.a.", "cpa", "c.p.a", "c.p.a.",
        "mba", "m.b.a", "m.b.a.", "esq", "esq.",
        "jd", "j.d", "j.d.", "llm", "ll.m", "ll.m.",
    }
}


def _strip_name_credentials(name: str) -> str:
    """
    Strip honorific prefixes, trailing credential suffixes, and comma-delimited
    credentials from a name string.

    "Mr. William J. Burns CPA, M.B.A." → "William J. Burns"
    "Mr. Michael J. Sardano J.D."      → "Michael J. Sardano"
    "Dr. Lindsay Allan Rosenwald M.D." → "Lindsay Allan Rosenwald"
    """
    # 1. Strip honorific prefix
    clean = _PREFIX_RE.sub("", name).strip()

    # 2. Strip everything from the first comma that introduces credentials
    #    e.g. "Burns CPA, M.B.A." → keep "Burns CPA" then step 3 strips "CPA"
    if "," in clean:
        before = clean.split(",")[0].strip()
        if len(before.split()) >= 2:
            clean = before

    # 3. Strip trailing credential tokens word-by-word
    parts = clean.split()
    while parts:
        tok = parts[-1].rstrip(".,").lower().replace(".", "").replace(",", "")
        if tok in _LI_SUFFIX_CLEAN:
            parts.pop()
        else:
            break

    return " ".join(parts).strip()


def _name_variations(full_name: str) -> list[str]:
    """
    Return a list of name strings to try for LinkedIn search, most specific first.

    Examples:
      "Mr. Frank M. DeMaria"              → ["Frank M. DeMaria", "Frank DeMaria"]
      "Mr. William J. Burns CPA, M.B.A."  → ["William J. Burns", "William Burns"]
      "Mr. Barry Scott Sloane"            → ["Barry Scott Sloane", "Barry Sloane"]
      "Ms. Vanessa Guzman-Clark CPA, MBA" → ["Vanessa Guzman-Clark"]  (only 2 parts → no extra)
    """
    clean = _strip_name_credentials(full_name)
    variations: list[str] = []
    seen: set[str] = set()

    def _add(v: str) -> None:
        v = v.strip()
        if v and v.lower() not in seen:
            seen.add(v.lower())
            variations.append(v)

    _add(clean)

    parts = clean.split()
    if len(parts) >= 3:
        # First + Last only (drop any middle names/initials)
        _add(f"{parts[0]} {parts[-1]}")

    return variations


# ── Public API ────────────────────────────────────────────────────────────────

def find_linkedin_url(
    name: str,
    company: str,
    title: str = "",
    sleep_range: tuple[float, float] = (1.0, 2.5),
) -> str:
    """
    Return the best-guess LinkedIn /in/ URL for *name* at *company*.
    Uses DuckDuckGo only (Google disabled — rate-limits immediately).

    Tries:
      - Each name variation (full → first+last)
      - With both the raw and the cleaned company name
      - Finally with just the name and no company (last resort)

    Returns empty string if nothing is found; never raises.
    """
    if not name:
        return ""

    sleep_s = random.uniform(*sleep_range)
    time.sleep(sleep_s)

    clean_co = _clean_company_for_search(company) if company else ""

    # Build (name_variant, company_variant) pairs to try
    name_vars = _name_variations(name)
    co_vars: list[str] = []
    if company and company not in co_vars:
        co_vars.append(company)
    if clean_co and clean_co not in co_vars:
        co_vars.append(clean_co)
    # First word of clean company (e.g. "Backblaze" from "Backblaze, Inc.")
    first_word = clean_co.split()[0] if clean_co else ""
    if first_word and first_word not in co_vars and len(first_word) > 3:
        co_vars.append(first_word)
    co_vars.append("")  # name-only fallback

    tried: set[tuple[str, str]] = set()

    for name_v in name_vars:
        for co_v in co_vars:
            key = (name_v.lower(), co_v.lower())
            if key in tried:
                continue
            tried.add(key)

            result = _ddg_linkedin(name_v, co_v)
            if result:
                return result

            time.sleep(random.uniform(0.4, 0.8))

    return ""
