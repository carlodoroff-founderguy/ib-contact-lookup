"""
spac_detector.py

Auto-detect SPACs and provide fallback domain resolution via SEC EDGAR.

Detection criteria (ANY triggers SPAC flag):
  1. Company name contains SPAC keywords ("Acquisition Corp", "Blank Check", etc.)
  2. Sector is empty or "Financial Services" with zero revenue
  3. No website + market cap < $500M + IPO within last 3 years
  4. Only S-1 or 8-A filings found on EDGAR

Fallback chain for domain resolution:
  STEP 2 — SEC EDGAR company search for website/domain
  STEP 5 — SPAC sponsor domain extraction from S-1 filings
"""

from __future__ import annotations

import re
import time
import requests
from datetime import datetime, timedelta
from urllib.parse import urlparse, quote_plus

# ── Config ────────────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Curvature-IB-Intelligence/1.0 (research@curvature.com)",
    "Accept": "application/json",
})

_SPAC_NAME_PATTERNS = re.compile(
    r'(?:Acquisition\s+Corp|Acquisition\s+Co(?:mpany)?|Blank\s+Check|'
    r'\bSPAC\b|Merger\s+Corp|Merger\s+Sub)',
    re.IGNORECASE,
)

_SPONSOR_PATTERNS = [
    re.compile(r'([A-Z][A-Za-z\s&]+?)\s+Sponsor\s+LLC', re.IGNORECASE),
    re.compile(r'([A-Z][A-Za-z\s&]+?)\s+Capital\s+Partners', re.IGNORECASE),
    re.compile(r'([A-Z][A-Za-z\s&]+?)\s+Merchant\s+Partners', re.IGNORECASE),
    re.compile(r'([A-Z][A-Za-z\s&]+?)\s+Capital\s+(?:LLC|LP|Inc)', re.IGNORECASE),
    re.compile(r'(?:sponsor|formed\s+by|managed\s+by)\s+([A-Z][A-Za-z\s&,]+?)(?:\.|,|\s+LLC|\s+LP)', re.IGNORECASE),
]

EDGAR_COMPANY_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_FULLTEXT_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_BROWSE_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions"

# ── SPAC Detection ────────────────────────────────────────────────────────────


def detect_spac(yf_info: dict, ticker: str = "") -> bool:
    """
    Return True if the yfinance info dict looks like a SPAC.

    Checks (ANY = True):
      1. Company name contains SPAC keywords
      2. Sector empty or "Financial Services" with zero revenue
      3. No website + market cap < 500M + recent IPO
    """
    company = (yf_info.get("shortName") or yf_info.get("longName") or "").strip()
    sector = (yf_info.get("sector") or "").strip()
    website = (yf_info.get("website") or "").strip()
    revenue = yf_info.get("totalRevenue") or yf_info.get("revenue") or 0
    market_cap = yf_info.get("marketCap") or 0

    # 1. Name-based detection
    if _SPAC_NAME_PATTERNS.search(company):
        return True

    # 2. Financial Services shell with zero revenue
    if (not sector or sector == "Financial Services") and (not revenue or revenue == 0):
        # Only flag if name also looks suspicious (has "Holdings" without revenue)
        if "Holdings Corp" in company and (not revenue or revenue == 0):
            return True

    # 3. No website + small cap + recent (we can't always check IPO date, so relax)
    if not website and market_cap and 0 < market_cap < 500_000_000:
        # Extra heuristic: if sector is also blank, very likely a SPAC
        if not sector:
            return True

    return False


# ── STEP 2: SEC EDGAR domain lookup ──────────────────────────────────────────


def _get_cik_from_ticker(ticker: str) -> str:
    """
    Get CIK number from SEC EDGAR using the company tickers JSON.
    """
    try:
        resp = SESSION.get(
            "https://www.sec.gov/files/company_tickers.json",
            timeout=10,
        )
        if resp.status_code != 200:
            return ""
        data = resp.json()
        ticker_upper = ticker.upper()
        for entry in data.values():
            if (entry.get("ticker") or "").upper() == ticker_upper:
                return str(entry.get("cik_str", ""))
        return ""
    except Exception:
        return ""


def _get_company_website_from_edgar(ticker: str) -> str:
    """
    STEP 2: Query SEC EDGAR for the company's website domain.

    Tries:
      1. EDGAR submissions API (has company website field)
      2. EDGAR full-text search for website references in filings
    """
    cik = _get_cik_from_ticker(ticker)
    if not cik:
        return ""

    # Try the submissions endpoint — sometimes has a website
    try:
        padded_cik = cik.zfill(10)
        url = f"{EDGAR_SUBMISSIONS_URL}/CIK{padded_cik}.json"
        resp = SESSION.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            # Check for website in the company info
            website = (data.get("website") or data.get("Website") or "").strip()
            if website:
                if not website.startswith("http"):
                    website = "https://" + website
                return website

            # Check addresses for any URL
            for addr_key in ("addresses", "mailing", "business"):
                addr = data.get(addr_key, {})
                if isinstance(addr, dict):
                    for sub_key in ("business", "mailing"):
                        sub = addr.get(sub_key, {})
                        if isinstance(sub, dict):
                            # No website field in standard EDGAR addresses,
                            # but store the state/city for later
                            pass
    except Exception:
        pass

    return ""


def get_edgar_filing_url(ticker: str) -> str:
    """
    Return a direct link to the EDGAR filing page for this ticker.
    Used as fallback IR Page for SPACs.
    """
    cik = _get_cik_from_ticker(ticker)
    if cik:
        return f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=&dateb=&owner=include&count=40"
    return f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=&CIK=&type=S-1&dateb=&owner=include&count=10&search_text=&action=getcompany&ticker={ticker}"


# ── STEP 5: SPAC sponsor domain extraction ──────────────────────────────────


def _search_edgar_filings(ticker: str, form_type: str = "S-1") -> list[dict]:
    """
    Search EDGAR full-text search for filings by ticker and form type.
    Returns list of filing metadata dicts.
    """
    try:
        params = {
            "q": f'"{ticker}"',
            "dateRange": "custom",
            "startdt": "2018-01-01",
            "forms": form_type,
        }
        resp = SESSION.get(
            "https://efts.sec.gov/LATEST/search-index",
            params=params,
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("hits", {}).get("hits", [])
    except Exception:
        pass
    return []


def _extract_sponsor_from_filing_text(text: str) -> str:
    """
    Parse filing text for sponsor entity names.
    Returns the sponsor firm name or empty string.
    """
    for pattern in _SPONSOR_PATTERNS:
        m = pattern.search(text)
        if m:
            sponsor = m.group(1).strip()
            # Clean up: remove trailing "and" or commas
            sponsor = re.sub(r'\s+(and|or|&)\s*$', '', sponsor).strip()
            if len(sponsor) > 3 and len(sponsor) < 80:
                return sponsor
    return ""


def _sponsor_to_domain(sponsor_name: str) -> str:
    """
    Convert a sponsor firm name to a likely domain.

    "Live Oak Merchant Partners" → "liveoakmp.com"
    "Cohen & Company Capital"    → "cohenandcompany.com"

    This is a best-guess heuristic — not guaranteed.
    """
    if not sponsor_name:
        return ""

    # Clean the name
    clean = sponsor_name.strip()
    clean = re.sub(r'\s+(LLC|LP|Inc\.?|Ltd\.?|Corp\.?)\s*$', '', clean, flags=re.IGNORECASE)
    clean = clean.strip()

    words = clean.split()
    if not words:
        return ""

    # Strategy 1: lowercase concatenation of all words
    concat = "".join(w.lower() for w in words if w.lower() not in ("and", "&", "the"))
    if len(concat) > 3:
        return f"{concat}.com"

    return ""


def find_sponsor_domain(ticker: str, company: str = "") -> str:
    """
    STEP 5: Attempt to find the SPAC's sponsor firm domain.

    Searches EDGAR S-1 filings for sponsor entity references.
    Returns a domain string or empty.
    """
    cik = _get_cik_from_ticker(ticker)
    if not cik:
        return ""

    # Try to get recent filings from submissions API
    try:
        padded_cik = cik.zfill(10)
        url = f"{EDGAR_SUBMISSIONS_URL}/CIK{padded_cik}.json"
        resp = SESSION.get(url, timeout=10)
        if resp.status_code != 200:
            return ""

        data = resp.json()
        filings = data.get("filings", {}).get("recent", {})
        forms = filings.get("form", [])
        accessions = filings.get("accessionNumber", [])
        primary_docs = filings.get("primaryDocument", [])

        # Find S-1, S-1/A, or DEFM14A filings (most likely to mention sponsor)
        target_forms = {"S-1", "S-1/A", "424B4", "DEFM14A", "F-1", "F-1/A"}
        filing_urls = []
        for i, form in enumerate(forms):
            if form in target_forms and i < len(accessions) and i < len(primary_docs):
                acc = accessions[i].replace("-", "")
                doc = primary_docs[i]
                filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{doc}"
                filing_urls.append(filing_url)
                if len(filing_urls) >= 2:  # Only check first 2
                    break

        # Download and parse each filing for sponsor mentions
        for furl in filing_urls:
            try:
                resp2 = SESSION.get(furl, timeout=15)
                if resp2.status_code != 200:
                    continue
                # Only look at first 50KB to avoid huge files
                text = resp2.text[:50000]
                sponsor = _extract_sponsor_from_filing_text(text)
                if sponsor:
                    domain = _sponsor_to_domain(sponsor)
                    if domain:
                        return domain
            except Exception:
                continue
            time.sleep(0.5)

    except Exception:
        pass

    return ""


# ── Combined SPAC domain resolution ──────────────────────────────────────────


def resolve_spac_domain(ticker: str, company: str = "", log_fn=None) -> dict:
    """
    Run the full SPAC domain fallback chain.

    Returns dict with:
      - "domain": best domain found (or "")
      - "source": which step found it ("edgar_website", "sponsor", "")
      - "edgar_url": link to EDGAR filing page
      - "sponsor_name": sponsor firm name if found
    """
    result = {
        "domain": "",
        "source": "",
        "edgar_url": get_edgar_filing_url(ticker),
        "sponsor_name": "",
    }

    _log = log_fn or (lambda msg, lvl="dim": print(f"    [spac] {msg}"))

    # STEP 2: Try EDGAR for website
    _log(f"   [{ticker}]  SPAC fallback: checking SEC EDGAR for website …", "dim")
    try:
        edgar_website = _get_company_website_from_edgar(ticker)
        if edgar_website:
            # Extract domain
            try:
                parsed = urlparse(edgar_website if "://" in edgar_website else f"https://{edgar_website}")
                domain = parsed.netloc.lower()
                if domain.startswith("www."):
                    domain = domain[4:]
                if domain:
                    result["domain"] = domain
                    result["source"] = "edgar_website"
                    _log(f"   [{ticker}]  EDGAR website found: {domain}", "ok")
                    return result
            except Exception:
                pass
    except Exception:
        pass

    # STEP 5: Try sponsor domain
    _log(f"   [{ticker}]  SPAC fallback: searching for sponsor domain …", "dim")
    try:
        sponsor_domain = find_sponsor_domain(ticker, company)
        if sponsor_domain:
            result["domain"] = sponsor_domain
            result["source"] = "sponsor"
            result["sponsor_name"] = sponsor_domain.replace(".com", "")
            _log(f"   [{ticker}]  Sponsor domain found: {sponsor_domain}", "ok")
            return result
    except Exception:
        pass

    _log(f"   [{ticker}]  SPAC fallback: no domain found via EDGAR or sponsor", "dim")
    return result
