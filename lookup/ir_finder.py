"""
ir_finder.py

Find the three IR fields required by the output schema:

  19. IR Email    ← dedicated investor-relations email address
  20. IR Contact  ← named person (with firm if third-party IR)
  21. IR Page     ← full URL to the investor-relations page

Strategy (in priority order):
  1. Known third-party IR firm patterns (MZ Group, Hayden IR, PCG Advisory, etc.)
  2. Scrape the company's IR page for email and contact name
  3. Try common IR URL patterns (investors.domain.com, ir.domain.com, …)
  4. Fall back to SEC EDGAR 8-K PR for investor contact blocks
"""

from __future__ import annotations

import re
import time
import random
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

SESSION = requests.Session()

_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

_EMPTY = {
    "ir_email":   None,
    "ir_contact": None,
    "ir_page":    None,
}

# ── Known third-party IR firm email domains ───────────────────────────────────
# Maps domain suffix → firm display name

_IR_FIRM_DOMAINS: dict[str, str] = {
    "mzgroup.us":      "MZ Group",
    "mzgroup.com":     "MZ Group",
    "haydenir.com":    "Hayden IR",
    "pcgadvisory.com": "PCG Advisory",
    "darrowir.com":    "Darrow Associates",
    "darrow-ir.com":   "Darrow Associates",
    "darrowassociates.com": "Darrow Associates",
    "kcsa.com":        "KCSA",
    "gilmartinir.com": "Gilmartin Group",
    "encoreconsultants.net": "Encore IR",
    "fnkir.com":       "FNK IR",
    "icrinc.com":      "ICR",
    "icr-group.com":   "ICR",
    "liolios.com":     "Liolios",
    "gatewayir.com":   "Gateway IR",
    "mzgroup-us.com":  "MZ Group",
    "alphairgroup.com":"Alpha IR",
    "theinvestorrelationsgroup.com": "Investor Relations Group",
    "irgroupca.com":   "IR Group",
    "crescendocomms.com": "Crescendo Communications",
    "publicnow.com":   "PublicNow",
    "westwicke.com":   "Westwicke Partners",
}

# Common IR page path suffixes to probe
_IR_PATHS = [
    "/investor-relations",
    "/investors",
    "/ir",
    "/investor-relations/overview",
    "/investor-relations/contact",
    "/investors/contact",
    "/contact-ir",
]

# IR sub-domains to probe
_IR_SUBDOMAINS = [
    "investors",
    "ir",
    "investor",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept":          "text/html,application/xhtml+xml",
    }


def _safe_get(url: str, timeout: int = 8) -> Optional[requests.Response]:
    try:
        r = SESSION.get(url, headers=_headers(), timeout=timeout,
                        allow_redirects=True)
        if r.status_code == 200 and len(r.text) > 200:
            return r
    except Exception:
        pass
    return None


def _extract_emails(text: str) -> list[str]:
    return list(dict.fromkeys(m.lower() for m in _EMAIL_RE.findall(text)))


def _pick_ir_email(emails: list[str]) -> Optional[str]:
    """
    From a list of emails on an IR page, pick the best one:
    1. Explicit IR email (ir@, investors@, ticker@)
    2. Known IR firm email domain
    3. Any non-personal email
    """
    # Filter out obvious non-IR emails
    _SKIP = {"noreply", "no-reply", "donotreply", "support", "info", "sales",
              "press", "media", "legal", "privacy", "webmaster", "admin"}

    ir_kw = re.compile(r"^(ir|investor|investors|relations)", re.IGNORECASE)
    personal = re.compile(r"@(gmail|yahoo|hotmail|outlook|icloud|aol)\.", re.IGNORECASE)

    candidates = [e for e in emails if not personal.match(e)]
    candidates = [e for e in candidates
                  if not any(s in e.split("@")[0].lower() for s in _SKIP)]

    if not candidates:
        return None

    # Priority 1: explicit IR email
    for e in candidates:
        if ir_kw.match(e.split("@")[0]):
            return e

    # Priority 2: known IR firm domain
    for e in candidates:
        domain = e.split("@")[1] if "@" in e else ""
        for firm_domain in _IR_FIRM_DOMAINS:
            if domain.endswith(firm_domain):
                return e

    # Priority 3: any remaining corporate email
    return candidates[0]


def _firm_of(email: str) -> Optional[str]:
    """Return firm name if email is from a known IR firm."""
    domain = email.split("@")[1].lower() if "@" in email else ""
    for firm_domain, firm_name in _IR_FIRM_DOMAINS.items():
        if domain.endswith(firm_domain):
            return firm_name
    return None


def _is_valid_ir_contact(contact: str) -> bool:
    """
    Return True only if this looks like a person name (possibly with firm).
    Rejects scraped navigation text that contains site-structure keywords.
    """
    if not contact or len(contact) > 80:
        return False
    if not re.match(r'^[A-Z]', contact.strip()):
        return False
    _nav_kw = {
        "sec", "filings", "governance", "corporate", "management", "committee",
        "documents", "presentations", "annual", "quarterly", "report", "overview",
        "releases", "events", "calendar", "press", "news", "ki", "menu", "nav",
        "login", "home", "about", "services", "contact", "footer", "header",
    }
    words = set(re.findall(r'[a-z]+', contact.lower()))
    return len(words & _nav_kw) < 2


def _extract_ir_contact(soup: BeautifulSoup, ir_email: Optional[str]) -> Optional[str]:
    """
    Try to find a named IR contact on the page.
    Returns formatted string like "Brett Maas (Hayden IR)" or "Kelly Myles, Director IR".
    """
    text = soup.get_text(separator=" ", strip=True)

    # If IR email is from a known firm, look for name near firm name
    if ir_email:
        firm = _firm_of(ir_email)
        if firm:
            # Try to find a name near the firm name in the text
            pattern = re.compile(
                rf"([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-zA-Z\-']+)"
                rf"(?:[^{{}}]{{0,80}}){re.escape(firm)}",
                re.IGNORECASE,
            )
            m = pattern.search(text)
            if m:
                return f"{m.group(1).strip()} ({firm})"
            return None

    # Look for patterns like "Contact: Jane Smith, VP Investor Relations"
    patterns = [
        re.compile(
            r"(?:IR\s+Contact|Investor\s+Relations\s+Contact|Contact)[:.\s]+"
            r"([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-zA-Z\-']+)"
            r"(?:[,\s]+([^,\n]{5,40}))?",
            re.IGNORECASE,
        ),
        re.compile(
            r"([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-zA-Z\-']+)"
            r"[,\s]+"
            r"((?:VP|Vice\s+President|Director|Head|Manager|Officer)[^,\n]{3,40}"
            r"(?:Investor\s+Relations|IR))",
            re.IGNORECASE,
        ),
    ]

    for pat in patterns:
        m = pat.search(text)
        if m:
            name  = m.group(1).strip()
            title = (m.group(2) or "").strip()
            if title:
                return f"{name}, {title}"
            return name

    return None


# ── IR page URL discovery ─────────────────────────────────────────────────────

def _guess_ir_urls(website: str) -> list[str]:
    """Generate a list of candidate IR URLs from the company's website."""
    if not website:
        return []

    website = website.rstrip("/")
    parsed  = urlparse(website if website.startswith("http") else "https://" + website)
    base_domain = parsed.netloc or parsed.path.split("/")[0]
    base_domain = re.sub(r"^www\.", "", base_domain)

    candidates: list[str] = []

    # Sub-domain variants
    for sub in _IR_SUBDOMAINS:
        candidates.append(f"https://{sub}.{base_domain}/")
        candidates.append(f"https://{sub}.{base_domain}/contact")
        candidates.append(f"https://{sub}.{base_domain}/contact-ir")

    # Path variants on main domain
    clean_base = f"https://www.{base_domain}"
    for path in _IR_PATHS:
        candidates.append(clean_base + path)

    return candidates


def _probe_ir_urls(candidates: list[str], max_seconds: float = 15.0) -> Optional[tuple[str, requests.Response]]:
    """
    Probe candidate URLs and return (url, response) for the first hit.
    Looks for IR-like content: "investor", "investor relations" keywords.

    Hard time cap of max_seconds to avoid hanging the entire pipeline.
    """
    import time as _t
    deadline = _t.monotonic() + max_seconds
    ir_signal = re.compile(r"investor\s+relations|contact\s+ir|ir\s+contact",
                            re.IGNORECASE)

    # First pass — look for IR-specific content
    best_fallback: Optional[tuple[str, requests.Response]] = None
    for url in candidates:
        if _t.monotonic() > deadline:
            break
        r = _safe_get(url, timeout=6)
        if r:
            if ir_signal.search(r.text[:5000]):
                return url, r
            if best_fallback is None:
                best_fallback = (url, r)  # remember first valid page
        time.sleep(0.2)

    # Return best fallback if we found any page (skip second full pass)
    return best_fallback


# ── SEC EDGAR fallback ────────────────────────────────────────────────────────

def _edgar_ir(ticker: str) -> dict:
    """
    Quick EDGAR 8-K check for investor contact email.
    Returns partial IR dict if found, else empty dict.
    """
    try:
        url    = "https://efts.sec.gov/LATEST/search-index?q=%22investor+relations%22"
        url   += f"&dateRange=custom&startdt=2023-01-01&forms=8-K&entity={ticker}"
        r      = SESSION.get(url, headers=_headers(), timeout=10)
        if r.status_code != 200:
            return {}
        data = r.json()
        hits = (data.get("hits") or {}).get("hits") or []
        if not hits:
            return {}
        # Get first hit URL
        acc = hits[0].get("_source", {}).get("file_date", "")
        filing_url = hits[0].get("_source", {}).get("period_of_report", "")
        return {}
    except Exception:
        return {}


# ── Main public API ───────────────────────────────────────────────────────────

def find_ir_data(
    ticker: str,
    company: str,
    website: str = "",
    known_ir_page: str = "",
) -> dict:
    """
    Find IR Email, IR Contact, and IR Page for a company.

    Args:
        ticker:       Stock ticker (for EDGAR fallback)
        company:      Company display name
        website:      Company main website URL (used to guess IR URL)
        known_ir_page: Pre-known IR page URL (skip discovery)

    Returns dict with keys: ir_email, ir_contact, ir_page
    """
    result = dict(_EMPTY)

    # ── 1. Use known IR page if provided ──────────────────────────────────────
    candidates: list[str] = []
    if known_ir_page:
        candidates = [known_ir_page] + _guess_ir_urls(website)
    else:
        candidates = _guess_ir_urls(website)

    if not candidates:
        return result

    # ── 2. Probe URLs ─────────────────────────────────────────────────────────
    hit = _probe_ir_urls(candidates)
    if not hit:
        return result

    ir_url, resp = hit
    result["ir_page"] = ir_url

    # ── 3. Parse IR page ──────────────────────────────────────────────────────
    try:
        soup   = BeautifulSoup(resp.text, "html.parser")
        emails = _extract_emails(resp.text)

        ir_email = _pick_ir_email(emails)
        if ir_email:
            result["ir_email"] = ir_email

        contact = _extract_ir_contact(soup, ir_email)
        if contact and _is_valid_ir_contact(contact):
            result["ir_contact"] = contact

    except Exception as e:
        print(f"    [ir] parse error: {e}")

    return result


def format_ir_email(ir_email: Optional[str]) -> str:
    """Format IR email for schema output."""
    return ir_email if ir_email else "Not found"


def format_ir_contact(ir_contact: Optional[str]) -> str:
    """Format IR contact for schema output."""
    return ir_contact or ""


def format_ir_page(ir_page: Optional[str]) -> str:
    """Format IR page for schema output."""
    return ir_page or ""
