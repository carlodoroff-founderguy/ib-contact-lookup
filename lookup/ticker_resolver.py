"""
ticker_resolver.py
Resolve a stock ticker → company name + CEO/CFO names.
Uses yfinance as the primary source; falls back to a direct
Yahoo Finance JSON API call if the officers list is empty.
"""

from __future__ import annotations
import time
from typing import Optional

# ── yfinance (install via: pip install yfinance) ──────────────────────────────
try:
    import yfinance as yf
    _HAS_YFINANCE = True
except ImportError:
    _HAS_YFINANCE = False

import requests

# Titles we care about — order matters: CEO before CFO before fallbacks
TARGET_TITLES = ("ceo", "chief executive", "cfo", "chief financial",
                 "coo", "chief operating", "president")

_NAME_PREFIXES = {"mr", "ms", "mrs", "miss", "dr", "prof", "sir", "rev"}
_NAME_SUFFIXES = {
    "jr", "jr.", "sr", "sr.", "ii", "iii", "iv",
    "phd", "ph.d", "ph.d.", "md", "m.d", "m.d.",
    "cfa", "c.f.a", "c.f.a.", "cpa", "c.p.a", "c.p.a.",
    "mba", "m.b.a", "m.b.a.", "esq", "esq.",
    "jd", "j.d", "j.d.", "llm", "ll.m", "ll.m.",
    "cfa.", "cpa.", "mba.",
}
# Pre-built cleaned set for fast lookup — strips dots AND commas
_SUFFIX_CLEAN = {s.replace(".", "").replace(",", "") for s in _NAME_SUFFIXES}

YF_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ib-contact-lookup/1.0)"}


# ─────────────────────────────────────────────────────────────────────────────

def split_name(full_name: str) -> tuple[str, str]:
    """Return (first, last) stripping honourifics and credentials."""
    parts = full_name.strip().split()
    while parts and parts[0].rstrip(".").lower() in _NAME_PREFIXES:
        parts.pop(0)
    # Strip suffixes — handle trailing commas e.g. "CPA," before "M.B.A."
    while parts:
        token_clean = parts[-1].rstrip(".,").lower().replace(".", "").replace(",", "")
        if token_clean in _SUFFIX_CLEAN:
            parts.pop()
        else:
            break
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def is_target(title: str) -> bool:
    """
    Return True for CEO / CFO / COO / President-level titles.
    'Vice President' is excluded UNLESS the title also contains CEO/CFO/COO keywords
    (e.g. 'Senior VP & Chief Financial Officer' should still match).
    """
    lower = title.lower()
    # Always match on explicit C-suite keywords (even if title also says "VP")
    if any(kw in lower for kw in ("ceo", "chief executive", "cfo", "chief financial",
                                   "coo", "chief operating")):
        return True
    # Match "president" but NOT "vice president" / "vice-president"
    if "president" in lower:
        if "vice president" in lower or "vice-president" in lower:
            return False
        return True
    return False


def _officers_from_yfinance(ticker: str) -> Optional[dict]:
    """Primary path: yfinance library."""
    if not _HAS_YFINANCE:
        return None
    try:
        info = yf.Ticker(ticker).info
        if not info or info.get("quoteType") == "NONE":
            return None
        executives = [
            {"name": o.get("name", "").strip(), "title": o.get("title", "").strip()}
            for o in info.get("companyOfficers", [])
            if o.get("name") and o.get("title")
        ]
        return {
            "ticker":         ticker,
            "company":        info.get("longName") or info.get("shortName", ""),
            "website":        info.get("website", ""),
            "industry":       info.get("industry", ""),
            "city":           info.get("city", ""),
            "state":          info.get("state", ""),
            "country":        info.get("country", ""),
            "employee_count": info.get("fullTimeEmployees", ""),
            "executives":     executives,
        }
    except Exception:
        return None


def _officers_from_yahoo_api(ticker: str) -> Optional[dict]:
    """Fallback path: direct Yahoo Finance API (no library needed)."""
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
        params = {"modules": "assetProfile,price"}
        r = requests.get(url, params=params, headers=YF_HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        result = (data.get("quoteSummary") or {}).get("result") or []
        if not result:
            return None
        item = result[0]
        profile = item.get("assetProfile", {})
        price   = item.get("price", {})

        executives = [
            {"name": o.get("name", "").strip(), "title": o.get("title", "").strip()}
            for o in profile.get("companyOfficers", [])
            if o.get("name") and o.get("title")
        ]
        return {
            "ticker":         ticker,
            "company":        price.get("longName") or price.get("shortName", ""),
            "website":        profile.get("website", ""),
            "industry":       profile.get("industry", ""),
            "city":           profile.get("city", ""),
            "state":          profile.get("state", ""),
            "country":        profile.get("country", ""),
            "employee_count": profile.get("fullTimeEmployees", ""),
            "executives":     executives,
        }
    except Exception:
        return None


def _ticker_variants(ticker: str) -> list[str]:
    """
    Return a list of ticker symbols to try, most likely first.

    Handles Canadian exchange suffixes:
      - "ENW-CA"  → tries ENW-CA, ENW.V, ENW.TO, ENW.VN, ENW.CN, ENW.NE, ENWF
      - "AUUA-CA" → tries AUUA-CA, AUUA.V, AUUA.TO, AUUAF (OTC pink sheets)
      - "ARG-CA"  → ARG-CA, ARG.V, ARG.TO, ARG.VN, ARGF, etc.
      - "BILD.V"  → ["BILD.V"] (already has exchange suffix)

    Also tries stripping .V/.TO etc. and adding F for OTC fallback on any ticker.
    """
    variants: list[str] = [ticker]

    if ticker.endswith("-CA"):
        base = ticker[:-3]
        # Try Canadian exchange suffixes in priority order (TSX-V most common for small-caps)
        for suffix in (".V", ".TO", ".VN", ".CN", ".NE"):
            sym = base + suffix
            if sym not in variants:
                variants.append(sym)
        # OTC pink-sheet fallback: strip -CA and append F (e.g. AUUAF, ARGF)
        otc = base + "F"
        if otc not in variants:
            variants.append(otc)
        # Also try plain base (some symbols work without suffix)
        if base not in variants:
            variants.append(base)

    # For any ticker that already has a dot-suffix, also try the OTC F variant
    elif "." in ticker:
        base = ticker.rsplit(".", 1)[0]
        otc  = base + "F"
        if otc not in variants:
            variants.append(otc)

    return variants


def resolve_ticker(ticker: str) -> Optional[dict]:
    """
    Return company info + list of executives for *ticker*.
    Returns None if the ticker cannot be resolved.
    """
    ticker = ticker.strip().upper()

    # Try the ticker and exchange-suffix variants (handles ENW-CA → ENW.V etc.)
    info = None
    for sym in _ticker_variants(ticker):
        info = _officers_from_yfinance(sym)
        if info is None:
            info = _officers_from_yahoo_api(sym)
        if info is not None:
            break

    if info is None:
        return None

    # If officers list is still empty, try the Yahoo API as second fallback
    if not info["executives"] and _HAS_YFINANCE:
        fallback = _officers_from_yahoo_api(ticker)
        if fallback and fallback["executives"]:
            info["executives"] = fallback["executives"]

    # Filter to CEO/CFO/COO/President
    info["targets"] = [e for e in info["executives"] if is_target(e["title"])]
    return info
