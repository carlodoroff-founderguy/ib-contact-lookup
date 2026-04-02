"""
schema_builder.py

Assemble the complete 21-column output row from all enriched data.

Column order (exact, must match xlsx header):
  1  Company Name
  2  Ticker
  3  Industry
  4  Exchange
  5  Stock Price (Most Recent)
  6  Market Cap (Most Recent)
  7  Cash (Latest K)
  8  Cash (Latest Q)
  9  1M Share Volume
  10 1D $ Share Volume
  11 Cash from Ops (Latest K)
  12 Cash from Ops (Latest Q)
  13 CEO
  14 CFO
  15 CEO EMAIL
  16 CEO NUMBER
  17 CFO EMAIL
  18 CFO NUMBER
  19 IR Email
  20 IR Contact
  21 IR Page

Contact formatting rules (from spec):
  EMAIL:
    - work email   → "john.smith@company.com"
    - personal only → "email@gmail.com (no work provided)"
    - not found    → "Not found"
    - not on LI    → "Not on LinkedIn"

  PHONE:
    - work phone   → "work +1 646-779-0768"
    - mobile/direct → "+1 917-379-1470"
    - not found    → "Not found"
    - not on LI    → "Not on LinkedIn"
"""

from __future__ import annotations

import re
from typing import Optional

# ── Personal email domain detection ──────────────────────────────────────────

_PERSONAL_DOMAINS = {
    "gmail", "yahoo", "hotmail", "outlook", "icloud",
    "aol", "protonmail", "me", "mac", "live", "msn",
    "ymail", "googlemail",
}


def _is_personal_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    domain_part = email.split("@")[1].lower()
    # "gmail.com" → "gmail"
    base = domain_part.rsplit(".", 1)[0]
    return base in _PERSONAL_DOMAINS


# ── Phone type detection ──────────────────────────────────────────────────────

def _is_work_phone(phone_type: str, phone_number: str) -> bool:
    """
    Return True if this phone should be prefixed with 'work '.
    A phone is 'work' if its type field says work/office/direct,
    or if it's the only number on a corporate SalesQL result.
    """
    t = (phone_type or "").lower()
    return any(kw in t for kw in ("work", "office", "direct", "hq"))


# ── Contact field formatters ──────────────────────────────────────────────────

def format_email(
    work_email: str,
    personal_email: str,
    direct_email: str,
    not_on_linkedin: bool = False,
    not_found: bool = False,
    company_domain: str = "",
) -> str:
    """
    Apply the email priority rules from the spec and return the formatted string.

    company_domain (optional): the primary domain of the company website.
    If provided, emails whose domain doesn't match get flagged as (ext. domain).
    """
    if not_on_linkedin:
        return "Not on LinkedIn"

    # Collect all non-empty emails
    we = (work_email or "").strip()
    de = (direct_email or "").strip()
    pe = (personal_email or "").strip()

    def _email_domain(em: str) -> str:
        if "@" in em:
            return em.split("@")[-1].lower().lstrip("www.")
        return ""

    def _domain_mismatch(em: str) -> bool:
        """True if email is from a different company domain (possible past employer)."""
        if not company_domain or not em or _is_personal_email(em):
            return False
        em_dom   = _email_domain(em)
        corp_dom = company_domain.lower().lstrip("www.")
        # Strip protocol if present
        corp_dom = corp_dom.replace("https://", "").replace("http://", "").split("/")[0]
        if not em_dom or not corp_dom:
            return False
        # Allow subdomain matches (e.g. mail.curvature.com ↔ curvature.com)
        return not (em_dom == corp_dom or em_dom.endswith("." + corp_dom)
                    or corp_dom.endswith("." + em_dom))

    # Any work / direct email from the current company domain → clean return
    if we and not _is_personal_email(we):
        if _domain_mismatch(we):
            return f"{we} (no work email found — ext. domain)"
        return we
    if de and not _is_personal_email(de):
        if _domain_mismatch(de):
            return f"{de} (no work email found — ext. domain)"
        return de

    # Personal-only
    if pe and _is_personal_email(pe):
        return f"{pe} (no work email found — personal)"
    if we and _is_personal_email(we):
        return f"{we} (no work email found — personal)"

    # Anything leftover that wasn't flagged personal
    for candidate in [we, de, pe]:
        if candidate:
            if _domain_mismatch(candidate):
                return f"{candidate} (no work email found — ext. domain)"
            return candidate

    return "Not found"


def format_phone(
    phone: str,
    phone_type: str = "",
    not_on_linkedin: bool = False,
) -> str:
    """
    Apply the phone priority rules from the spec (updated: direct > work > mobile).

    phone_type from salesql_enricher: 'direct' | 'work' | 'mobile' | ''

    Formatting rules:
      - direct or work phone  → prefix with 'work '  (e.g. 'work +1 646-779-0768')
      - mobile / personal     → no prefix, raw number (e.g. '+1 917-379-1470')
      - not found             → 'Not found'
      - not on LinkedIn       → 'Not on LinkedIn'
    """
    if not_on_linkedin:
        return "Not on LinkedIn"

    p = (phone or "").strip()
    if not p:
        return "Not found"

    t = (phone_type or "").lower()
    # Per spec: only 'work' / 'office' / 'hq' get the "work " prefix.
    # 'direct' is a personal direct-dial line (no prefix, e.g. +1 917-379-1470).
    # 'mobile' / '' / unknown also get no prefix.
    is_business = any(kw in t for kw in ("work", "office", "hq"))

    if is_business:
        # Already prefixed?
        if p.lower().startswith("work "):
            return p
        # Ensure country code present
        if p.startswith("+"):
            return f"work {p}"
        else:
            return f"work +{p.lstrip('+')}"
    else:
        # Mobile / unknown — return as-is with + if it looks like a plain number
        if not p.startswith("+") and re.match(r"^\d", p):
            return f"+{p}"
        return p


# ── SalesQL result → formatted contact ────────────────────────────────────────

def _build_contact(enrichment: dict, company_domain: str = "") -> tuple[str, str]:
    """
    Given a SalesQL enrichment dict, return (formatted_email, formatted_phone).

    The enrichment dict has keys:
        work_email, personal_email, direct_email,
        phone, phone_type, source, linkedin_url
    """
    source = (enrichment.get("source") or "").lower()

    not_on_linkedin = "not_on_linkedin" in source or source == "not_on_linkedin"
    not_found = source in ("not_found", "no_url", "no_name", "error", "timeout",
                           "bad_params", "")

    email = format_email(
        work_email      = enrichment.get("work_email", ""),
        personal_email  = enrichment.get("personal_email", ""),
        direct_email    = enrichment.get("direct_email", ""),
        not_on_linkedin = not_on_linkedin,
        not_found       = not_found,
        company_domain  = company_domain,
    )

    # Phone handling — SalesQL currently returns a single 'phone' string.
    # We check if the enrichment has a 'phone_type' key; fall back to
    # inferring from the raw phone value itself.
    raw_phone   = (enrichment.get("phone") or "").strip()
    phone_type  = (enrichment.get("phone_type") or "").lower()

    # If SalesQL didn't return a phone_type, use heuristics:
    # - Numbers that look like US main lines (area code matches company) → work
    # - All others → unknown (no prefix added)
    phone = format_phone(
        phone           = raw_phone,
        phone_type      = phone_type,
        not_on_linkedin = not_on_linkedin,
    )

    return email, phone


# ── Exchange normalisation ─────────────────────────────────────────────────────

def _normalise_exchange(exchange: str, ticker: str) -> str:
    """Map Yahoo Finance exchange codes to display names."""
    if not exchange:
        exchange = ""
    e = exchange.upper()
    if "NMS" in e or "NGS" in e or "NASDAQ" in e:
        return "NASDAQ"
    if "NYQ" in e or "NYSE" in e:
        return "NYSE"
    if "OTC" in e or "PINK" in e or "GREY" in e or "OB" in e:
        return "US OTC"
    if "TSX" in e or ".TO" in ticker.upper():
        return "TSX"
    if "CVE" in e or ".V" in ticker.upper():
        return "TSX-V"
    if "CNQ" in e or ".CN" in ticker.upper():
        return "CSE"
    if exchange:
        return exchange
    # Infer from ticker suffix
    if ticker.upper().endswith("-CA"):
        return "TSX"
    return exchange or "US OTC"


# ── Main builder ──────────────────────────────────────────────────────────────

COLUMN_ORDER = [
    "Company Name",
    "Ticker",
    "Industry",
    "Exchange",
    "Stock Price (Most Recent)",
    "Market Cap (Most Recent)",
    "Cash (Latest K)",
    "Cash (Latest Q)",
    "1M Share Volume",
    "1D $ Share Volume",
    "Cash from Ops (Latest K)",
    "Cash from Ops (Latest Q)",
    "CEO",
    "CFO",
    "CEO EMAIL",
    "CEO NUMBER",
    "CFO EMAIL",
    "CFO NUMBER",
    "IR Email",
    "IR Contact",
    "IR Page",
]


def build_row(
    ticker:       str,
    company_info: dict,          # from ticker_resolver
    financials:   dict,          # from financial_fetcher
    executives:   list[dict],    # list of {role, name, title, enrichment}
    ir_data:      dict,          # from ir_finder
    company_domain: str = "",    # primary website domain for email validation
) -> dict:
    """
    Build the complete 21-column output row.

    executives should be:
    [
      {
        "role":       "CEO" | "CFO" | ...,
        "name":       "David S. Rosenblatt, MBA",
        "title":      "Chief Executive Officer",
        "enrichment": { work_email, personal_email, direct_email, phone, phone_type, source }
      },
      ...
    ]
    """
    # ── CEO / CFO lookup ──────────────────────────────────────────────────────
    ceo_info = next((e for e in executives if e.get("role") == "CEO"), None)
    cfo_info = next((e for e in executives if e.get("role") == "CFO"), None)

    def _name(e):   return (e or {}).get("name", "Not found")
    def _enrich(e): return (e or {}).get("enrichment", {})

    ceo_name  = _name(ceo_info)
    cfo_name  = _name(cfo_info) if cfo_info else "Not found"

    ceo_email, ceo_phone = _build_contact(_enrich(ceo_info), company_domain) if ceo_info else ("Not found", "Not found")
    cfo_email, cfo_phone = _build_contact(_enrich(cfo_info), company_domain) if cfo_info else ("Not found", "Not found")

    # ── Company info ──────────────────────────────────────────────────────────
    company_name = (company_info.get("company") or "").strip()
    industry     = company_info.get("industry", "") or ""
    exchange_raw = company_info.get("exchange", "") or ""
    exchange     = _normalise_exchange(exchange_raw, ticker)

    # ── Financial data ────────────────────────────────────────────────────────
    def _f(key): return financials.get(key)

    # ── Assemble row ──────────────────────────────────────────────────────────
    row = {
        "Company Name":              company_name,
        "Ticker":                    ticker,
        "Industry":                  industry,
        "Exchange":                  exchange,
        "Stock Price (Most Recent)": _f("stock_price"),
        "Market Cap (Most Recent)":  _f("market_cap"),
        "Cash (Latest K)":           _f("cash_annual"),
        "Cash (Latest Q)":           _f("cash_quarterly"),
        "1M Share Volume":           _f("volume_1m"),
        "1D $ Share Volume":         _f("volume_1d_dollar"),
        "Cash from Ops (Latest K)":  _f("ops_annual"),
        "Cash from Ops (Latest Q)":  _f("ops_quarterly"),
        "CEO":                       ceo_name,
        "CFO":                       cfo_name,
        "CEO EMAIL":                 ceo_email,
        "CEO NUMBER":                ceo_phone,
        "CFO EMAIL":                 cfo_email,
        "CFO NUMBER":                cfo_phone,
        "IR Email":                  ir_data.get("ir_email") or "Not found",
        "IR Contact":                ir_data.get("ir_contact") or "",
        "IR Page":                   ir_data.get("ir_page") or "",
    }

    return row


def empty_row(ticker: str, error_msg: str = "API Error") -> dict:
    """Return a row with all contact/financial fields set to error_msg."""
    row: dict = {col: error_msg for col in COLUMN_ORDER}
    row["Ticker"] = ticker
    row["Company Name"] = error_msg
    return row
