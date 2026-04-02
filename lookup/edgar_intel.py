"""
edgar_intel.py  —  SEC EDGAR intelligence layer

Retrieves recent filings (10-Q, 8-K, S-1, S-3) for a given ticker and extracts:
  • Lawyer / law firm contacts from "copies to:" sections of S-1 / S-3 filings
  • Investor Relations contact from 8-K EX-99.1 press release footers
  • Recent capital raises (last 12 months) with ROFR clause detection
  • One-sentence plain-English company context

SEC fair-use policy: max 10 req/sec — enforced via RATE_SLEEP between every call.
Required:  EDGAR_USER_NAME and EDGAR_USER_EMAIL in .env (or os environment).
Optional:  ANTHROPIC_API_KEY or OPENAI_API_KEY for AI-generated context sentence.
"""

from __future__ import annotations

import os
import re
import json
import time
from datetime import datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ── SEC fair-use headers (required by SEC policy) ─────────────────────────────
_EDGAR_NAME  = os.getenv("EDGAR_USER_NAME",  "IB Contact Lookup")
_EDGAR_EMAIL = os.getenv("EDGAR_USER_EMAIL", "contact@example.com")
EDGAR_HEADERS = {"User-Agent": f"{_EDGAR_NAME} {_EDGAR_EMAIL}"}

RATE_SLEEP = 0.11   # slightly above the 0.1s floor (10 req/sec max)


# ── Capital-raise type signals (order matters — more specific first) ──────────
_RAISE_TYPE_MAP = [
    (re.compile(r"\bIPO\b|\binitial\s+public\s+offering\b", re.I),                       "IPO"),
    (re.compile(r"\bat[\s\-]the[\s\-]market\b|\bATM\s+(?:offering|program|facility|sales|equity)\b", re.I), "ATM"),
    (re.compile(r"\bPIPE\b|\bprivate\s+investment\s+in\s+public\s+equity\b", re.I),      "PIPE"),
    (re.compile(r"\bRDO\b|\bregistered\s+direct\s+offering\b", re.I),                    "RDO"),
    (re.compile(r"\bfollow[\s\-]on\b|\bFPO\b", re.I),                                    "Follow-On"),
    (re.compile(r"\bconvertible\s+note\b|\bconvertible\s+debenture\b", re.I),            "Convertible Note"),
    (re.compile(r"\bshelf\s+takedown\b", re.I),                                           "Shelf Takedown"),
    (re.compile(r"\bprivate\s+placement\b", re.I),                                        "Private Placement"),
    (re.compile(r"\bunderwritten\s+(?:public\s+)?offering\b", re.I),                     "Underwritten Offering"),
    (re.compile(r"\bpublic\s+offering\b", re.I),                                          "Public Offering"),
]

# ── Amount regex — handles "$25M", "$25 million", "$25,000,000" ───────────────
_AMOUNT_RE = re.compile(
    r"\$\s*([\d,]+(?:\.\d+)?)\s*"
    r"(billion|million|thousand|B\b|M\b|K\b)?",
    re.IGNORECASE,
)

# ── Agent / underwriter extraction ───────────────────────────────────────────
# A firm name: 2-6 capitalized words, optionally ending with ", LLC / Inc / Corp"
_FIRM_NAME_CHUNK = (
    r"(?:[A-Z][A-Za-z0-9\.\-&/']+\s+){2,6}"       # 2-6 cap words
    r"(?:,\s+(?:LLC|Inc\.?|Corp\.?|Ltd\.?|L\.P\.|LLP|PLC)\s+)?"  # optional suffix
)
_AGENT_ROLE = (
    r"(?:the\s+)?(?:sole\s+|exclusive\s+|lead\s+|co[\s\-])?"
    r"(?:placement\s+agent|book[\s\-]?runner|underwriter|managing\s+underwriter"
    r"|financial\s+advisor|selling\s+agent)"
)
_ACTING_VERB = (
    r"(?:is|was|will\s+be|agreed?\s+to\s+act\s+as|acted?\s+as|"
    r"acting\s+as|has\s+agreed\s+to\s+act\s+as|to\s+serve\s+as|"
    r"served?\s+as|serving\s+as)"
)

# Pattern A: "Roth Capital Partners LLC acted as placement agent"
_AGENT_BEFORE_RE = re.compile(
    r"(" + _FIRM_NAME_CHUNK + r")"
    + _ACTING_VERB
    + r"\s+" + _AGENT_ROLE,
    re.IGNORECASE,
)
# Pattern B: "placement agent for this offering, Roth Capital Partners"
_AGENT_AFTER_RE = re.compile(
    _AGENT_ROLE
    + r"(?:[^,\n]{0,60})?"                             # optional filler
    r"[,\s]+(?:is|was|will\s+be|has\s+been|:)?\s*"
    r"(" + _FIRM_NAME_CHUNK + r")",
    re.IGNORECASE,
)

# ── ROFR classification ───────────────────────────────────────────────────────
# Explicitly absent — includes "not grant / not provide / no" before ROFR phrase
_ROFR_ABSENT_RE = re.compile(
    r"(?:"
    r"no\s+"                                         # "no right of first refusal"
    r"|without\s+(?:any\s+)?"                        # "without any right of first refusal"
    r"|waiv(?:e[sd]?|ing)\s+(?:any\s+)?"            # "waiving any right of first refusal"
    r"|not\s+(?:grant|provide|give|have|contain|include)\s+(?:\w+\s+){0,3}"  # "not grant ... right"
    r"|does\s+not\s+have\s+"
    r")"
    r"(?:any\s+)?(?:right\s+of\s+first\s+refusal|ROFR|first\s+refusal\s+right)",
    re.IGNORECASE,
)
# Explicitly present
_ROFR_PRESENT_RE = re.compile(
    r"right\s+of\s+first\s+refusal|"
    r"\bROFR\b|"
    r"\bfirst\s+refusal\b|"
    r"participation\s+right|"
    r"pro[\s\-]rata\s+right",
    re.IGNORECASE,
)

# ── Tail-fee detection ────────────────────────────────────────────────────────
_NO_TAIL_RE = re.compile(
    r"no\s+tail\s*(?:fee|period|provision|payment)?|"
    r"without\s+(?:a\s+)?tail",
    re.IGNORECASE,
)
_HAS_TAIL_RE = re.compile(
    r"\btail\s+(?:fee|period|provision|payment)\b",
    re.IGNORECASE,
)


# ── Low-level helpers ─────────────────────────────────────────────────────────

def _get(url: str, **kwargs) -> requests.Response:
    """Rate-limited GET with required EDGAR headers. Returns a dummy 503 on error."""
    time.sleep(RATE_SLEEP)
    try:
        return requests.get(url, headers=EDGAR_HEADERS, timeout=20, **kwargs)
    except Exception:
        r = requests.models.Response()
        r.status_code = 503
        return r


def _clean_text(raw: str) -> str:
    """
    Strip HTML/XML tags and normalize whitespace, preserving line breaks.
    Using newline as separator so paragraph/line structure is retained for
    downstream regex parsing (lawyer blocks, IR contact blocks, etc.).
    """
    try:
        soup = BeautifulSoup(raw, "lxml")
        text = soup.get_text(separator="\n")
    except Exception:
        text = re.sub(r"<[^>]+>", " ", raw)
    # Collapse whitespace within each line; drop blank lines
    lines = []
    for line in text.split("\n"):
        cleaned = re.sub(r"[ \t]+", " ", line).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


def _sentences_around(text: str, match: re.Match, n: int = 5) -> str:
    """Return up to *n* sentences of plain text centred on *match*."""
    flat = re.sub(r"\s+", " ", text)
    start = max(0, flat.rfind(".", 0, match.start()) + 1)
    end   = match.end()
    for _ in range(n):
        nxt = flat.find(".", end)
        if nxt == -1:
            break
        end = nxt + 1
    return flat[start:end].strip()


# ── Step 1 — Ticker → CIK ────────────────────────────────────────────────────

def _get_cik(ticker: str) -> Optional[str]:
    """Return the zero-padded 10-digit CIK for *ticker*, or None."""
    r = _get("https://www.sec.gov/files/company_tickers.json")
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    ticker_up = ticker.upper()
    for entry in data.values():
        if entry.get("ticker", "").upper() == ticker_up:
            return str(entry["cik_str"]).zfill(10)
    return None


# ── Step 2 — EDGAR submissions index ─────────────────────────────────────────

def _get_submissions(cik: str) -> dict:
    """Fetch the EDGAR submissions JSON for a given CIK."""
    r = _get(f"https://data.sec.gov/submissions/CIK{cik}.json")
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}


def _select_filings(
    submissions: dict,
    form_types: list[str],
    since_date: Optional[datetime] = None,
    limit: int = 5,
) -> list[dict]:
    """
    Return up to *limit* filings per form type from the submissions blob.
    Each entry carries: type, date, url (index page), doc_url (primary doc),
    accession, cik.
    """
    recent       = submissions.get("filings", {}).get("recent", {})
    forms        = recent.get("form",             [])
    dates        = recent.get("filingDate",       [])
    accessions   = recent.get("accessionNumber",  [])
    primary_docs = recent.get("primaryDocument",  [])
    cik          = str(submissions.get("cik", "")).lstrip("0") or "0"
    cik_padded   = cik.zfill(10)

    counts: dict[str, int] = {ft: 0 for ft in form_types}
    results: list[dict]    = []

    for i, form in enumerate(forms):
        if form not in form_types:
            continue
        if counts.get(form, 0) >= limit:
            continue

        filed = dates[i]        if i < len(dates) else ""
        if since_date and filed:
            try:
                if datetime.fromisoformat(filed) < since_date:
                    continue
            except ValueError:
                pass

        acc       = accessions[i]    if i < len(accessions)   else ""
        acc_clean = acc.replace("-", "")
        pdoc      = primary_docs[i] if i < len(primary_docs) else ""

        index_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik}/"
            f"{acc_clean}/{acc}-index.htm"
        )
        doc_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{pdoc}"
            if pdoc else ""
        )

        results.append({
            "type":      form,
            "date":      filed,
            "url":       index_url,
            "doc_url":   doc_url,
            "accession": acc,
            "cik":       cik,
        })
        counts[form] = counts.get(form, 0) + 1

    return results


# ── Filing index / exhibit resolution ────────────────────────────────────────

def _get_exhibit_url(
    cik: str,
    accession: str,
    exhibit_type: Optional[str] = None,
    fallback_doc_url: str = "",
) -> Optional[str]:
    """
    Parse the filing index page and return the URL of the primary document
    or a specific exhibit (e.g. 'EX-99.1').
    Falls back to *fallback_doc_url* when no specific exhibit is needed.
    """
    # If we only need the primary doc and already have its URL, use it directly.
    if not exhibit_type and fallback_doc_url:
        return fallback_doc_url

    acc_clean = accession.replace("-", "")
    idx_url   = (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/"
        f"{acc_clean}/{accession}-index.htm"
    )
    r = _get(idx_url)
    if r.status_code != 200:
        return fallback_doc_url or None

    soup  = BeautifulSoup(r.text, "lxml")
    table = soup.find("table", class_="tableFile") or soup.find("table")
    if not table:
        return fallback_doc_url or None

    first_url: Optional[str] = None

    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        # Collect all cell texts to find the document type column
        cell_texts = [c.get_text(strip=True).upper() for c in cells]
        doc_type   = ""
        href       = ""

        for idx_c, cell in enumerate(cells):
            ct = cell_texts[idx_c]
            # Type column: looks like "EX-99.1", "8-K", "S-1", etc.
            if re.match(r"^(EX-\d+[\.\d]*|10-[KQ]|8-K|S-[13]|424B\d*)$", ct):
                doc_type = ct
            link = cell.find("a", href=True)
            if link and not href:
                href = link["href"]

        if not href:
            continue
        if not href.startswith("http"):
            href = "https://www.sec.gov" + href

        if first_url is None:
            first_url = href

        if exhibit_type and doc_type == exhibit_type.upper():
            return href

    # No specific exhibit found — return primary doc
    if not exhibit_type:
        return first_url or fallback_doc_url
    return None


def _fetch_doc_text(url: str) -> str:
    """Download a filing document and return cleaned plain text."""
    if not url:
        return ""
    r = _get(url)
    if r.status_code != 200:
        return ""
    return _clean_text(r.text)


# ── Step 3 — Lawyer extraction from S-1 / S-3 ────────────────────────────────

_COPIES_TO_RE = re.compile(
    r"(?:copies?\s+to|with\s+a\s+copy\s+to)\s*:?\s*([\s\S]{50,2000}?)(?=\n{3,}|table\s+of\s+contents|prospectus\s+summary|dear\s+|sincerely|$)",
    re.IGNORECASE,
)
_ESQ_LINE_RE  = re.compile(
    r"^(.+?),?\s*(?:Esq\.?|Attorney(?:\s+at\s+Law)?|Partner|Counsel)[\s,]*$",
    re.IGNORECASE | re.MULTILINE,
)
_FIRM_LINE_RE = re.compile(
    r"^([A-Z][A-Za-z\s&,\.\-]+?(?:LLP|LLC|P\.C\.|PC|Attorneys\s+at\s+Law))\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def _extract_lawyers(text: str) -> list[dict]:
    """
    Parse the 'Copies to:' / 'With a copy to:' block near the top of an S-1/S-3.
    Returns a list of {name, firm, linkedin_url} dicts.
    """
    # Scan only the first ~8 000 characters (cover page area)
    snippet = text[:8000]

    copies_match = _COPIES_TO_RE.search(snippet)
    block = copies_match.group(1) if copies_match else snippet[:2000]

    # Find lawyer-name lines (contain Esq./Partner/Counsel)
    names_found = _ESQ_LINE_RE.findall(block)
    firms_found = _FIRM_LINE_RE.findall(block)

    lawyers: list[dict] = []
    seen: set[str]      = set()

    for i, raw_name in enumerate(names_found):
        name = raw_name.strip()
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        firm = firms_found[i].strip() if i < len(firms_found) else ""
        lawyers.append({"name": name, "firm": firm, "linkedin_url": ""})

    # Fallback: if regex found nothing, try line-by-line approach
    if not lawyers:
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        i = 0
        while i < len(lines):
            line = lines[i]
            if re.search(r"\bEsq\.?\b|\bAttorney\b|\bPartner\b|\bCounsel\b", line, re.I):
                name = re.sub(
                    r",?\s*(?:Esq\.?|Attorney(?:\s+at\s+Law)?|Partner|Counsel).*",
                    "", line, flags=re.I,
                ).strip()
                firm = ""
                for j in range(i + 1, min(i + 5, len(lines))):
                    candidate = lines[j]
                    if re.search(r"\bLLP\b|\bLLC\b|\bP\.C\.\b|\bPC\b", candidate, re.I):
                        firm = candidate
                        break
                    if re.match(r"[A-Z]", candidate) and not re.match(r"\d+", candidate):
                        firm = candidate
                        break
                if name and name.lower() not in seen:
                    seen.add(name.lower())
                    lawyers.append({"name": name, "firm": firm, "linkedin_url": ""})
            i += 1

    return lawyers


def _linkedin_search(name: str, firm: str) -> str:
    """
    Search DuckDuckGo for a LinkedIn profile URL.
    Returns the first linkedin.com/in/... hit, or empty string.
    """
    if not name:
        return ""
    query = f'"{name}" "{firm}" site:linkedin.com/in' if firm else f'"{name}" lawyer site:linkedin.com/in'
    try:
        time.sleep(RATE_SLEEP)
        r = requests.post(
            "https://html.duckduckgo.com/html/",
            data={"q": query},
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; ib-contact-lookup/1.0)",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=15,
        )
        if r.status_code == 200:
            m = re.search(r"https?://(?:www\.)?linkedin\.com/in/[\w\-]+", r.text)
            if m:
                return m.group(0)
    except Exception:
        pass
    return ""


# ── Step 4 — IR contact from 8-K press releases ───────────────────────────────

_IR_HEADER_RE = re.compile(
    r"(?:investor\s+(?:relations|contact)|media\s+contact|for\s+(?:more\s+)?information\s+contact)"
    r"(.{0,800})",
    re.IGNORECASE | re.DOTALL,
)
_EMAIL_RE = re.compile(r"[\w\.\+\-]+@[\w\.\-]+\.[a-zA-Z]{2,6}")
_PHONE_RE = re.compile(r"(?:\+1[\s\-\.]?)?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}")
_IR_NAME_RE = re.compile(
    r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*$",
    re.MULTILINE,
)


def _extract_ir_contact(text: str) -> dict:
    """
    Locate the investor-relations / media-contact block, typically at the
    bottom of an 8-K EX-99.1 press release. Returns {name, title, email, phone}.
    """
    empty: dict = {"name": "", "title": "", "email": "", "phone": ""}

    # Search the last 3 000 characters first (IR block is usually at the foot)
    tail    = text[-3000:]
    m_block = _IR_HEADER_RE.search(tail)
    block   = m_block.group(0) if m_block else tail[-1500:]

    email_m = _EMAIL_RE.search(block)
    phone_m = _PHONE_RE.search(block)
    name_m  = _IR_NAME_RE.search(block)

    result = dict(empty)
    if email_m:
        result["email"] = email_m.group(0).strip()
    if phone_m:
        result["phone"] = phone_m.group(0).strip()
    if name_m:
        result["name"]  = name_m.group(1).strip()
    return result


# ── Step 5 — Capital raises + ROFR detection ─────────────────────────────────

def _parse_amount(text: str) -> float:
    """Extract the largest plausible dollar amount from *text*. Returns 0.0 if none found."""
    # Prefer amounts mentioned near "gross proceeds", "aggregate", "offering size"
    context_re = re.compile(
        r"(?:aggregate\s+(?:gross\s+)?proceeds|gross\s+proceeds|offering\s+size|"
        r"raised?|raising|up\s+to)\s+(?:of\s+)?"
        r"\$\s*([\d,]+(?:\.\d+)?)\s*(billion|million|thousand|B\b|M\b|K\b)?",
        re.IGNORECASE,
    )
    best = 0.0

    def _scale(raw: float, unit: str) -> float:
        u = (unit or "").lower().rstrip(".")
        if u in ("billion", "b"):    return raw * 1_000_000_000
        if u in ("million", "m"):    return raw * 1_000_000
        if u in ("thousand", "k"):   return raw * 1_000
        # bare number — heuristic: <10 000 → treat as millions (common shorthand)
        if raw < 10_000:             return raw * 1_000_000
        return raw

    for m in context_re.finditer(text):
        v = _scale(float(m.group(1).replace(",", "")), m.group(2) or "")
        if v > best:
            best = v

    if best:
        return best

    # Fall back to all dollar amounts in text, return largest
    for m in _AMOUNT_RE.finditer(text):
        v = _scale(float(m.group(1).replace(",", "")), m.group(2) or "")
        if v > best:
            best = v
    return best


def _extract_agent(text: str) -> str:
    """
    Extract the placement agent / underwriter name from filing text.
    Tries two patterns: name-before-role and role-before-name.
    """
    # Name-before-role: "Roth Capital Partners acted as placement agent"
    m = _AGENT_BEFORE_RE.search(text)
    if m:
        name = m.group(1).strip().rstrip(" .,;")
        if len(name) > 3:
            return name

    # Role-before-name: "placement agent for the offering, Roth Capital Partners"
    m = _AGENT_AFTER_RE.search(text)
    if m:
        name = m.group(1).strip().rstrip(" .,;")
        if len(name) > 3:
            return name

    return ""


def _classify_rofr(text: str) -> str:
    """
    Return 'No ROFR', 'ROFR confirmed', or 'ROFR not confirmed'.
    Checks for explicit absence first (most reliable), then presence.
    """
    if _ROFR_ABSENT_RE.search(text):
        return "No ROFR"
    if _ROFR_PRESENT_RE.search(text):
        return "ROFR confirmed"
    return "ROFR not confirmed"


def _classify_tail(text: str) -> str:
    """Return 'No Tail Fee', 'Has Tail Fee', or '' if undetectable."""
    if _NO_TAIL_RE.search(text):
        return "No Tail Fee"
    if _HAS_TAIL_RE.search(text):
        return "Has Tail Fee"
    return ""


def _fmt_amount(amount_usd: float) -> str:
    """Format dollar amount: 25000000 → '$25M', 2700000 → '$2.7M'"""
    if amount_usd >= 1_000_000:
        n = amount_usd / 1_000_000
        s = f"{n:.2f}".rstrip("0").rstrip(".")
        return f"${s}M"
    elif amount_usd > 0:
        n = amount_usd / 1_000
        s = f"{n:.1f}".rstrip("0").rstrip(".")
        return f"${s}K"
    return "?"


def _fmt_raise(raise_dict: dict) -> str:
    """
    Format a raise into a short summary line, e.g.:
      'Mar '25 $25M RDO w/ Roth Capital Partners — No ROFR — No Tail Fee'
      '~$2.7M ATM — Agent unknown — ROFR not confirmed'
    """
    date_str = ""
    if d := raise_dict.get("date"):
        try:
            dt = datetime.fromisoformat(d)
            date_str = dt.strftime("%b '") + dt.strftime("%y")  # "Mar '25"
        except ValueError:
            date_str = d[:7]

    amount = _fmt_amount(raise_dict.get("amount_usd", 0))
    if raise_dict.get("amount_approx"):
        amount = "~" + amount

    rtype  = raise_dict.get("type", "")
    agent  = (raise_dict.get("underwriter") or "").strip().rstrip(".,;") or "Agent unknown"
    rofr   = raise_dict.get("rofr_status", "ROFR not confirmed")
    tail   = raise_dict.get("tail_status", "")

    parts: list[str] = []
    if date_str:
        parts.append(date_str)
    parts.append(f"{amount} {rtype} w/ {agent}")
    parts.append(rofr)
    if tail:
        parts.append(tail)

    return " — ".join(parts)


def _detect_raise_in_text(text: str, filing_date: str) -> Optional[dict]:
    """
    Parse a filing document and return a raise dict if a capital raise is found.
    Returns None if no raise signal detected.
    """
    for pattern, label in _RAISE_TYPE_MAP:
        if pattern.search(text):
            amount    = _parse_amount(text)
            agent     = _extract_agent(text)
            rofr      = _classify_rofr(text)
            tail      = _classify_tail(text)
            raise_dict = {
                "type":        label,
                "amount_usd":  amount,
                "date":        filing_date,
                "underwriter": agent,
                "rofr_status": rofr,
                "tail_status": tail,
                "summary":     "",   # filled below
            }
            raise_dict["summary"] = _fmt_raise(raise_dict)
            return raise_dict
    return None


def _detect_rofr(text: str) -> tuple[bool, str]:
    """Legacy helper — kept for backward compat. Returns (bool, snippet)."""
    m = _ROFR_PRESENT_RE.search(text)
    if not m:
        return False, ""
    return True, _sentences_around(text, m, n=5)


# ── Step 6 — Company context sentence ────────────────────────────────────────

def _build_context(text: str, ticker: str) -> str:
    """
    Generate a one-sentence company summary.
    Priority: Anthropic API → OpenAI API → first 2 sentences of filing excerpt.
    """
    excerpt = re.sub(r"\s+", " ", text[:2000]).strip()
    prompt  = (
        f"In exactly one sentence, describe what {ticker} does and its current "
        f"financial posture based on this SEC filing excerpt:\n\n{excerpt}"
    )

    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    if anthropic_key:
        try:
            import anthropic  # type: ignore
            client = anthropic.Anthropic(api_key=anthropic_key)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=120,
                messages=[{"role": "user", "content": prompt}],
            )
            return msg.content[0].text.strip()
        except Exception:
            pass

    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key:
        try:
            import openai  # type: ignore
            client = openai.OpenAI(api_key=openai_key)
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                max_tokens=120,
                messages=[{"role": "user", "content": prompt}],
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            pass

    # Fallback: first 2 sentences of the excerpt
    sentences = re.split(r"(?<=[.!?])\s+", excerpt)
    return " ".join(sentences[:2])


# ── Public API ────────────────────────────────────────────────────────────────

def get_edgar_intel(ticker: str) -> dict:
    """
    Pull SEC EDGAR intelligence for *ticker*.

    Returns:
    {
      "ticker":           str,
      "filings":          [{"type": str, "date": str, "url": str}],
      "lawyers":          [{"name": str, "firm": str, "linkedin_url": str}],
      "ir_contact":       {"name": str, "title": str, "email": str, "phone": str},
      "recent_raises":    [{"type": str, "amount_usd": float, "date": str, "underwriter": str}],
      "rofr_detected":    bool,
      "rofr_snippet":     str,
      "context_sentence": str,
    }
    """
    result: dict = {
        "ticker":           ticker,
        "filings":          [],
        "lawyers":          [],
        "ir_contact":       {"name": "", "title": "", "email": "", "phone": ""},
        "recent_raises":    [],
        "raise_summary":    "",
        "rofr_detected":    False,
        "rofr_snippet":     "",
        "context_sentence": "",
    }

    # ── 1. Resolve ticker → CIK ───────────────────────────────────────────────
    print(f"    [EDGAR] Resolving CIK for {ticker} …")
    cik = _get_cik(ticker)
    if not cik:
        print(f"    [EDGAR] CIK not found for {ticker} — skipping EDGAR intel")
        return result

    # ── 2. Fetch submissions index ────────────────────────────────────────────
    print(f"    [EDGAR] Fetching submissions index (CIK {cik}) …")
    submissions = _get_submissions(cik)
    if not submissions:
        print(f"    [EDGAR] No submissions data — skipping")
        return result

    one_year_ago = datetime.now() - timedelta(days=365)

    all_filings = _select_filings(
        submissions,
        form_types=["10-Q", "8-K", "S-1", "S-1/A", "S-3", "S-3/A",
                    "424B3", "424B4", "424B5"],
        limit=10,
    )

    result["filings"] = [
        {"type": f["type"], "date": f["date"], "url": f["url"]}
        for f in all_filings
    ]
    print(f"    [EDGAR] Found {len(all_filings)} filings "
          f"({', '.join(f['type'] for f in all_filings[:6])} …)")

    # ── 3. Lawyers from most recent S-1 or S-3 ───────────────────────────────
    for filing in all_filings:
        if filing["type"] not in ("S-1", "S-3"):
            continue
        print(f"    [EDGAR] Extracting lawyers from {filing['type']} ({filing['date']}) …")
        doc_url = _get_exhibit_url(
            filing["cik"], filing["accession"],
            fallback_doc_url=filing["doc_url"],
        )
        text = _fetch_doc_text(doc_url) if doc_url else ""
        if not text:
            continue
        lawyers = _extract_lawyers(text)
        if lawyers:
            print(f"    [EDGAR] Found {len(lawyers)} lawyer(s) — enriching LinkedIn …")
            for lawyer in lawyers:
                lawyer["linkedin_url"] = _linkedin_search(
                    lawyer["name"], lawyer["firm"]
                )
            result["lawyers"] = lawyers
            break   # use most recent S-1/S-3 only

    # ── 4. IR contact from most recent 8-K with a press release ──────────────
    for filing in all_filings:
        if filing["type"] != "8-K":
            continue
        ex_url = _get_exhibit_url(
            filing["cik"], filing["accession"],
            exhibit_type="EX-99.1",
        )
        if not ex_url:
            continue
        text = _fetch_doc_text(ex_url)
        if not text:
            continue
        ir = _extract_ir_contact(text)
        if ir.get("email") or ir.get("phone"):
            print(f"    [EDGAR] IR contact found: {ir.get('name') or '—'} / {ir.get('email') or '—'}")
            result["ir_contact"] = ir
            break

    # ── 5. Recent capital raises + ROFR (last 12 months) ─────────────────────
    recent_filings = [
        f for f in all_filings
        if f["type"] in ("S-1", "S-1/A", "S-3", "S-3/A", "8-K", "424B3", "424B4", "424B5")
        and f.get("date", "") >= one_year_ago.strftime("%Y-%m-%d")
    ]
    print(f"    [EDGAR] Scanning {len(recent_filings)} filings from last 12 months for raises / ROFR …")

    # Track seen (type, ~amount) pairs to avoid double-counting 8-K + 424B for same deal
    seen_raises: set[tuple[str, int]] = set()

    for filing in recent_filings:
        ftype = filing["type"]

        # --- Primary document to check ---
        if ftype == "8-K":
            # Check EX-99.1 (press release) for raise announcement
            doc_url = _get_exhibit_url(
                filing["cik"], filing["accession"], exhibit_type="EX-99.1"
            )
        else:
            doc_url = _get_exhibit_url(
                filing["cik"], filing["accession"],
                fallback_doc_url=filing["doc_url"],
            )

        if not doc_url:
            continue
        text = _fetch_doc_text(doc_url)
        if not text:
            continue

        raise_dict = _detect_raise_in_text(text, filing["date"])

        # For 8-K raises: also scan placement-agent / underwriting exhibits
        # (EX-1.1, EX-10.1, EX-10.2) for ROFR and tail-fee language — these
        # agreements are more explicit than the press release.
        if raise_dict and ftype == "8-K":
            for exhibit in ("EX-1.1", "EX-10.1", "EX-10.2", "EX-10.3"):
                ex_url = _get_exhibit_url(
                    filing["cik"], filing["accession"], exhibit_type=exhibit
                )
                if not ex_url:
                    continue
                ex_text = _fetch_doc_text(ex_url)
                if not ex_text:
                    continue
                # Overwrite ROFR/tail with data from the agreement (more reliable)
                rofr_from_agreement = _classify_rofr(ex_text)
                tail_from_agreement = _classify_tail(ex_text)
                if rofr_from_agreement != "ROFR not confirmed":
                    raise_dict["rofr_status"] = rofr_from_agreement
                if tail_from_agreement:
                    raise_dict["tail_status"] = tail_from_agreement
                # Rebuild summary with updated ROFR/tail
                raise_dict["summary"] = _fmt_raise(raise_dict)
                break   # first matching exhibit is enough

        if raise_dict:
            # Dedup: skip if we already recorded same type + similar amount
            bucket = (raise_dict["type"], round(raise_dict["amount_usd"] / 1_000_000))
            if bucket not in seen_raises:
                seen_raises.add(bucket)
                result["recent_raises"].append(raise_dict)
                print(f"    [EDGAR] ✦ Raise found: {raise_dict['summary']}")

        # Top-level ROFR flag (legacy — keep for backward compat)
        if not result["rofr_detected"]:
            detected, snippet = _detect_rofr(text)
            if detected:
                print(f"    [EDGAR] ⚠  ROFR language in {ftype} ({filing['date']})")
                result["rofr_detected"] = True
                result["rofr_snippet"]  = snippet

    # Build a combined raise_summary string (one line per raise)
    if result["recent_raises"]:
        result["raise_summary"] = "\n".join(
            r["summary"] for r in result["recent_raises"]
        )
    else:
        result["raise_summary"] = "No raises found in last 12 months"

    # ── 6. Context sentence (most recent 10-Q, then 8-K) ─────────────────────
    for filing in all_filings:
        if filing["type"] not in ("10-Q", "8-K"):
            continue
        if filing["type"] == "8-K":
            doc_url = _get_exhibit_url(
                filing["cik"], filing["accession"], exhibit_type="EX-99.1"
            )
        else:
            doc_url = _get_exhibit_url(
                filing["cik"], filing["accession"],
                fallback_doc_url=filing["doc_url"],
            )
        text = _fetch_doc_text(doc_url) if doc_url else ""
        if text:
            print(f"    [EDGAR] Building context sentence from {filing['type']} ({filing['date']}) …")
            result["context_sentence"] = _build_context(text, ticker)
            break

    return result
