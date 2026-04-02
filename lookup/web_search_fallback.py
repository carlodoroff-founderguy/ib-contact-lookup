"""
web_search_fallback.py

When yfinance returns no officers for a public company, search the web for
"<Company> CEO", "<Company> CFO", etc. to find executive names.

Uses DuckDuckGo HTML search (same approach as linkedin_finder.py — no rate
limits, no API key required).
"""

from __future__ import annotations

import re
import time
import random
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

DDG_URL = "https://html.duckduckgo.com/html/"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

SESSION = requests.Session()

# ── Name extraction patterns ──────────────────────────────────────────────────

# ── Title keywords we search for ──────────────────────────────────────────────

_TITLE_KEYWORDS = {
    "CEO": r"CEO|Chief\s+Executive\s+Officer",
    "CFO": r"CFO|Chief\s+Financial\s+Officer",
    "COO": r"COO|Chief\s+Operating\s+Officer",
}

# A capitalised person name: 2-4 words, each starting uppercase or an initial
_NAME_PAT = r'[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-zA-Z\-\']+'


def _build_patterns(role: str) -> list[re.Pattern]:
    """Build extraction patterns for a given role."""
    title_re = _TITLE_KEYWORDS.get(role, re.escape(role))
    return [
        # PATTERN 1:  "Name, CEO" / "Name is the CEO" / "Name as CEO" / "Name — CEO"
        re.compile(
            rf'({_NAME_PAT})\s*(?:,\s*|\s+is\s+(?:the\s+)?|\s+as\s+(?:the\s+)?|\s+(?:—|–|-)\s+|\s*[·•|]\s*)'
            rf'(?:the\s+)?(?:{title_re})',
            re.IGNORECASE,
        ),
        # PATTERN 2:  "CEO: Name" / "CEO is Name" / "CEO — Name"
        re.compile(
            rf'(?:{title_re})\s*(?::\s*|(?:\s+is\s+)|\s+(?:—|–|-)\s+)'
            rf'({_NAME_PAT})',
            re.IGNORECASE,
        ),
        # PATTERN 3:  "CEO of <Company> is Name" / "CEO of <anything> Name"
        re.compile(
            rf'(?:{title_re})\s+(?:of|at|for)\s+.{{1,60}}?(?:\s+is\s+|\s*,\s*|\s+(?:—|–|-)\s+)'
            rf'({_NAME_PAT})',
            re.IGNORECASE,
        ),
        # PATTERN 4:  "Name serves as CEO" / "Name appointed CEO"
        re.compile(
            rf'({_NAME_PAT})\s+(?:serves?\s+as|appointed|named|became|elected)\s+(?:the\s+)?(?:{title_re})',
            re.IGNORECASE,
        ),
    ]

# Words that are NOT person names — reject matches containing these
_REJECT_WORDS = {
    "company", "inc", "corp", "corporation", "group", "stock", "share",
    "price", "market", "cap", "revenue", "profit", "annual", "quarter",
    "report", "news", "press", "release", "board", "director", "fund",
    "about", "contact", "email", "phone", "address", "site", "page",
    "wiki", "wikipedia", "bloomberg", "reuters", "yahoo", "finance",
    "linkedin", "glassdoor", "indeed", "twitter", "sec", "filing",
    "investor", "relations", "earnings", "dividend", "executive",
    "compensation", "salary", "net", "worth", "biography", "bio",
    "age", "education", "career",
}


def _is_plausible_name(name: str) -> bool:
    """Return True if `name` looks like an actual person name."""
    parts = name.strip().split()
    if len(parts) < 2 or len(parts) > 4:
        return False
    # All parts should start with uppercase
    for p in parts:
        if not p[0].isupper():
            return False
    # No reject words
    words_lower = {w.lower().rstrip(".,") for w in parts}
    if words_lower & _REJECT_WORDS:
        return False
    # Name parts shouldn't be too long (probably scraped garbage)
    if any(len(p) > 20 for p in parts):
        return False
    return True


def _extract_name_from_results(html_text: str, role: str) -> Optional[str]:
    """
    Parse DDG search results HTML and extract the best person name
    for the given role (CEO/CFO/etc.).
    """
    soup = BeautifulSoup(html_text, "html.parser")

    # Get text from search result snippets
    snippets: list[str] = []
    for el in soup.select(".result__snippet"):
        snippets.append(el.get_text(separator=" ", strip=True))
    # Also check result titles
    for el in soup.select(".result__title"):
        snippets.append(el.get_text(separator=" ", strip=True))

    # If no DDG-specific elements, fall back to full text
    if not snippets:
        snippets = [soup.get_text(separator=" ", strip=True)]

    full_text = " ".join(snippets)

    # Try each extraction pattern (built dynamically for the role)
    candidates: list[str] = []
    patterns = _build_patterns(role)

    for pat in patterns:
        for m in pat.finditer(full_text):
            name = m.group(1).strip()
            if _is_plausible_name(name):
                candidates.append(name)

    # De-duplicate preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for c in candidates:
        key = c.lower()
        if key not in seen:
            seen.add(key)
            unique.append(c)

    return unique[0] if unique else None


# ── Public API ────────────────────────────────────────────────────────────────

def search_executive(
    company: str,
    role: str = "CEO",
    delay: float = 1.2,
) -> Optional[str]:
    """
    Search the web for "{company} {role}" and return the person's name,
    or None if not found.

    Args:
        company:  Company display name (e.g. "The Coca-Cola Company")
        role:     Executive role to search for — "CEO", "CFO", etc.
        delay:    Seconds to sleep before the request (rate-limit courtesy)

    Returns:
        Person name string (e.g. "James Quincey") or None
    """
    if not company:
        return None

    time.sleep(delay)

    # Build search query
    query = f"{company} {role} name"

    headers = {"User-Agent": random.choice(USER_AGENTS)}
    data = {"q": query}

    try:
        r = SESSION.post(DDG_URL, data=data, headers=headers, timeout=12)
        if r.status_code != 200:
            return None

        name = _extract_name_from_results(r.text, role)
        return name

    except Exception:
        return None


def search_executives(
    company: str,
    roles: list[str] | None = None,
    delay: float = 1.2,
) -> list[dict]:
    """
    Search for multiple executive roles at once.

    Args:
        company:  Company display name
        roles:    List of roles to search (default: ["CEO", "CFO"])
        delay:    Seconds between requests

    Returns:
        List of dicts with keys: name, title
        e.g. [{"name": "James Quincey", "title": "CEO"}, ...]
    """
    if roles is None:
        roles = ["CEO", "CFO"]

    results: list[dict] = []
    seen_names: set[str] = set()

    for role in roles:
        name = search_executive(company, role, delay=delay)
        if name and name.lower() not in seen_names:
            seen_names.add(name.lower())
            results.append({"name": name, "title": role})

    return results
