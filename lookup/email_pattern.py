"""
email_pattern.py

Infer a company's email format from executives we already have emails for,
then apply that pattern to fill in missing contacts.

Example:
  Barry Sloane  → bsloane@newtekone.com  ─┐
  Peter Downs   → pdowns@newtekone.com   ─┤ pattern: {f}{last}
                                          ─┘
  Frank DeMaria → fdemaria@newtekone.com  ← guessed ✓

Supported patterns (checked in priority order):
  first_initial_last        f + last          →  bsloane
  first_dot_last            first + . + last  →  barry.sloane
  first_last                first + last      →  barrysloane
  first_initial_dot_last    f + . + last      →  b.sloane
  first_underscore_last     first + _ + last  →  barry_sloane
  last_dot_first            last + . + first  →  sloane.barry
  last_first_initial        last + f          →  sloaneב
  last_dot_first_initial    last + . + f      →  sloane.b
  first_only                first             →  barry
  last_only                 last              →  sloane
"""

from __future__ import annotations
import re
from typing import Optional, Callable


# ── Helpers ───────────────────────────────────────────────────────────────────

_PERSONAL_DOMAINS = {
    "gmail", "yahoo", "hotmail", "outlook", "icloud",
    "aol", "protonmail", "me", "mac", "live",
}


def _local(email: str) -> str:
    return email.split("@")[0].lower() if "@" in email else ""


def _domain_of(email: str) -> str:
    return email.split("@")[1].lower() if "@" in email else ""


def _is_personal(email: str) -> bool:
    tld_stripped = _domain_of(email).rsplit(".", 1)[0]   # "gmail.com" → "gmail"
    return tld_stripped in _PERSONAL_DOMAINS


def _alpha(s: str) -> str:
    """Lowercase, letters only — strips hyphens, spaces, accents, etc."""
    return re.sub(r"[^a-z]", "", s.lower())


def extract_domain(website: str) -> str:
    """
    Pull a clean domain from a website URL.
    "https://www.newtekone.com/about" → "newtekone.com"
    """
    if not website:
        return ""
    domain = re.sub(r"^https?://", "", website.strip())
    domain = re.sub(r"^www\.", "", domain)
    domain = domain.split("/")[0].split("?")[0].split("#")[0]
    return domain.lower().strip()


# ── Pattern catalogue ─────────────────────────────────────────────────────────

PatternFn = Callable[[str, str], str]   # (first_alpha, last_alpha) → local_part

PATTERNS: list[tuple[str, PatternFn]] = [
    ("first_initial_last",      lambda f, l: f[0] + l),
    ("first_dot_last",          lambda f, l: f + "." + l),
    ("first_last",              lambda f, l: f + l),
    ("first_initial_dot_last",  lambda f, l: f[0] + "." + l),
    ("first_underscore_last",   lambda f, l: f + "_" + l),
    ("last_dot_first",          lambda f, l: l + "." + f),
    ("last_first_initial",      lambda f, l: l + f[0]),
    ("last_dot_first_initial",  lambda f, l: l + "." + f[0]),
    ("first_only",              lambda f, l: f),
    ("last_only",               lambda f, l: l),
]


def _try_match(fn: PatternFn, first: str, last: str, local: str) -> bool:
    try:
        af, al = _alpha(first), _alpha(last)
        if not af or not al:
            return False
        return fn(af, al) == local
    except (IndexError, Exception):
        return False


# ── Public API ────────────────────────────────────────────────────────────────

class DetectedPattern:
    """Holds a confirmed email pattern for a company domain."""

    def __init__(self, name: str, fn: PatternFn, domain: str, confidence: int):
        self.name       = name
        self.fn         = fn
        self.domain     = domain
        self.confidence = confidence   # number of known emails that confirm it

    def guess(self, first: str, last: str) -> str:
        """Return a guessed email address, or '' on failure."""
        try:
            af, al = _alpha(first), _alpha(last)
            if not af or not al:
                return ""
            local = self.fn(af, al)
            return f"{local}@{self.domain}" if local else ""
        except Exception:
            return ""

    def __repr__(self) -> str:
        return (f"DetectedPattern(name={self.name!r}, domain={self.domain!r}, "
                f"confidence={self.confidence})")


def detect_pattern(
    executives: list[dict],
    website: str = "",
) -> Optional[DetectedPattern]:
    """
    Analyse the emails already found for this company's executives and return
    the most likely email pattern, or None if there isn't enough data.

    Each dict in *executives* must have:
        name        : full name string (e.g. "Mr. Barry Scott Sloane")
        first_name  : first name  (already split)
        last_name   : last name   (already split)
        best_email  : email address (may be empty)

    At least 1 confirmed corporate email is required to infer a pattern.
    """
    # Build the domain we expect emails to come from
    preferred_domain = extract_domain(website)

    # Collect (first, last, local_part, domain) for execs with corporate emails
    samples: list[tuple[str, str, str]] = []   # (first, last, local)
    domain_votes: dict[str, int] = {}

    for exc in executives:
        email = (exc.get("best_email") or "").strip()
        first = (exc.get("first_name") or "").strip()
        last  = (exc.get("last_name")  or "").strip()

        if not email or not first or not last or "@" not in email:
            continue
        if _is_personal(email):
            continue

        dom   = _domain_of(email)
        local = _local(email)
        samples.append((first, last, local, dom))
        domain_votes[dom] = domain_votes.get(dom, 0) + 1

    if not samples:
        return None

    # Pick the target domain: prefer the company website domain if it matches
    # any email domain, otherwise use the most common email domain
    if preferred_domain and preferred_domain in domain_votes:
        target_domain = preferred_domain
    elif preferred_domain:
        # Try to match on the base domain (e.g. "newtekone.com" matches "mail.newtekone.com")
        for dom in domain_votes:
            if dom.endswith(preferred_domain) or preferred_domain.endswith(dom):
                target_domain = dom
                break
        else:
            target_domain = max(domain_votes, key=domain_votes.get)
    else:
        target_domain = max(domain_votes, key=domain_votes.get)

    domain_samples = [(f, l, loc) for f, l, loc, d in samples if d == target_domain]

    if not domain_samples:
        # Fall back to all samples regardless of domain
        domain_samples = [(f, l, loc) for f, l, loc, _ in samples]
        if not domain_samples:
            return None

    # Score each pattern against the confirmed emails
    best: Optional[DetectedPattern] = None
    best_score = 0

    for pname, fn in PATTERNS:
        score = sum(1 for f, l, loc in domain_samples if _try_match(fn, f, l, loc))
        if score > best_score:
            best_score = score
            best = DetectedPattern(pname, fn, target_domain, score)

    # Require at least 1 match — a single confirmed email is enough
    return best if best_score >= 1 else None


def fill_missing_emails(
    executives: list[dict],
    website: str = "",
    verbose: bool = True,
) -> list[dict]:
    """
    For any executive missing a best_email, attempt to guess it using the
    email pattern detected from the other executives at the same company.

    Updates each exec dict in-place (adds/updates best_email and enrich_source).
    Returns the same list for convenience.
    """
    pattern = detect_pattern(executives, website)

    if pattern is None:
        if verbose:
            print("  [pattern] Not enough data to infer email pattern.")
        return executives

    if verbose:
        print(f"  [pattern] Detected: {pattern.name}@{pattern.domain} "
              f"(confidence: {pattern.confidence} email(s))")

    filled = 0
    for exc in executives:
        if exc.get("best_email"):
            continue   # already has an email — skip

        first = exc.get("first_name", "")
        last  = exc.get("last_name",  "")
        guess = pattern.guess(first, last)

        if guess:
            exc["best_email"]    = guess
            exc["work_email"]    = guess
            exc["enrich_source"] = f"pattern_guess ({pattern.name})"
            if verbose:
                print(f"    ✉ {exc.get('name','?')} → {guess}  [pattern guess]")
            filled += 1

    if verbose and filled == 0:
        print("  [pattern] No missing emails to fill.")

    return executives
