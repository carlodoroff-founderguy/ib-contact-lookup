"""
spac_contact_lookup.py

Dedicated SPAC contact enrichment chain.
Takes executives with known names (from spac_roster.json) and finds
their emails and phone numbers via SalesQL + LinkedIn.

This is separate from the normal ticker lookup flow — names are already
provided, so skip yfinance and domain resolution entirely.

Enrichment chain per executive:
  STEP 1 — Name clean (strip credentials)
  STEP 2 — SalesQL name + company search (no domain)
  STEP 3 — LinkedIn URL search → SalesQL enrich by URL
  STEP 4 — Broader LinkedIn search (relaxed query)
  STEP 5 — Email inference from other exec at same SPAC
  STEP 6 — Store what we have

Performance features:
  - Per-run person cache (same exec across multiple SPACs → one API call)
  - Smart delays: 0.8s SalesQL, 1.2s DuckDuckGo (via linkedin_finder)
  - Common Asian surname detection → skip LinkedIn (too many false positives)
  - Thread-safe roster save for parallel enrichment
"""

from __future__ import annotations

import re
import time
import json
import threading
from pathlib import Path
from typing import Optional, Callable

from lookup.salesql_enricher import (
    search_by_name_and_company,
    enrich_by_url,
    _empty as salesql_empty,
)
from lookup.linkedin_finder import find_linkedin_url
from lookup.email_pattern import detect_pattern


# ── Config ────────────────────────────────────────────────────────────────────

ROSTER_PATH = Path(__file__).resolve().parent.parent / "spac_data" / "spac_roster.json"

SALESQL_DELAY = 0.8    # seconds between SalesQL calls (down from 1.5)
DDG_DELAY     = 1.2    # seconds before DuckDuckGo/LinkedIn calls
DEFAULT_DELAY = 0.8    # legacy compat — used when caller passes delay=

_PERSONAL_DOMAINS = {
    "gmail", "yahoo", "hotmail", "outlook", "icloud",
    "aol", "protonmail", "me", "mac", "live", "msn",
}

# ── FIX 3: Common Asian surnames — LinkedIn returns too many false positives ──

COMMON_AMBIGUOUS_SURNAMES = {
    "chen", "wang", "li", "zhang", "lee", "kim", "park", "liu", "wu",
    "ng", "tan", "lim", "chan", "lin", "yang", "huang", "zhao", "sun",
    "zhou", "xu", "zhu", "ma", "hu", "guo", "he", "lu", "luo", "song",
    "zheng", "deng", "cao", "wei", "xie", "han", "tang", "feng", "yu",
    "dong", "xiao", "cheng", "pan", "yuan", "su", "ye", "jiang",
    "du", "ren", "peng", "liang", "shi", "fu", "fang",
}


def _should_try_linkedin(cleaned_name: str) -> bool:
    """
    Return False if LinkedIn search would be too ambiguous for this name.
    Two-word names with very common East Asian surnames produce hundreds
    of false positives on LinkedIn.
    """
    parts = cleaned_name.strip().split()
    if len(parts) != 2:
        return True  # 3+ word names are disambiguated enough
    _, last = parts[0], parts[-1]
    if last.lower() in COMMON_AMBIGUOUS_SURNAMES:
        return False  # e.g. "Cong Wang", "Jing Lu" — too many matches
    return True


# ── FIX 4: Person cache ──────────────────────────────────────────────────────

_person_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()


def _cache_key(name: str) -> str:
    """Normalise name for cache lookup."""
    return " ".join(name.lower().split())


def reset_cache():
    """Clear the person cache (call at start of a new enrichment run)."""
    global _person_cache
    with _cache_lock:
        _person_cache = {}


# ── Thread-safe roster save ──────────────────────────────────────────────────

_save_lock = threading.Lock()


# ── STEP 1: Name cleaning ────────────────────────────────────────────────────

_PREFIX_RE = re.compile(
    r'^\s*(Mr\.?|Ms\.?|Mrs\.?|Miss\.?|Dr\.?|Prof\.?|Sir)\s+',
    re.IGNORECASE,
)
_SUFFIX_TOKENS = {
    s.replace(".", "").replace(",", "").lower() for s in {
        "jr", "jr.", "sr", "sr.", "ii", "iii", "iv",
        "phd", "ph.d", "ph.d.", "md", "m.d", "m.d.",
        "cfa", "c.f.a", "c.f.a.", "cpa", "c.p.a", "c.p.a.",
        "mba", "m.b.a", "m.b.a.", "esq", "esq.",
        "jd", "j.d", "j.d.", "llm", "ll.m", "ll.m.",
        "emba",
    }
}


def clean_name(raw_name: str) -> str:
    """
    Strip credentials from an executive name for SalesQL/LinkedIn search.

    Examples:
      "Gregory R. Monahan, MBA"                   → "Gregory R. Monahan"
      "Adam H. Jaffe, CPA"                        → "Adam H. Jaffe"
      "Jay Taragin, MBA, CPA"                     → "Jay Taragin"
      "Antônio Carlos Augusto R. Bonchristiano"   → "Antônio Carlos Augusto R. Bonchristiano"
      "James Joseph McEntee, III"                  → "James Joseph McEntee"
      "Louis Charles Gerken, III, MBA"             → "Louis Charles Gerken"
      "Justin di Rezze, MD"                        → "Justin di Rezze"

    Keep middle initials — they help disambiguate common names.
    """
    if not raw_name or not raw_name.strip():
        return ""

    # Strip honorific prefix
    clean = _PREFIX_RE.sub("", raw_name).strip()

    # Split on first comma that introduces credentials
    if "," in clean:
        before = clean.split(",")[0].strip()
        after_parts = clean.split(",")[1:]
        after_text = ",".join(after_parts).strip()
        if len(before.split()) >= 2:
            after_tokens = re.split(r'[\s,]+', after_text)
            all_creds = all(
                t.rstrip(".,").lower().replace(".", "").replace(",", "") in _SUFFIX_TOKENS
                for t in after_tokens if t.strip()
            )
            if all_creds and after_tokens:
                clean = before

    # Strip trailing credential tokens word-by-word
    parts = clean.split()
    while parts:
        tok = parts[-1].rstrip(".,").lower().replace(".", "").replace(",", "")
        if tok in _SUFFIX_TOKENS:
            parts.pop()
        else:
            break

    return " ".join(parts).strip()


def split_name(full_name: str) -> tuple[str, str]:
    """Return (first, last) from a cleaned name."""
    parts = full_name.strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


# ── STEP 2: SalesQL name + company search ─────────────────────────────────────

def _step2_salesql_name(
    cleaned_name: str, company: str, role: str,
    log_fn: Callable,
) -> dict:
    """SalesQL search by name + company name (no domain)."""
    log_fn(f"  STEP 2: SalesQL name+company → {cleaned_name}", "dim")
    time.sleep(SALESQL_DELAY)
    result = search_by_name_and_company(cleaned_name, company, title=role)
    if result.get("best_email") or result.get("phone"):
        log_fn(f"  ✓ Found via SalesQL name search: {result.get('best_email','—')} {result.get('phone','—')}", "ok")
        return result
    log_fn(f"  · SalesQL name search: no result", "dim")
    return {}


# ── STEPS 3 & 4: LinkedIn → SalesQL ──────────────────────────────────────────

def _step3_linkedin_targeted(
    cleaned_name: str, company: str, role: str,
    log_fn: Callable,
) -> dict:
    """
    STEP 3: Search LinkedIn for "{cleaned_name}" "{company}" site:linkedin.com/in
    Then enrich the found URL via SalesQL.
    """
    log_fn(f"  STEP 3: LinkedIn targeted → {cleaned_name} @ {company}", "dim")
    # DDG delay is handled inside find_linkedin_url (sleep_range param)
    li_url = find_linkedin_url(cleaned_name, company, role, sleep_range=(DDG_DELAY, DDG_DELAY + 0.3))
    if li_url:
        log_fn(f"  · LinkedIn found: {li_url} — enriching via SalesQL", "dim")
        time.sleep(SALESQL_DELAY)
        result = enrich_by_url(li_url)
        if result.get("best_email") or result.get("phone"):
            result["linkedin_url"] = li_url
            log_fn(f"  ✓ Found via LinkedIn+SalesQL: {result.get('best_email','—')} {result.get('phone','—')}", "ok")
            return result
        log_fn(f"  · LinkedIn URL found but SalesQL returned nothing", "dim")
    else:
        log_fn(f"  · LinkedIn: no profile found", "dim")
    return {}


def _step4_linkedin_broad(
    cleaned_name: str, role: str,
    log_fn: Callable,
) -> dict:
    """
    STEP 4: Broader LinkedIn search — "{first} {last}" SPAC {role}
    """
    first, last = split_name(cleaned_name)
    if not first or not last:
        return {}

    broad_name = f"{first} {last}"
    broad_company = f"SPAC {role}"
    log_fn(f"  STEP 4: LinkedIn broad → {broad_name} (SPAC {role})", "dim")
    li_url = find_linkedin_url(broad_name, broad_company, role, sleep_range=(DDG_DELAY, DDG_DELAY + 0.3))
    if li_url:
        log_fn(f"  · LinkedIn broad found: {li_url} — enriching via SalesQL", "dim")
        time.sleep(SALESQL_DELAY)
        result = enrich_by_url(li_url)
        if result.get("best_email") or result.get("phone"):
            result["linkedin_url"] = li_url
            log_fn(f"  ✓ Found via broad LinkedIn: {result.get('best_email','—')} {result.get('phone','—')}", "ok")
            return result
    log_fn(f"  · LinkedIn broad: no result", "dim")
    return {}


# ── STEP 5: Email inference ───────────────────────────────────────────────────

def _is_personal_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    domain = email.split("@")[1].lower().rsplit(".", 1)[0]
    return domain in _PERSONAL_DOMAINS


def _step5_infer_email(
    cleaned_name: str,
    other_exec_email: str,
    log_fn: Callable,
) -> str:
    """
    STEP 5: If the other executive at this SPAC has a confirmed email,
    try to infer this executive's email using the same pattern.
    """
    if not other_exec_email or _is_personal_email(other_exec_email):
        return ""

    first, last = split_name(cleaned_name)
    if not first or not last:
        return ""

    domain = other_exec_email.split("@")[1] if "@" in other_exec_email else ""
    if not domain:
        return ""

    # Build a synthetic exec list for pattern detection
    local_part = other_exec_email.split("@")[0].lower()

    execs = [
        {
            "name": "other_exec",
            "first_name": "x",
            "last_name": "x",
            "best_email": other_exec_email,
        },
    ]

    # Try to detect pattern using the known email
    pattern = detect_pattern(execs, website=f"https://{domain}")
    if pattern:
        guess = pattern.guess(first, last)
        if guess:
            log_fn(f"  STEP 5: Inferred email → {guess} (pattern: {pattern.name})", "info")
            return guess

    # Fallback: manual pattern matching on the known email
    alpha_first = re.sub(r'[^a-z]', '', first.lower())
    alpha_last = re.sub(r'[^a-z]', '', last.lower())

    if not alpha_first or not alpha_last:
        return ""

    other_local = other_exec_email.split("@")[0].lower()
    if "." in other_local:
        parts = other_local.split(".")
        if len(parts[0]) == 1:
            guess = f"{alpha_first[0]}.{alpha_last}@{domain}"
        else:
            guess = f"{alpha_first}.{alpha_last}@{domain}"
        log_fn(f"  STEP 5: Inferred email → {guess} (manual pattern match)", "info")
        return guess
    elif len(other_local) > 2:
        if len(other_local) <= len(alpha_last) + 1:
            guess = f"{alpha_first[0]}{alpha_last}@{domain}"
        else:
            guess = f"{alpha_first}{alpha_last}@{domain}"
        log_fn(f"  STEP 5: Inferred email → {guess} (manual pattern match)", "info")
        return guess

    return ""


# ── Main enrichment function ──────────────────────────────────────────────────

def enrich_spac_executive(
    raw_name: str,
    role: str,
    company: str,
    existing_email: str = "",
    existing_phone: str = "",
    other_exec_email: str = "",
    delay: float = DEFAULT_DELAY,
    skip_linkedin: bool = False,
    log_fn: Optional[Callable] = None,
) -> dict:
    """
    Run the full SPAC enrichment chain for a single executive.

    Returns dict with keys:
      email, phone, phone_type, linkedin_url, source, notes

    Never overwrites existing data — if email already exists, returns it as-is.
    Uses per-run cache so the same person is only looked up once across SPACs.
    """
    _log = log_fn or (lambda msg, lvl="dim": print(f"    {msg}"))

    result = {
        "email": existing_email,
        "phone": existing_phone,
        "phone_type": "",
        "linkedin_url": "",
        "source": "Known" if existing_email else "",
        "notes": "",
    }

    # If email already exists, skip enrichment
    if existing_email:
        _log(f"  ✓ {role}: {raw_name} → {existing_email} (already known)", "ok")
        return result

    # If no name, nothing to search
    if not raw_name or not raw_name.strip():
        result["source"] = "No name"
        result["notes"] = "No executive name provided"
        _log(f"  · {role}: no name provided — skipping", "dim")
        return result

    # STEP 1: Clean the name
    cleaned = clean_name(raw_name)
    _log(f"  STEP 1: Clean name: {raw_name!r} → {cleaned!r}", "dim")

    # ── FIX 4: Check person cache ────────────────────────────────────────────
    ckey = _cache_key(cleaned)
    with _cache_lock:
        cached = _person_cache.get(ckey)
    if cached:
        _log(f"  [cache hit] {cleaned} → {cached.get('email','—')}", "info")
        return {
            "email": cached.get("email", ""),
            "phone": cached.get("phone", ""),
            "phone_type": cached.get("phone_type", ""),
            "linkedin_url": cached.get("linkedin_url", ""),
            "source": cached.get("source", "Cached"),
            "notes": f"Cached from earlier lookup",
        }

    # STEP 2: SalesQL name + company search
    enrichment = _step2_salesql_name(cleaned, company, role, _log)
    if enrichment:
        result["email"] = enrichment.get("best_email", "")
        result["phone"] = enrichment.get("phone", "")
        result["phone_type"] = enrichment.get("phone_type", "")
        result["linkedin_url"] = enrichment.get("linkedin_url", "")
        result["source"] = "SalesQL-name"
        # Cache it
        with _cache_lock:
            _person_cache[ckey] = dict(result)
        return result

    # ── FIX 3: Check if LinkedIn would be useful for this name ───────────────
    if not skip_linkedin:
        if not _should_try_linkedin(cleaned):
            _log(f"  · Skipping LinkedIn — common surname, too many false positives", "dim")
            skip_linkedin = True  # override for this exec only

    # STEP 3: LinkedIn targeted → SalesQL
    if not skip_linkedin:
        enrichment = _step3_linkedin_targeted(cleaned, company, role, _log)
        if enrichment:
            result["email"] = enrichment.get("best_email", "")
            result["phone"] = enrichment.get("phone", "")
            result["phone_type"] = enrichment.get("phone_type", "")
            result["linkedin_url"] = enrichment.get("linkedin_url", "")
            result["source"] = "SalesQL-linkedin"
            with _cache_lock:
                _person_cache[ckey] = dict(result)
            return result

        # STEP 4: Broader LinkedIn search
        enrichment = _step4_linkedin_broad(cleaned, role, _log)
        if enrichment:
            result["email"] = enrichment.get("best_email", "")
            result["phone"] = enrichment.get("phone", "")
            result["phone_type"] = enrichment.get("phone_type", "")
            result["linkedin_url"] = enrichment.get("linkedin_url", "")
            result["source"] = "SalesQL-linkedin"
            with _cache_lock:
                _person_cache[ckey] = dict(result)
            return result

    # STEP 5: Email inference from other exec
    if other_exec_email:
        inferred = _step5_infer_email(cleaned, other_exec_email, _log)
        if inferred:
            result["email"] = inferred
            result["source"] = "Inferred"
            result["notes"] = "Inferred from other exec email pattern"
            # Don't cache inferred — it's SPAC-specific (domain varies)
            return result

    # STEP 6: Store what we have (nothing found)
    result["source"] = "Not found"
    result["notes"] = "All enrichment steps failed"
    _log(f"  ⚠ {role}: {raw_name} → no contact found (all steps exhausted)", "warn")
    # Cache the miss too so we don't retry the same person
    with _cache_lock:
        _person_cache[ckey] = dict(result)
    return result


def enrich_spac_ticker(
    ticker: str,
    spac_data: dict,
    delay: float = DEFAULT_DELAY,
    skip_linkedin: bool = False,
    log_fn: Optional[Callable] = None,
) -> dict:
    """
    Enrich both CEO and CFO for a single SPAC ticker.

    spac_data: dict with keys company, ceo_name, ceo_email, cfo_name, cfo_email, etc.

    Returns the updated spac_data dict with any newly found emails/phones.
    Never overwrites existing data.
    """
    _log = log_fn or (lambda msg, lvl="dim": print(f"    {msg}"))

    company = spac_data.get("company", "")
    _log(f"[{ticker}] {company}", "info")

    # Enrich CEO
    ceo_result = enrich_spac_executive(
        raw_name=spac_data.get("ceo_name", ""),
        role="CEO",
        company=company,
        existing_email=spac_data.get("ceo_email", ""),
        existing_phone=spac_data.get("ceo_phone", ""),
        other_exec_email=spac_data.get("cfo_email", ""),
        delay=delay,
        skip_linkedin=skip_linkedin,
        log_fn=_log,
    )

    # Enrich CFO (pass CEO email for inference)
    ceo_email_for_inference = ceo_result.get("email", "") or spac_data.get("ceo_email", "")
    cfo_result = enrich_spac_executive(
        raw_name=spac_data.get("cfo_name", ""),
        role="CFO",
        company=company,
        existing_email=spac_data.get("cfo_email", ""),
        existing_phone=spac_data.get("cfo_phone", ""),
        other_exec_email=ceo_email_for_inference,
        delay=delay,
        skip_linkedin=skip_linkedin,
        log_fn=_log,
    )

    # Now try inference again for CEO if CFO was found and CEO wasn't
    if not ceo_result.get("email") and cfo_result.get("email") and cfo_result["source"] != "Inferred":
        inferred = _step5_infer_email(
            clean_name(spac_data.get("ceo_name", "")),
            cfo_result["email"],
            _log,
        )
        if inferred:
            ceo_result["email"] = inferred
            ceo_result["source"] = "Inferred"
            ceo_result["notes"] = "Inferred from CFO email pattern"

    # Update spac_data — never overwrite existing data
    if ceo_result.get("email") and not spac_data.get("ceo_email"):
        spac_data["ceo_email"] = ceo_result["email"]
    if ceo_result.get("phone") and not spac_data.get("ceo_phone"):
        spac_data["ceo_phone"] = ceo_result["phone"]
    if cfo_result.get("email") and not spac_data.get("cfo_email"):
        spac_data["cfo_email"] = cfo_result["email"]
    if cfo_result.get("phone") and not spac_data.get("cfo_phone"):
        spac_data["cfo_phone"] = cfo_result["phone"]

    # Determine source
    sources = set()
    if ceo_result.get("source") and ceo_result["source"] != "No name":
        sources.add(ceo_result["source"])
    if cfo_result.get("source") and cfo_result["source"] != "No name":
        sources.add(cfo_result["source"])
    spac_data["source"] = " / ".join(sorted(sources)) if sources else spac_data.get("source", "")

    # Notes
    notes = []
    if ceo_result.get("notes"):
        notes.append(f"CEO: {ceo_result['notes']}")
    if cfo_result.get("notes"):
        notes.append(f"CFO: {cfo_result['notes']}")
    if notes:
        spac_data["notes"] = "; ".join(notes)

    return spac_data


# ── Roster I/O ────────────────────────────────────────────────────────────────

def load_roster(path: Optional[Path] = None) -> dict:
    """Load the SPAC roster JSON. Returns {ticker: {data...}, ...}."""
    p = path or ROSTER_PATH
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def save_roster(roster: dict, path: Optional[Path] = None) -> None:
    """Save the SPAC roster JSON (pretty-printed). Thread-safe."""
    p = path or ROSTER_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    with _save_lock:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(roster, f, indent=2, ensure_ascii=False)


def needs_enrichment(spac_data: dict) -> bool:
    """Return True if this SPAC has executives missing email."""
    ceo_name = spac_data.get("ceo_name", "").strip()
    cfo_name = spac_data.get("cfo_name", "").strip()
    ceo_email = spac_data.get("ceo_email", "").strip()
    cfo_email = spac_data.get("cfo_email", "").strip()

    if ceo_name and not ceo_email:
        return True
    if cfo_name and not cfo_email:
        return True
    return False


def get_urgency(spac_data: dict) -> str:
    """Return urgency level based on days_remaining."""
    days = spac_data.get("days_remaining")
    if days is None:
        return "Unknown"
    if days < 30:
        return "URGENT"
    if days <= 90:
        return "Near-term"
    if days <= 180:
        return "Upcoming"
    return "Standard"
