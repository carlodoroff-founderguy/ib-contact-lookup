"""
bouncer_verifier.py

Email deliverability verification via Bouncer API v1.1.
https://docs.usebouncer.com/

Single-email endpoint:
  GET https://api.usebouncer.com/v1.1/email/verify?email=<email>
  Header: x-api-key: <key>

Response status values:
  deliverable   → email is valid and accepting mail
  undeliverable → email does not exist / hard bounce
  risky         → may bounce; catch-all or role address
  unknown       → server didn't respond, couldn't determine

Usage:
  from lookup.bouncer_verifier import verify_email, apply_flag

  result = verify_email("john@company.com")
  flagged = apply_flag("john@company.com", result)
"""
from __future__ import annotations

import os
import re
import time

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

# ── Config ────────────────────────────────────────────────────────────────────

_BOUNCER_URL = "https://api.usebouncer.com/v1.1/email/verify"
_BOUNCER_KEY = os.getenv("BOUNCER_API_KEY", "ZNlbiWNS5jtVBXdcFZ2W8PKKYC27fxMxacSMiBK2")
_TIMEOUT     = 8   # seconds per request

# Strings that mean "no real email to verify"
_SKIP_VALUES = {
    "", "not found", "not on linkedin", "not on sql", "n/a", "—",
    "api error", "error", "no data", "not found on linkedin",
}

# ── Verification flags ─────────────────────────────────────────────────────────

_STATUS_FLAGS = {
    "deliverable":   "",                   # clean — no flag appended
    "undeliverable": " ⚠ INVALID",
    "risky":         " ⚠ RISKY",
    "unknown":       " ⚠ UNVERIFIED",
}

# Reason codes that are worth surfacing as a secondary note
_REASON_NOTES: dict[str, str] = {
    "no_mx_record":      " (no MX)",
    "disposable_email":  " (disposable)",
    "role_account":      " (role acct)",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _extract_email(value: str) -> str:
    """
    Pull the bare email address out of a formatted cell value.

    e.g. "john@co.com (no work provided)" → "john@co.com"
         "john@co.com ⚠ RISKY"            → "john@co.com"
    """
    if not value:
        return ""
    # Take the first token that looks like an email
    m = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", value)
    return m.group(0) if m else ""


def _should_skip(value: str) -> bool:
    """Return True if this cell value doesn't contain a real email to verify."""
    if not value:
        return True
    vl = value.strip().lower()
    if vl in _SKIP_VALUES:
        return True
    # Already has a Bouncer flag → don't re-verify
    if "⚠" in value:
        return True
    return not _extract_email(value)


# ── Core API call ──────────────────────────────────────────────────────────────

def verify_email(email_value: str) -> dict:
    """
    Verify a single email address with Bouncer.

    Parameters
    ----------
    email_value : str
        The raw cell value (may include annotations like "(no work provided)").

    Returns
    -------
    dict with keys:
      status  : "deliverable" | "undeliverable" | "risky" | "unknown" | "skipped"
      reason  : Bouncer reason code or local skip reason
      score   : int 0-100 (if provided by Bouncer, else None)
      raw     : full Bouncer response dict (or {})
    """
    if _should_skip(email_value):
        return {"status": "skipped", "reason": "no_email", "score": None, "raw": {}}

    bare = _extract_email(email_value)
    if not bare:
        return {"status": "skipped", "reason": "parse_failed", "score": None, "raw": {}}

    if not _HAS_REQUESTS:
        return {"status": "unknown", "reason": "requests_not_installed", "score": None, "raw": {}}

    if not _BOUNCER_KEY:
        return {"status": "unknown", "reason": "no_api_key", "score": None, "raw": {}}

    try:
        resp = _requests.get(
            _BOUNCER_URL,
            params  = {"email": bare},
            headers = {"x-api-key": _BOUNCER_KEY},
            timeout = _TIMEOUT,
        )
        if resp.status_code == 402:
            return {"status": "unknown", "reason": "bouncer_no_credits", "score": None, "raw": {}}
        if resp.status_code == 429:
            time.sleep(2)
            return {"status": "unknown", "reason": "bouncer_rate_limited", "score": None, "raw": {}}
        if resp.status_code != 200:
            return {"status": "unknown", "reason": f"http_{resp.status_code}", "score": None, "raw": {}}

        data   = resp.json()
        status = data.get("status", "unknown").lower()
        reason = data.get("reason", "")
        score  = data.get("score")
        return {"status": status, "reason": reason, "score": score, "raw": data}

    except Exception as exc:
        return {"status": "unknown", "reason": str(exc)[:80], "score": None, "raw": {}}


# ── Flag application ───────────────────────────────────────────────────────────

def apply_flag(email_value: str, result: dict) -> str:
    """
    Process an email based on its Bouncer verification status.

    Rules:
      deliverable   → no change (keep as-is)
      skipped       → no change
      risky         → keep, append " ⚠ RISKY"
      undeliverable → REMOVE (return empty string)
      unknown       → REMOVE (return empty string)

    Undeliverable and unknown emails are hidden entirely because they
    are either invalid or cannot be confirmed.
    """
    status = result.get("status", "unknown")
    if status in ("deliverable", "skipped"):
        return email_value

    # Remove undeliverable and unknown emails entirely
    if status in ("undeliverable", "unknown"):
        return ""

    # Risky emails: keep but flag
    flag        = _STATUS_FLAGS.get(status, f" ⚠ {status.upper()}")
    reason_note = _REASON_NOTES.get(result.get("reason", ""), "")

    return f"{email_value}{flag}{reason_note}"


# ── Batch verification ────────────────────────────────────────────────────────

def verify_row_emails(
    row: dict,
    email_cols: tuple[str, ...] = ("CEO EMAIL", "CFO EMAIL", "IR Email"),
    delay: float = 0.3,
    log_fn=None,
) -> dict:
    """
    Verify all email columns in a single row dict.
    Modifies the dict in-place AND returns it.

    Parameters
    ----------
    row       : the result row dict
    email_cols: which columns to check
    delay     : seconds to wait between Bouncer calls (rate-limiting)
    log_fn    : optional callable(msg, level) for logging
    """
    def _log(msg, level="dim"):
        if log_fn:
            log_fn(msg, level)

    for col in email_cols:
        val = row.get(col, "")
        if _should_skip(str(val)):
            continue

        result = verify_email(str(val))
        flagged = apply_flag(str(val), result)
        row[col] = flagged

        status = result.get("status", "unknown")
        if status == "deliverable":
            _log(f"   ✓ Bouncer: {col} → deliverable", "ok")
        elif status == "skipped":
            pass
        else:
            _log(f"   ⚠ Bouncer: {col} → {status} ({result.get('reason','')})", "warn")

        if delay > 0:
            time.sleep(delay)

    return row
