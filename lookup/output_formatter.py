"""
output_formatter.py
Render results as a Rich terminal table, JSON, or CSV.
"""

from __future__ import annotations
import json
import csv
import io
from typing import Any

try:
    from rich.console import Console
    from rich.table import Table
    from rich import box
    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False


def _dash(val: Any) -> str:
    """Return value or '—' if empty."""
    s = str(val).strip() if val is not None else ""
    return s if s else "—"


def print_rich_table(result: dict) -> None:
    """
    Print a formatted Rich table for a single ticker result.
    Falls back to plain text if Rich is not installed.
    """
    ticker  = result.get("ticker", "")
    company = result.get("company", "Unknown")
    execs   = result.get("executives", [])

    if not _HAS_RICH:
        # Plain-text fallback
        print(f"\n{'='*65}")
        print(f"  EXECUTIVE CONTACT LOOKUP — {ticker}  ({company})")
        print(f"{'='*65}")
        print(f"{'Role':<12} {'Name':<22} {'Email':<28} {'Phone':<16}")
        print(f"{'-'*12} {'-'*22} {'-'*28} {'-'*16}")
        for e in execs:
            role  = _dash(e.get("role"))
            name  = _dash(e.get("name"))
            email = _dash(e.get("best_email"))
            phone = _dash(e.get("phone"))
            print(f"{role:<12} {name:<22} {email:<28} {phone:<16}")
        print()
        return

    console = Console()
    table = Table(
        title=f"EXECUTIVE CONTACT LOOKUP — [bold cyan]{ticker}[/] ({company})",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold magenta",
        min_width=72,
    )
    table.add_column("Role",       style="cyan",  no_wrap=True, min_width=10)
    table.add_column("Name",       style="white", no_wrap=True, min_width=22)
    table.add_column("Email",      style="green", no_wrap=True, min_width=28)
    table.add_column("Phone",      style="yellow",no_wrap=True, min_width=14)
    table.add_column("Source",     style="dim",   no_wrap=True, min_width=8)

    for e in execs:
        table.add_row(
            _dash(e.get("role")),
            _dash(e.get("name")),
            _dash(e.get("best_email")),
            _dash(e.get("phone")),
            _dash(e.get("enrich_source")),
        )

    console.print(table)


def to_json(result: dict, indent: int = 2) -> str:
    """Serialize result dict to JSON string."""
    return json.dumps(result, indent=indent, ensure_ascii=False)


def to_csv_rows(result: dict) -> list[dict]:
    """
    Flatten a ticker result into one CSV row per executive.
    EDGAR intel fields are appended to every row (same values per ticker).
    """
    import json as _json

    rows    = []
    ticker  = result.get("ticker", "")
    company = result.get("company", "")
    website = result.get("website", "")
    industry= result.get("industry", "")
    city    = result.get("city", "")
    state   = result.get("state", "")
    country = result.get("country", "")
    emp_cnt = result.get("employee_count", "")

    # ── EDGAR intel fields (flattened to scalar / short JSON strings) ──────────
    intel         = result.get("edgar_intel") or {}
    ir            = intel.get("ir_contact") or {}
    raises        = intel.get("recent_raises") or []
    top_raise     = raises[0] if raises else {}
    lawyers       = intel.get("lawyers") or []

    edgar_context     = intel.get("context_sentence", "")
    edgar_ir_name     = ir.get("name", "")
    edgar_ir_email    = ir.get("email", "")
    edgar_ir_phone    = ir.get("phone", "")
    edgar_rofr        = "Yes" if intel.get("rofr_detected") else ("No" if intel else "")
    edgar_rofr_snippet= intel.get("rofr_snippet", "")
    edgar_raise_type  = top_raise.get("type", "")
    edgar_raise_amt   = str(top_raise.get("amount_usd", "")) if top_raise else ""
    edgar_raise_date  = top_raise.get("date", "")
    edgar_raise_uw    = top_raise.get("underwriter", "")
    edgar_lawyers     = (
        _json.dumps([{"name": l["name"], "firm": l["firm"]} for l in lawyers],
                    ensure_ascii=False)
        if lawyers else ""
    )

    for e in result.get("executives", []):
        rows.append({
            "ticker":              ticker,
            "company":             company,
            "website":             website,
            "industry":            industry,
            "city":                city,
            "state":               state,
            "country":             country,
            "employee_count":      emp_cnt,
            "role":                e.get("role", ""),
            "executive_name":      e.get("name", ""),
            "title":               e.get("title", ""),
            "linkedin_url":        e.get("linkedin_url", ""),
            "direct_email":        e.get("direct_email", ""),
            "work_email":          e.get("work_email", ""),
            "personal_email":      e.get("personal_email", ""),
            "best_email":          e.get("best_email", ""),
            "phone":               e.get("phone", ""),
            "enrich_source":       e.get("enrich_source", ""),
            # ── EDGAR fields ────────────────────────────────────────────────
            "edgar_context":       edgar_context,
            "edgar_ir_name":       edgar_ir_name,
            "edgar_ir_email":      edgar_ir_email,
            "edgar_ir_phone":      edgar_ir_phone,
            "edgar_rofr":          edgar_rofr,
            "edgar_rofr_snippet":  edgar_rofr_snippet,
            "edgar_raise_type":    edgar_raise_type,
            "edgar_raise_amount":  edgar_raise_amt,
            "edgar_raise_date":    edgar_raise_date,
            "edgar_raise_uw":      edgar_raise_uw,
            "edgar_lawyers":       edgar_lawyers,
        })
    return rows


CSV_FIELDNAMES = [
    # Company / contact fields
    "ticker", "company", "website", "industry",
    "city", "state", "country", "employee_count",
    "role", "executive_name", "title", "linkedin_url",
    "direct_email", "work_email", "personal_email", "best_email",
    "phone", "enrich_source",
    # EDGAR intel fields
    "edgar_context",
    "edgar_ir_name", "edgar_ir_email", "edgar_ir_phone",
    "edgar_rofr", "edgar_rofr_snippet",
    "edgar_raise_type", "edgar_raise_amount", "edgar_raise_date", "edgar_raise_uw",
    "edgar_lawyers",
]


def write_csv_header(file_handle) -> csv.DictWriter:
    writer = csv.DictWriter(file_handle, fieldnames=CSV_FIELDNAMES)
    writer.writeheader()
    return writer
