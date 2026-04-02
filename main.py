#!/usr/bin/env python3
"""
main.py — IB Executive Contact Lookup CLI

Usage:
  python main.py AAPL
  python main.py TSLA --output json
  python main.py NVDA --export csv
  python main.py --batch tickers.csv --export csv
"""

from __future__ import annotations
import argparse
import csv
import json
import sys
import time
import os

from lookup.ticker_resolver   import resolve_ticker, split_name
from lookup.linkedin_finder   import find_linkedin_url
from lookup.salesql_enricher  import search_by_name, search_by_name_with_variations, enrich_by_url, enrich_contact
from lookup.output_formatter  import (
    print_rich_table, to_json, to_csv_rows, write_csv_header, CSV_FIELDNAMES
)
from lookup.excel_writer      import build_excel
from lookup.edgar_intel       import get_edgar_intel
from lookup.email_pattern     import fill_missing_emails

SALESQL_DELAY  = 1.2   # seconds between SalesQL calls
LINKEDIN_DELAY = (1.5, 3.0)  # range — only used as bonus step


# ── Core Pipeline ─────────────────────────────────────────────────────────────

def _role_label(title: str) -> str:
    """Classify a title into a short role label."""
    t = title.lower()
    if "chief executive" in t or "ceo" in t:
        return "CEO"
    if "chief financial" in t or "cfo" in t:
        return "CFO"
    if "chief operating" in t or "coo" in t:
        return "COO"
    if "president" in t:
        return "President"
    return title[:20]


def lookup_ticker(
    ticker: str,
    verbose: bool = True,
    skip_edgar: bool = False,
) -> dict | None:
    """
    Run the full pipeline for a single ticker.
    Returns a result dict or None on hard failure.

    Args:
        ticker:      Stock ticker symbol (e.g. "AAPL").
        verbose:     Print progress to stdout.
        skip_edgar:  When True the EDGAR intel step is bypassed (faster).
    """
    ticker = ticker.strip().upper()
    if verbose:
        print(f"\n[→] {ticker}")

    # Step 1: resolve company + officer list
    info = resolve_ticker(ticker)
    if info is None:
        print(f"  [!] Ticker not found: {ticker}")
        return None

    company  = info.get("company", "")
    targets  = info.get("targets", [])

    if verbose:
        print(f"    Company  : {company}")
        print(f"    Industry : {info.get('industry', '')}")
        print(f"    Officers : {len(info.get('executives', []))} total → {len(targets)} targeted")

    result = {
        "ticker":         ticker,
        "company":        company,
        "website":        info.get("website", ""),
        "industry":       info.get("industry", ""),
        "city":           info.get("city", ""),
        "state":          info.get("state", ""),
        "country":        info.get("country", ""),
        "employee_count": info.get("employee_count", ""),
        "executives":     [],
    }

    if not targets:
        if verbose:
            print(f"    [warn] No targeted executives found for {ticker}")
        return result

    for exec_info in targets:
        name  = exec_info["name"]
        title = exec_info["title"]
        role  = _role_label(title)
        first, last = split_name(name)

        if verbose:
            print(f"  → {role}: {name} ({title})")

        # ── Step 2: SalesQL name+org search (PRIMARY — no LinkedIn needed) ──
        if verbose:
            print(f"    SalesQL name search …")
        contact = search_by_name_with_variations(first, last, name, company)
        time.sleep(SALESQL_DELAY)

        if verbose:
            src = contact.get("source", "")
            print(f"      direct_email  = {contact.get('direct_email') or '—'}")
            print(f"      work_email    = {contact.get('work_email')   or '—'}")
            print(f"      phone         = {contact.get('phone')        or '—'}")
            print(f"      source        = {src}")

        # ── Step 3: LinkedIn URL (fallback whenever email is still missing) ──
        li_url = contact.get("linkedin_url", "")  # SalesQL sometimes returns it
        if not contact["best_email"]:
            if verbose:
                print(f"    No email found — trying LinkedIn URL search …")
            li_url = find_linkedin_url(name, company, title,
                                       sleep_range=LINKEDIN_DELAY)
            if verbose:
                print(f"    LinkedIn : {li_url or '—'}")
            if li_url:
                contact2 = enrich_by_url(li_url)
                time.sleep(SALESQL_DELAY)
                # Merge: prefer the richer result
                if contact2["best_email"] or contact2["phone"]:
                    contact = contact2
                    contact["linkedin_url"] = li_url
                    if verbose:
                        print(f"      → URL enrichment: {contact['best_email'] or '—'}")

        result["executives"].append({
            "role":           role,
            "name":           name,
            "title":          title,
            "first_name":     first,
            "last_name":      last,
            "linkedin_url":   contact.get("linkedin_url", "") or li_url,
            "direct_email":   contact.get("direct_email", ""),
            "work_email":     contact.get("work_email", ""),
            "personal_email": contact.get("personal_email", ""),
            "best_email":     contact.get("best_email", ""),
            "phone":          contact.get("phone", ""),
            "enrich_source":  contact.get("source", ""),
        })

    # ── Step 4: Email pattern inference — fill gaps using known company emails ──
    if verbose:
        print(f"  → Email pattern inference …")
    fill_missing_emails(result["executives"], website=result.get("website", ""),
                        verbose=verbose)

    # ── Step 5 (optional): EDGAR intelligence ─────────────────────────────────
    if not skip_edgar:
        if verbose:
            print(f"  → EDGAR Intel …")
        intel = get_edgar_intel(ticker)
        result["edgar_intel"] = intel
        if verbose:
            n_filings = len(intel.get("filings", []))
            n_lawyers = len(intel.get("lawyers", []))
            raises    = intel.get("recent_raises", [])
            rofr_flag = "⚠ ROFR DETECTED" if intel.get("rofr_detected") else "clean"
            print(f"    filings={n_filings}  lawyers={n_lawyers}  "
                  f"raises={len(raises)}  rofr={rofr_flag}")
            if raises:
                print(f"    Raise history (12 mo):")
                for r in raises:
                    print(f"      • {r.get('summary', '—')}")
    else:
        result["edgar_intel"] = {}

    return result


# ── Batch mode ────────────────────────────────────────────────────────────────

def load_tickers_from_file(path: str) -> list[str]:
    tickers = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            val = line.strip().upper()
            if val and val != "TICKER":
                tickers.append(val)
    return tickers


def run_batch(
    tickers: list[str],
    export_csv: str | None = None,
    output_mode: str = "table",
    skip_edgar: bool = False,
) -> None:
    """Process a list of tickers, optionally writing to CSV."""
    csv_writer = None
    csv_fh     = None

    if export_csv:
        csv_fh     = open(export_csv, "w", newline="", encoding="utf-8")
        csv_writer = write_csv_header(csv_fh)
        print(f"[i] Writing CSV → {export_csv}")

    try:
        for idx, ticker in enumerate(tickers, 1):
            print(f"\n{'─'*60}")
            print(f"[{idx}/{len(tickers)}] Processing {ticker}")

            result = lookup_ticker(ticker, verbose=True, skip_edgar=skip_edgar)

            if result is None:
                if csv_writer:
                    csv_writer.writerow({f: "" for f in CSV_FIELDNAMES} | {"ticker": ticker})
                continue

            # Render output
            if output_mode == "json":
                print(to_json(result))
            else:
                print_rich_table(result)

            # Write CSV rows
            if csv_writer:
                rows = to_csv_rows(result)
                if rows:
                    for row in rows:
                        csv_writer.writerow(row)
                else:
                    csv_writer.writerow(
                        {f: "" for f in CSV_FIELDNAMES} | {
                            "ticker":  result["ticker"],
                            "company": result["company"],
                        }
                    )
                if csv_fh:
                    csv_fh.flush()   # write incrementally

    finally:
        if csv_fh:
            csv_fh.close()
            print(f"\n[✓] CSV saved → {export_csv}")

        # Auto-generate Excel after every batch run
        if export_csv and os.path.exists(export_csv):
            xlsx_path = export_csv.replace(".csv", ".xlsx")
            if not xlsx_path.endswith(".xlsx"):
                xlsx_path += ".xlsx"
            try:
                summary = build_excel(export_csv, xlsx_path)
                print(f"[✓] Excel saved → {xlsx_path}")
                print(f"    {summary['records']} executives | "
                      f"{summary['with_email']} emails | "
                      f"{summary['with_phone']} phones")
            except Exception as e:
                print(f"[warn] Excel export failed: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="IB Executive Contact Lookup — CEO/CFO emails + phones from LinkedIn & SalesQL"
    )
    parser.add_argument("ticker",  nargs="?",  help="Stock ticker symbol (e.g. AAPL)")
    parser.add_argument("--batch", metavar="FILE",
                        help="CSV file with one ticker per row (batch mode)")
    parser.add_argument("--output", choices=["table", "json"], default="table",
                        help="Output format (default: table)")
    parser.add_argument("--export", metavar="OUTPUT.csv",
                        help="Save results to a CSV file")
    parser.add_argument("--skip-edgar", action="store_true", default=False,
                        help="Skip EDGAR intel step (faster; no filing/lawyer/ROFR data)")

    args = parser.parse_args()

    # ── Batch mode ─────────────────────────────────────────────────────────
    if args.batch:
        if not os.path.exists(args.batch):
            print(f"[!] File not found: {args.batch}")
            sys.exit(1)
        tickers = load_tickers_from_file(args.batch)
        print(f"[i] Loaded {len(tickers)} tickers from {args.batch}")
        run_batch(
            tickers,
            export_csv=args.export,
            output_mode=args.output,
            skip_edgar=args.skip_edgar,
        )
        return

    # ── Single ticker ───────────────────────────────────────────────────────
    if not args.ticker:
        parser.print_help()
        sys.exit(1)

    result = lookup_ticker(args.ticker, verbose=True, skip_edgar=args.skip_edgar)
    if result is None:
        sys.exit(1)

    if args.output == "json":
        print(to_json(result))
    else:
        print_rich_table(result)

    if args.export:
        with open(args.export, "w", newline="", encoding="utf-8") as f:
            writer = write_csv_header(f)
            for row in to_csv_rows(result):
                writer.writerow(row)
        print(f"\n[✓] Saved → {args.export}")


if __name__ == "__main__":
    main()
