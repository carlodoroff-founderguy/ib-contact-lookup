"""
pages/2_Validate.py — Quality Control & Accuracy Validation
Curvature Securities IB Intelligence Platform

Runs the full 93-ticker benchmark against the Planet MicroCap ground truth
and scores every field. Target: ≥80% weighted accuracy.
"""

from __future__ import annotations

import io
import os
import re
import sys
import time
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd

# ── Path setup ─────────────────────────────────────────────────────────────────
APP_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(APP_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(APP_DIR / ".env")
except ImportError:
    pass

from lookup.ticker_resolver   import resolve_ticker, split_name
from lookup.linkedin_finder   import find_linkedin_url
from lookup.salesql_enricher  import (
    search_by_name_with_variations, enrich_by_url, _empty as salesql_empty,
)
from lookup.financial_fetcher import fetch_financials_safe
from lookup.ir_finder         import find_ir_data
from lookup.schema_builder    import build_row, empty_row, COLUMN_ORDER
from lookup.email_pattern     import fill_missing_emails
from components.styles        import inject_css, render_header, render_section
from components.export        import rows_to_xlsx, export_filename

# ── Ground truth (full 93-ticker set) ─────────────────────────────────────────
# Imported directly from validate_batch.py so there's one source of truth
try:
    from validate_batch import (
        GROUND_TRUTH, SCORE_FIELDS, FIELD_WEIGHTS,
        TOTAL_WEIGHTED, PASS_THRESHOLD_WEIGHTED,
        score_field, run_ticker,
    )
    _HAS_GT = True
except Exception as _e:
    _HAS_GT   = False
    _GT_ERROR = str(_e)

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Validate — Curvature IB",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

_API_KEY  = os.getenv("SALESQL_API_KEY", "")
_API_LIVE = bool(_API_KEY)

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:0.5rem 0 1rem;">
      <div style="font-family:'Playfair Display',serif;font-size:1rem;
                  color:#C9A84C;letter-spacing:0.1em;text-transform:uppercase;">
        Curvature Securities
      </div>
      <div style="font-size:0.65rem;color:#4A4D56;letter-spacing:0.12em;
                  text-transform:uppercase;margin-top:2px;">
        IB Intelligence Platform
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.divider()
    if _API_LIVE:
        st.markdown('<span style="color:#2ECC71;font-size:0.82rem;">● SalesQL Connected</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span style="color:#E74C3C;font-size:0.82rem;">● SalesQL — key missing</span>', unsafe_allow_html=True)
    st.divider()
    skip_linkedin = st.toggle("Skip LinkedIn", value=False)
    delay_s       = st.slider("API delay (s)", 0.5, 5.0, 1.5, 0.5)

render_header(api_connected=_API_LIVE)

# ── Page header ────────────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-bottom:1.5rem;">
  <h1 style="margin-bottom:0.2rem;">Quality Control</h1>
  <p style="color:#8A8D96;font-size:0.88rem;margin:0;">
    Run the 93-ticker Planet MicroCap benchmark · Target accuracy ≥ 80% weighted
  </p>
</div>
""", unsafe_allow_html=True)

if not _HAS_GT:
    st.error(f"Could not load ground truth from validate_batch.py: {_GT_ERROR}")
    st.stop()

ALL_TICKERS = sorted(GROUND_TRUTH.keys())

# ── Info card ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="cs-card" style="border-left:3px solid #C9A84C;">
  <div style="font-size:0.83rem;color:#8A8D96;line-height:1.7;">
    Runs the full enrichment pipeline on all <strong style="color:#F0EDE8;">
    {len(ALL_TICKERS)} reference tickers</strong> and compares every field against
    manually verified ground truth data. Weighted scoring gives CEO/CFO email &amp;
    phone <strong style="color:#C9A84C;">4× weight</strong> over name and IR fields.
    A score ≥ 80% confirms SalesQL is operating at target fidelity.
  </div>
</div>
""", unsafe_allow_html=True)

# ── Run options ────────────────────────────────────────────────────────────────
render_section("Run Options")

opt_col1, opt_col2 = st.columns([3, 2], gap="large")

with opt_col1:
    spot_check = st.multiselect(
        "Spot-check specific tickers (leave empty to run all 93)",
        options=ALL_TICKERS,
        default=[],
        placeholder="Select tickers or leave blank for full run…",
    )
    run_tickers = spot_check if spot_check else ALL_TICKERS

with opt_col2:
    st.markdown(f"""
    <div class="cs-card" style="text-align:center;padding:1rem;">
      <div style="font-family:'JetBrains Mono',monospace;font-size:2rem;
                  color:#C9A84C;font-weight:600;">{len(run_tickers)}</div>
      <div style="font-size:0.72rem;color:#4A4D56;text-transform:uppercase;
                  letter-spacing:0.1em;">tickers selected</div>
      <div style="font-size:0.78rem;color:#8A8D96;margin-top:0.5rem;">
        ~{max(1, round(len(run_tickers) * 0.5))} min estimated
      </div>
    </div>
    """, unsafe_allow_html=True)

run_btn = st.button(
    f"  Run Validation — {len(run_tickers)} Tickers  ",
    type="primary",
    use_container_width=True,
)

st.markdown("<hr>", unsafe_allow_html=True)

# ── Validate uploaded output file (no re-run needed) ──────────────────────────
render_section("Or validate an existing output file")
upload_col1, upload_col2 = st.columns([3, 2])
with upload_col1:
    val_upload = st.file_uploader(
        "Upload a previously exported Excel file to score it",
        type=["xlsx"],
        key="val_upload_file",
        label_visibility="collapsed",
    )

st.markdown("<div style='margin-top:1rem;'></div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# ── Scoring helpers ────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

def _score_rows(rows: list[dict]) -> dict:
    """Score a list of output rows against GROUND_TRUTH. Returns full results dict."""
    all_scores: list[dict] = []
    weighted_pts = 0
    weighted_max = 0

    for row in rows:
        ticker = row.get("Ticker", "")
        gt     = GROUND_TRUTH.get(ticker)
        if not gt:
            continue
        for field in SCORE_FIELDS:
            actual   = row.get(field, "")
            expected = gt.get(field, "")
            passed, reason = score_field(actual, expected, field)
            w = FIELD_WEIGHTS.get(field, 1)
            all_scores.append({
                "Ticker":   ticker,
                "Field":    field,
                "Weight":   w,
                "Pass":     passed,
                "Actual":   str(actual   or ""),
                "Expected": str(expected or ""),
                "Reason":   reason,
            })
            weighted_max += w
            if passed:
                weighted_pts += w

    df = pd.DataFrame(all_scores)
    pct_weighted = round(100 * weighted_pts / weighted_max, 1) if weighted_max else 0.0

    # Per-field summary
    field_summary = []
    for field in SCORE_FIELDS:
        sub = df[df["Field"] == field] if len(df) else pd.DataFrame()
        passes = int(sub["Pass"].sum()) if len(sub) else 0
        total  = len(sub)
        field_summary.append({
            "Field":     field,
            "Weight":    FIELD_WEIGHTS.get(field, 1),
            "Pass":      passes,
            "Fail":      total - passes,
            "Pass Rate": f"{round(100*passes/total,1)}%" if total else "—",
        })

    mismatches = df[df["Pass"] == False].copy() if len(df) else pd.DataFrame()

    return {
        "scores":         df,
        "weighted_pts":   weighted_pts,
        "weighted_max":   weighted_max,
        "pct_weighted":   pct_weighted,
        "field_summary":  field_summary,
        "mismatches":     mismatches,
        "tickers_scored": df["Ticker"].nunique() if len(df) else 0,
    }


def _render_score_result(result: dict) -> None:
    pct   = result["pct_weighted"]
    pts   = result["weighted_pts"]
    mx    = result["weighted_max"]
    color = "#2ECC71" if pct >= 80 else "#E74C3C"
    badge = "✓  ABOVE THRESHOLD" if pct >= 80 else "✗  BELOW THRESHOLD"
    badge_class = "cs-badge-pass" if pct >= 80 else "cs-badge-fail"
    n     = result["tickers_scored"]

    st.markdown(f"""
    <div class="cs-card" style="border-left:4px solid {color};padding:1.5rem 2rem;">
      <div style="display:flex;align-items:baseline;gap:1rem;flex-wrap:wrap;">
        <span style="font-family:'Playfair Display',serif;font-size:3rem;
                     font-weight:700;color:{color};line-height:1;">{pct}%</span>
        <span class="{badge_class}" style="font-size:0.82rem;">{badge}</span>
      </div>
      <div style="font-size:0.88rem;color:#8A8D96;margin-top:0.4rem;">
        {pts} / {mx} weighted points passing &nbsp;·&nbsp; {n} tickers scored
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Field-by-field breakdown
    render_section("Accuracy by Field")
    df_fs = pd.DataFrame(result["field_summary"])

    # Render as styled HTML table
    th = ("background:#1A1D24;color:#8A8D96;font-size:0.68rem;text-transform:uppercase;"
          "letter-spacing:0.1em;padding:8px 14px;text-align:left;border-bottom:1px solid #2A2D35;"
          "font-family:'DM Sans',sans-serif;font-weight:500;")
    td = "padding:8px 14px;border-bottom:1px solid #1A1D24;font-size:0.82rem;"

    rows_html = ""
    for i, r in enumerate(result["field_summary"]):
        bg   = "#111318" if i % 2 == 0 else "#161A22"
        pr   = r["Pass Rate"].replace("%","") if r["Pass Rate"] != "—" else "0"
        try:    pr_f = float(pr)
        except: pr_f = 0.0
        pr_color = "#2ECC71" if pr_f >= 80 else "#E74C3C" if pr_f < 60 else "#F39C12"
        w_badge  = (f'<span style="background:#1A1D24;border:1px solid #2A2D35;'
                    f'border-radius:3px;padding:1px 6px;font-family:JetBrains Mono,monospace;'
                    f'font-size:0.72rem;color:#C9A84C;">{r["Weight"]}×</span>')
        rows_html += (
            f"<tr style='background:{bg};'>"
            f"<td style='{td}color:#F0EDE8;font-weight:500;'>{r['Field']}</td>"
            f"<td style='{td}text-align:center;'>{w_badge}</td>"
            f"<td style='{td}text-align:center;color:#2ECC71;'>{r['Pass']}</td>"
            f"<td style='{td}text-align:center;color:#E74C3C;'>{r['Fail']}</td>"
            f"<td style='{td}text-align:center;font-family:JetBrains Mono,monospace;"
            f"font-weight:600;color:{pr_color};'>{r['Pass Rate']}</td>"
            f"</tr>"
        )

    st.markdown(f"""
    <div style="overflow-x:auto;border:1px solid #2A2D35;border-radius:8px;margin-top:0.5rem;">
    <table style="width:100%;border-collapse:collapse;background:#111318;">
    <thead><tr>
      <th style="{th}">Field</th>
      <th style="{th}text-align:center;">Weight</th>
      <th style="{th}text-align:center;">Pass</th>
      <th style="{th}text-align:center;">Fail</th>
      <th style="{th}text-align:center;">Pass Rate</th>
    </tr></thead>
    <tbody>{rows_html}</tbody>
    </table></div>
    """, unsafe_allow_html=True)

    # Mismatches
    if len(result["mismatches"]) > 0:
        render_section(f"Mismatches — {len(result['mismatches'])} fields")
        st.dataframe(
            result["mismatches"][["Ticker","Field","Actual","Expected","Reason"]],
            use_container_width=True,
            hide_index=True,
            height=350,
        )
        # Export mismatch report
        mis_buf = io.BytesIO()
        with pd.ExcelWriter(mis_buf, engine="openpyxl") as wr:
            result["mismatches"].to_excel(wr, index=False, sheet_name="Mismatches")
        st.download_button(
            "⬇  Download Mismatch Report",
            data      = mis_buf.getvalue(),
            file_name = f"Curvature_Validation_Mismatches_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
            mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.success("🎉 Perfect match — no mismatches!")


# ─────────────────────────────────────────────────────────────────────────────
# ── Validate from uploaded file ────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

if val_upload:
    try:
        df_up = pd.read_excel(val_upload)
        rows  = df_up.to_dict(orient="records")
        st.info(f"Loaded {len(rows)} rows from **{val_upload.name}** — scoring against ground truth…")
        result = _score_rows(rows)
        _render_score_result(result)
    except Exception as e:
        st.error(f"Error reading file: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# ── Live run ────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

if run_btn:
    log_ph   = st.empty()
    prog_ph  = st.empty()
    res_ph   = st.empty()

    log_lines: list[str] = []

    def log(msg: str, level: str = "dim"):
        cls   = {"ok":"ok","err":"err","warn":"warn","dim":"dim","info":"info"}.get(level,"dim")
        clean = msg.replace("<","&lt;").replace(">","&gt;")
        log_lines.append(f'<span class="{cls}">{clean}</span>')
        log_ph.markdown(
            '<div class="cs-log">' + "<br>".join(log_lines[-30:]) + "</div>",
            unsafe_allow_html=True,
        )

    n       = len(run_tickers)
    t_start = time.time()
    rows: list[dict] = []

    for i, tkr in enumerate(run_tickers):
        elapsed  = time.time() - t_start
        avg_pace = elapsed / max(i, 1)
        est_rem  = int(avg_pace * (n - i))
        prog_ph.progress(i / n, text=(
            f"{i+1} / {n}  ·  {tkr}"
            + (f"  ·  est. {est_rem//60}m {est_rem%60}s remaining" if i > 0 else "")
        ))
        try:
            row = run_ticker(tkr, skip_linkedin=skip_linkedin)
            rows.append(row)
        except Exception as e:
            log(f"✗  [{tkr}]  error: {e}", "err")
            rows.append(empty_row(tkr, "Error"))

    prog_ph.progress(1.0, text=f"✅  Done — {len(rows)} tickers processed")

    with res_ph.container():
        result = _score_rows(rows)
        _render_score_result(result)

        # Export full results
        render_section("Export")
        dl1, dl2 = st.columns(2)
        with dl1:
            st.download_button(
                "⬇  Export Full Results to Excel",
                data      = rows_to_xlsx(rows),
                file_name = f"Curvature_Validation_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
                mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with dl2:
            csv_buf = pd.DataFrame(rows, columns=COLUMN_ORDER).to_csv(index=False)
            st.download_button(
                "⬇  Export to CSV",
                data      = csv_buf,
                file_name = f"Curvature_Validation_{datetime.now().strftime('%Y-%m-%d')}.csv",
                mime      = "text/csv",
                use_container_width=True,
            )
