"""
pages/3_History.py — Run History
Curvature Securities IB Intelligence Platform

Shows a log of every past research batch. Each row is clickable to
re-download the Excel output without re-running the pipeline.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd

APP_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(APP_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(APP_DIR / ".env")
except ImportError:
    pass

from components.styles import inject_css, render_header, render_section
from components.export import rows_to_xlsx, rows_to_csv, COLUMN_ORDER

HISTORY_FILE = APP_DIR / "history" / "runs.json"

st.set_page_config(
    page_title="History — Curvature IB",
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

render_header(api_connected=_API_LIVE)

st.markdown("""
<div style="margin-bottom:1.5rem;">
  <h1 style="margin-bottom:0.2rem;">Run History</h1>
  <p style="color:#8A8D96;font-size:0.88rem;margin:0;">
    Every research batch is saved automatically · Re-download any export without re-running
  </p>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# ── History helpers ────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

def _load_history() -> list[dict]:
    try:
        if HISTORY_FILE.exists():
            with open(HISTORY_FILE) as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


def _save_history(runs: list[dict]) -> None:
    try:
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(HISTORY_FILE, "w") as f:
            json.dump(runs, f, indent=2, default=str)
    except Exception:
        pass


def _fields_populated(rows: list[dict]) -> int:
    """Count non-empty contact fields across all rows."""
    contact_fields = ["CEO EMAIL","CEO NUMBER","CFO EMAIL","CFO NUMBER","IR Email"]
    _blank = {"","not found","not on linkedin","not on sql","n/a","—","api error","error"}
    count = 0
    for row in rows:
        for f in contact_fields:
            v = str(row.get(f,"") or "").strip().lower()
            if v not in _blank:
                count += 1
    return count


# ─────────────────────────────────────────────────────────────────────────────
# ── Render ─────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

runs = _load_history()

if not runs:
    st.markdown("""
    <div class="cs-empty">
      <div class="cs-empty-icon">◈</div>
      <div class="cs-empty-title">No runs yet</div>
      <div class="cs-empty-sub">
        Run a research batch on the Research page — it will appear here automatically.
      </div>
    </div>
    """, unsafe_allow_html=True)
else:
    # Summary stats
    total_runs    = len(runs)
    total_tickers = sum(r.get("ticker_count", 0) for r in runs)
    last_run_dt   = runs[-1].get("timestamp", "—") if runs else "—"

    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("Total Runs",      total_runs)
    mc2.metric("Total Tickers",   total_tickers)
    mc3.metric("Last Run",        last_run_dt[:16] if len(last_run_dt) > 16 else last_run_dt)

    st.markdown("<div style='margin:1rem 0;'></div>", unsafe_allow_html=True)
    render_section("Run Log")

    # Table header
    th = ("background:#1A1D24;color:#8A8D96;font-size:0.68rem;text-transform:uppercase;"
          "letter-spacing:0.1em;padding:9px 14px;text-align:left;border-bottom:1px solid #2A2D35;"
          "font-family:'DM Sans',sans-serif;font-weight:500;white-space:nowrap;")
    td = "padding:9px 14px;border-bottom:1px solid #1A1D24;font-size:0.82rem;vertical-align:middle;"

    header_html = (
        f"<tr>"
        f"<th style='{th}'>#</th>"
        f"<th style='{th}'>Date &amp; Time</th>"
        f"<th style='{th}'>Tickers</th>"
        f"<th style='{th}'>Contact Fields Found</th>"
        f"<th style='{th}'>Tickers Processed</th>"
        f"<th style='{th}'>Duration</th>"
        f"</tr>"
    )

    rows_html = ""
    for i, run in enumerate(reversed(runs)):
        idx      = total_runs - i
        bg       = "#111318" if i % 2 == 0 else "#161A22"
        ts       = run.get("timestamp", "—")[:16]
        n_tick   = run.get("ticker_count", 0)
        n_fields = run.get("fields_populated", "—")
        n_proc   = run.get("tickers_processed", n_tick)
        dur      = run.get("duration_sec")
        dur_str  = f"{int(dur//60)}m {int(dur%60)}s" if dur else "—"

        tickers_str = ", ".join(run.get("tickers", [])[:6])
        if len(run.get("tickers", [])) > 6:
            tickers_str += f" +{len(run.get('tickers',[]))-6} more"

        rows_html += (
            f"<tr style='background:{bg};'>"
            f"<td style='{td}color:#4A4D56;font-family:JetBrains Mono,monospace;'>#{idx}</td>"
            f"<td style='{td}color:#F0EDE8;font-family:JetBrains Mono,monospace;font-size:0.78rem;'>{ts}</td>"
            f"<td style='{td}color:#8A8D96;font-size:0.78rem;'>{tickers_str}</td>"
            f"<td style='{td}color:#2ECC71;font-family:JetBrains Mono,monospace;'>{n_fields}</td>"
            f"<td style='{td}color:#C9A84C;font-family:JetBrains Mono,monospace;'>{n_proc}</td>"
            f"<td style='{td}color:#8A8D96;'>{dur_str}</td>"
            f"</tr>"
        )

    st.markdown(f"""
    <div style="overflow-x:auto;border:1px solid #2A2D35;border-radius:8px;margin-bottom:1.5rem;">
    <table style="width:100%;border-collapse:collapse;background:#111318;">
    <thead>{header_html}</thead>
    <tbody>{rows_html}</tbody>
    </table></div>
    """, unsafe_allow_html=True)

    # ── Per-run downloads ──────────────────────────────────────────────────────
    render_section("Re-download a Past Run")
    st.caption("Select a run below to download its Excel output without re-running the pipeline.")

    run_labels = [
        f"#{total_runs - i}  —  {r.get('timestamp','')[:16]}  —  "
        f"{r.get('ticker_count',0)} tickers"
        for i, r in enumerate(reversed(runs))
    ]
    selected_label = st.selectbox(
        "Select run",
        options=run_labels,
        label_visibility="collapsed",
    )

    if selected_label:
        sel_idx = total_runs - 1 - run_labels.index(selected_label)
        sel_run = runs[sel_idx]
        sel_rows = sel_run.get("rows", [])

        if sel_rows:
            dl1, dl2 = st.columns(2)
            ts_safe  = sel_run.get("timestamp","")[:10]
            with dl1:
                st.download_button(
                    "⬇  Re-download Excel",
                    data      = rows_to_xlsx(sel_rows),
                    file_name = f"Curvature_Research_{ts_safe}.xlsx",
                    mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            with dl2:
                st.download_button(
                    "⬇  Re-download CSV",
                    data      = rows_to_csv(sel_rows),
                    file_name = f"Curvature_Research_{ts_safe}.csv",
                    mime      = "text/csv",
                    use_container_width=True,
                )
        else:
            st.warning("No row data stored for this run — it may have been saved in a previous format.")

    # ── Clear history ──────────────────────────────────────────────────────────
    st.markdown("<div style='margin-top:2rem;'></div>", unsafe_allow_html=True)
    with st.expander("⚠  Danger zone"):
        st.caption("This permanently deletes all run history. The action cannot be undone.")
        if st.button("Clear all history", type="secondary"):
            _save_history([])
            st.success("History cleared.")
            st.rerun()
