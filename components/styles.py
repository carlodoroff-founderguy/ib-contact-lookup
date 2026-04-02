"""
components/styles.py
Curvature Securities — full CSS design system injection for Streamlit.
"""

import streamlit as st

# ── Color tokens ──────────────────────────────────────────────────────────────
C = {
    "bg_primary":      "#0A0C10",
    "bg_secondary":    "#111318",
    "bg_tertiary":     "#1A1D24",
    "border":          "#2A2D35",
    "gold":            "#C9A84C",
    "gold_dim":        "#8A6F30",
    "text_primary":    "#F0EDE8",
    "text_secondary":  "#8A8D96",
    "text_muted":      "#4A4D56",
    "success":         "#2ECC71",
    "warning":         "#F39C12",
    "error":           "#E74C3C",
}

CSS = """
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Reset Streamlit chrome ─────────────────────────────────────────── */
#MainMenu, footer, header { visibility: hidden !important; }
.stDeployButton { display: none !important; }
[data-testid="stToolbar"] { display: none !important; }

/* ── Base ────────────────────────────────────────────────────────────── */
html, body, .stApp {
    background-color: #0A0C10 !important;
    color: #F0EDE8 !important;
    font-family: 'DM Sans', sans-serif !important;
}

/* Subtle grain texture on the outermost background */
.stApp::before {
    content: '';
    position: fixed;
    inset: 0;
    background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.03'/%3E%3C/svg%3E");
    pointer-events: none;
    z-index: 0;
}

/* ── Main content area ───────────────────────────────────────────────── */
.block-container {
    padding: 2rem 2.5rem 4rem !important;
    max-width: 1700px !important;
    background: transparent !important;
}

/* ── Sidebar ─────────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background-color: #0D0F14 !important;
    border-right: 1px solid #2A2D35 !important;
}
section[data-testid="stSidebar"] * {
    color: #F0EDE8 !important;
}
section[data-testid="stSidebar"] .stSlider > div { color: #8A8D96 !important; }

/* ── Typography ──────────────────────────────────────────────────────── */
h1, h2, h3 {
    font-family: 'Playfair Display', serif !important;
    color: #F0EDE8 !important;
    letter-spacing: -0.3px;
}
h1 { font-size: 1.9rem !important; font-weight: 700 !important; }
h2 { font-size: 1.4rem !important; font-weight: 600 !important; }
h3 { font-size: 1.1rem !important; font-weight: 600 !important; color: #C9A84C !important; }

p, span, div, label {
    font-family: 'DM Sans', sans-serif !important;
    color: #F0EDE8 !important;
}

/* ── Inputs ──────────────────────────────────────────────────────────── */
.stTextInput input,
.stTextArea textarea,
.stSelectbox select,
.stMultiSelect [data-baseweb="select"] {
    background-color: #1A1D24 !important;
    border: 1px solid #2A2D35 !important;
    border-radius: 6px !important;
    color: #F0EDE8 !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.9rem !important;
    padding: 0.5rem 0.75rem !important;
    transition: border-color 0.15s ease !important;
}
.stTextInput input:focus,
.stTextArea textarea:focus {
    border-color: #C9A84C !important;
    box-shadow: 0 0 0 2px rgba(201,168,76,0.15) !important;
    outline: none !important;
}
.stTextInput label,
.stTextArea label {
    color: #8A8D96 !important;
    font-size: 0.78rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    font-weight: 500 !important;
}

/* ── Buttons ─────────────────────────────────────────────────────────── */
.stButton > button {
    background: transparent !important;
    border: 1px solid #2A2D35 !important;
    color: #8A8D96 !important;
    border-radius: 5px !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.82rem !important;
    transition: all 0.15s ease !important;
}
.stButton > button:hover {
    border-color: #C9A84C !important;
    color: #C9A84C !important;
}
.stButton > button[kind="primary"] {
    background: #C9A84C !important;
    border: none !important;
    color: #0A0C10 !important;
    font-weight: 700 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    font-size: 0.8rem !important;
    padding: 0.65rem 1.5rem !important;
}
.stButton > button[kind="primary"]:hover {
    background: #D4B660 !important;
    color: #0A0C10 !important;
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 12px rgba(201,168,76,0.3) !important;
}

/* ── Download buttons ────────────────────────────────────────────────── */
.stDownloadButton > button {
    background: #111318 !important;
    border: 1px solid #2A2D35 !important;
    color: #C9A84C !important;
    border-radius: 5px !important;
    font-weight: 600 !important;
    font-size: 0.8rem !important;
    letter-spacing: 0.05em !important;
    transition: all 0.15s ease !important;
}
.stDownloadButton > button:hover {
    border-color: #C9A84C !important;
    background: #1A1D24 !important;
}

/* ── File uploader ───────────────────────────────────────────────────── */
[data-testid="stFileUploader"] {
    background: #111318 !important;
    border: 1px dashed #2A2D35 !important;
    border-radius: 8px !important;
    padding: 1rem !important;
    transition: border-color 0.15s ease !important;
}
[data-testid="stFileUploader"]:hover {
    border-color: #C9A84C !important;
}

/* ── Progress bar ────────────────────────────────────────────────────── */
.stProgress > div > div > div > div {
    background: linear-gradient(90deg, #8A6F30, #C9A84C) !important;
    border-radius: 4px !important;
}
.stProgress > div > div {
    background: #1A1D24 !important;
    border-radius: 4px !important;
}

/* ── Metrics ─────────────────────────────────────────────────────────── */
[data-testid="stMetricValue"] {
    color: #F0EDE8 !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 1.3rem !important;
    font-weight: 500 !important;
}
[data-testid="stMetricLabel"] {
    color: #8A8D96 !important;
    font-size: 0.72rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
}
[data-testid="metric-container"] {
    background: #111318 !important;
    border: 1px solid #2A2D35 !important;
    border-radius: 8px !important;
    padding: 0.9rem 1rem !important;
}
[data-testid="stMetricDelta"] svg { display: none !important; }

/* ── Tabs ────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid #2A2D35 !important;
    gap: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: #4A4D56 !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.83rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    padding: 0.65rem 1.4rem !important;
    border: none !important;
    border-bottom: 2px solid transparent !important;
    transition: all 0.15s !important;
}
.stTabs [aria-selected="true"] {
    color: #C9A84C !important;
    border-bottom: 2px solid #C9A84C !important;
}
.stTabs [data-baseweb="tab"]:hover {
    color: #F0EDE8 !important;
    background: transparent !important;
}
.stTabs [data-baseweb="tab-panel"] {
    background: transparent !important;
    padding-top: 1.5rem !important;
}

/* ── Expander ────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    background: #111318 !important;
    border: 1px solid #2A2D35 !important;
    border-radius: 6px !important;
}
[data-testid="stExpander"] summary {
    color: #8A8D96 !important;
    font-size: 0.8rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
}

/* ── DataFrames / Tables ─────────────────────────────────────────────── */
.stDataFrame {
    border: 1px solid #2A2D35 !important;
    border-radius: 8px !important;
    overflow: hidden !important;
}
.stDataFrame [data-testid="stDataFrameResizable"] {
    background: #111318 !important;
}

/* ── Alerts ──────────────────────────────────────────────────────────── */
[data-testid="stAlert"] {
    border-radius: 6px !important;
    font-size: 0.83rem !important;
}
.stSuccess {
    background: rgba(46,204,113,0.1) !important;
    border: 1px solid rgba(46,204,113,0.3) !important;
    color: #2ECC71 !important;
}
.stError {
    background: rgba(231,76,60,0.1) !important;
    border: 1px solid rgba(231,76,60,0.3) !important;
    color: #E74C3C !important;
}
.stWarning {
    background: rgba(243,156,18,0.1) !important;
    border: 1px solid rgba(243,156,18,0.3) !important;
    color: #F39C12 !important;
}
.stInfo {
    background: rgba(201,168,76,0.08) !important;
    border: 1px solid rgba(201,168,76,0.2) !important;
    color: #C9A84C !important;
}

/* ── Spinner ─────────────────────────────────────────────────────────── */
.stSpinner > div {
    border-top-color: #C9A84C !important;
}

/* ── Divider ─────────────────────────────────────────────────────────── */
hr {
    border-color: #2A2D35 !important;
    margin: 1.2rem 0 !important;
}

/* ── Toggle / Checkbox ───────────────────────────────────────────────── */
.stCheckbox label, .stToggle label {
    color: #8A8D96 !important;
    font-size: 0.83rem !important;
}

/* ── Slider ──────────────────────────────────────────────────────────── */
.stSlider [data-baseweb="slider"] [role="slider"] {
    background: #C9A84C !important;
}
.stSlider [data-baseweb="slider"] [data-testid="stSliderTrackFill"] {
    background: #C9A84C !important;
}

/* ── Custom component classes ────────────────────────────────────────── */

/* Topbar wordmark */
.cs-wordmark {
    font-family: 'Playfair Display', serif;
    font-size: 1.25rem;
    font-weight: 700;
    color: #C9A84C;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    line-height: 1.1;
}
.cs-wordmark-sub {
    font-family: 'DM Sans', sans-serif;
    font-size: 0.68rem;
    color: #4A4D56;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    margin-top: 1px;
}
.cs-header {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 0.6rem 0 1.2rem;
    border-bottom: 1px solid #2A2D35;
    margin-bottom: 1.8rem;
}
.cs-header-right {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 0.6rem;
}
.cs-status-dot {
    display: inline-block;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: #2ECC71;
    margin-right: 4px;
    box-shadow: 0 0 6px rgba(46,204,113,0.6);
}
.cs-status-dot.offline { background: #E74C3C; box-shadow: 0 0 6px rgba(231,76,60,0.6); }
.cs-status-label {
    font-size: 0.72rem;
    color: #4A4D56;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

/* Status log */
.cs-log {
    background: #0D0F14;
    border: 1px solid #1A1D24;
    border-left: 2px solid #C9A84C;
    border-radius: 4px;
    padding: 0.6rem 1rem;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.73rem;
    color: #4A4D56;
    max-height: 180px;
    overflow-y: auto;
    line-height: 1.7;
    margin: 0.5rem 0;
}
.cs-log .ok   { color: #2ECC71; }
.cs-log .warn { color: #F39C12; }
.cs-log .err  { color: #E74C3C; }
.cs-log .dim  { color: #4A4D56; }
.cs-log .info { color: #C9A84C; }

/* Contact cell styling used inside rendered HTML tables */
.cs-email-work    { font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: #F0EDE8; }
.cs-email-personal{ font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: #F39C12; }
.cs-email-missing { font-family: 'DM Sans', sans-serif;       font-size: 0.8rem; color: #4A4D56; font-style: italic; }
.cs-phone-work    { font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: #2ECC71; }
.cs-phone-mobile  { font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: #F0EDE8; }
.cs-phone-missing { font-family: 'DM Sans', sans-serif;       font-size: 0.8rem; color: #4A4D56; font-style: italic; }

/* Section label */
.cs-section {
    font-size: 0.7rem;
    font-weight: 600;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #4A4D56;
    border-bottom: 1px solid #1A1D24;
    padding-bottom: 0.35rem;
    margin: 1.5rem 0 0.8rem;
}

/* Ticker chip */
.cs-chip {
    display: inline-block;
    background: #1A1D24;
    border: 1px solid #2A2D35;
    border-radius: 4px;
    padding: 1px 8px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    color: #C9A84C;
    margin: 2px 3px;
}

/* Accuracy badge */
.cs-badge-pass {
    display: inline-block;
    background: rgba(46,204,113,0.12);
    border: 1px solid rgba(46,204,113,0.3);
    color: #2ECC71;
    border-radius: 4px;
    padding: 0.15rem 0.6rem;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.05em;
}
.cs-badge-fail {
    display: inline-block;
    background: rgba(231,76,60,0.12);
    border: 1px solid rgba(231,76,60,0.3);
    color: #E74C3C;
    border-radius: 4px;
    padding: 0.15rem 0.6rem;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.05em;
}

/* Card panel */
.cs-card {
    background: #111318;
    border: 1px solid #2A2D35;
    border-radius: 8px;
    padding: 1.2rem 1.5rem;
    margin-bottom: 1rem;
}

/* Empty state */
.cs-empty {
    text-align: center;
    padding: 4rem 2rem;
    color: #2A2D35;
}
.cs-empty-icon {
    font-size: 3rem;
    margin-bottom: 0.8rem;
    opacity: 0.4;
}
.cs-empty-title {
    font-family: 'Playfair Display', serif;
    font-size: 1.2rem;
    color: #2A2D35;
    margin-bottom: 0.3rem;
}
.cs-empty-sub {
    font-size: 0.82rem;
    color: #2A2D35;
}
"""


def inject_css() -> None:
    """Call this once at the top of every page to apply the full design system."""
    st.markdown(f"<style>{CSS}</style>", unsafe_allow_html=True)


def render_header(api_connected: bool = True) -> None:
    """Render the top wordmark + live status bar."""
    status_html = (
        '<span class="cs-status-dot"></span>'
        '<span class="cs-status-label">Live</span>'
        if api_connected else
        '<span class="cs-status-dot offline"></span>'
        '<span class="cs-status-label">Offline</span>'
    )
    st.markdown(f"""
    <div class="cs-header">
      <div>
        <div class="cs-wordmark">Curvature Securities</div>
        <div class="cs-wordmark-sub">Investment Banking Intelligence</div>
      </div>
      <div class="cs-header-right">{status_html}</div>
    </div>
    """, unsafe_allow_html=True)


def render_section(label: str) -> None:
    st.markdown(f'<div class="cs-section">{label}</div>', unsafe_allow_html=True)


def render_empty_state() -> None:
    st.markdown("""
    <div class="cs-empty">
      <div class="cs-empty-icon">◈</div>
      <div class="cs-empty-title">Enter tickers above to begin</div>
      <div class="cs-empty-sub">Results will appear here · Supports single or batch input</div>
    </div>
    """, unsafe_allow_html=True)
