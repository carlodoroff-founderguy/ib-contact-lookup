"""
financial_fetcher.py

Fetch all 12 financial columns required by the output schema:

  5.  Stock Price (Most Recent)       ← latest close
  6.  Market Cap (Most Recent)        ← raw dollars
  7.  Cash (Latest K)                 ← annual cash & equivalents, $M
  8.  Cash (Latest Q)                 ← quarterly cash & equivalents, $M
  9.  1M Share Volume                 ← rolling 30-day total shares
  10. 1D $ Share Volume               ← most recent day's volume × price
  11. Cash from Ops (Latest K)        ← annual operating cash flow, $M
  12. Cash from Ops (Latest Q)        ← quarterly operating cash flow, $M

All dollar amounts in $millions.  Volume figures are raw counts/dollars.
Returns a dict with these exact keys; missing values → None.
"""

from __future__ import annotations

import re
import time
from typing import Optional

import requests

try:
    import yfinance as yf
    _HAS_YFINANCE = True
except ImportError:
    _HAS_YFINANCE = False

try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False


# ── Constants ─────────────────────────────────────────────────────────────────

YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ib-contact-lookup/1.0; +https://github.com)"
}

# Balance-sheet row names for cash, in priority order
_CASH_ROWS = [
    "Cash And Cash Equivalents",
    "Cash",
    "CashAndCashEquivalents",
    "Cash Cash Equivalents And Short Term Investments",
    "CashCashEquivalentsAndShortTermInvestments",
    "Cash And Short Term Investments",
]

# Cash-flow row names for operating cash flow, in priority order
_OPS_ROWS = [
    "Operating Cash Flow",
    "Total Cash From Operating Activities",
    "Cash Flows From Used In Operating Activities",
    "Net Cash Provided By Used In Operating Activities",
    "NetCashProvidedByUsedInOperatingActivities",
]

_EMPTY: dict = {
    "stock_price":       None,
    "market_cap":        None,
    "cash_annual":       None,   # $M
    "cash_quarterly":    None,   # $M
    "volume_1m":         None,
    "volume_1d_dollar":  None,
    "ops_annual":        None,   # $M
    "ops_quarterly":     None,   # $M
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_millions(value) -> Optional[float]:
    """Convert a raw dollar value to millions, rounded to 3 decimal places."""
    try:
        v = float(value)
        return round(v / 1_000_000, 3)
    except (TypeError, ValueError):
        return None


def _first_value(df, row_names: list[str]):
    """
    Return the most recent non-null value from a DataFrame indexed by metric names.
    df columns = dates (newest first).
    """
    if df is None or df.empty:
        return None
    for name in row_names:
        if name in df.index:
            row = df.loc[name]
            for v in row:
                try:
                    if v is not None and str(v) not in ("", "nan", "None", "<NA>"):
                        return float(v)
                except (TypeError, ValueError):
                    continue
    return None


def _normalize_ticker(ticker: str) -> str:
    """
    Map tickers to Yahoo Finance symbols:
      - TSX: 'BYL' → 'BYL.TO' (if explicitly ends with -CA)
      - OTC  tickers: pass through unchanged
      - Canadian tickers ending in -CA or with .V / .CN
    """
    t = ticker.strip().upper()
    # Handle explicit Canadian markers
    if t.endswith("-CA"):
        return t[:-3] + ".TO"
    if t.endswith(".V") or t.endswith("-V"):
        return re.sub(r"\.V$|-V$", ".V", t)
    if t.endswith(".CN"):
        return t
    return t


# ── Main fetcher ──────────────────────────────────────────────────────────────

def fetch_financials(ticker: str) -> dict:
    """
    Return dict with all 8 financial metrics.
    Falls back to Yahoo Finance JSON API if yfinance returns empty data.
    """
    yf_ticker = _normalize_ticker(ticker)

    result = dict(_EMPTY)

    # ── 1. Price + Market Cap ─────────────────────────────────────────────────
    if _HAS_YFINANCE:
        try:
            t = yf.Ticker(yf_ticker)
            info = t.info or {}

            # Price
            price = (
                info.get("currentPrice")
                or info.get("regularMarketPrice")
                or info.get("previousClose")
            )
            if price:
                result["stock_price"] = float(price)

            # Market cap
            mc = info.get("marketCap")
            if mc:
                result["market_cap"] = float(mc)

        except Exception as e:
            print(f"    [financial] yfinance info error for {yf_ticker}: {e}")

    # ── 2. Balance sheet — Cash ───────────────────────────────────────────────
    if _HAS_YFINANCE and _HAS_PANDAS:
        try:
            t = yf.Ticker(yf_ticker)

            # Annual
            bs_a = t.balance_sheet
            val = _first_value(bs_a, _CASH_ROWS)
            if val is not None:
                result["cash_annual"] = _to_millions(val)

            # Quarterly
            bs_q = t.quarterly_balance_sheet
            val = _first_value(bs_q, _CASH_ROWS)
            if val is not None:
                result["cash_quarterly"] = _to_millions(val)

        except Exception as e:
            print(f"    [financial] balance sheet error for {yf_ticker}: {e}")

    # ── 3. Cash flow — Operating ──────────────────────────────────────────────
    if _HAS_YFINANCE and _HAS_PANDAS:
        try:
            t = yf.Ticker(yf_ticker)

            # Annual
            cf_a = t.cashflow
            val = _first_value(cf_a, _OPS_ROWS)
            if val is not None:
                result["ops_annual"] = _to_millions(val)

            # Quarterly
            cf_q = t.quarterly_cashflow
            val = _first_value(cf_q, _OPS_ROWS)
            if val is not None:
                result["ops_quarterly"] = _to_millions(val)

        except Exception as e:
            print(f"    [financial] cashflow error for {yf_ticker}: {e}")

    # ── 4. Volume ─────────────────────────────────────────────────────────────
    if _HAS_YFINANCE and _HAS_PANDAS:
        try:
            t = yf.Ticker(yf_ticker)
            hist = t.history(period="1mo", auto_adjust=True)

            if hist is not None and not hist.empty:
                # 1-month total share volume
                result["volume_1m"] = float(hist["Volume"].sum())

                # 1-day dollar volume = latest day volume × close price
                last_row = hist.iloc[-1]
                vol      = float(last_row.get("Volume", 0) or 0)
                close    = float(last_row.get("Close", 0) or 0)
                if vol and close:
                    result["volume_1d_dollar"] = round(vol * close, 2)

        except Exception as e:
            print(f"    [financial] volume history error for {yf_ticker}: {e}")

    # ── 5. Yahoo Finance JSON fallback for price / market cap ────────────────
    if result["stock_price"] is None or result["market_cap"] is None:
        try:
            url    = f"https://query2.finance.yahoo.com/v8/finance/chart/{yf_ticker}"
            params = {"interval": "1d", "range": "1d"}
            resp   = requests.get(url, params=params, headers=YF_HEADERS, timeout=12)
            if resp.status_code == 200:
                data = resp.json()
                meta = (
                    ((data.get("chart") or {}).get("result") or [{}])[0]
                    .get("meta", {})
                )
                if result["stock_price"] is None:
                    p = meta.get("regularMarketPrice") or meta.get("previousClose")
                    if p:
                        result["stock_price"] = float(p)
                if result["market_cap"] is None:
                    pass  # chart endpoint doesn't include market cap
        except Exception:
            pass

    return result


def fetch_financials_safe(ticker: str) -> dict:
    """
    Wrapper with retry logic.  Returns _EMPTY on total failure.
    """
    for attempt in range(2):
        try:
            return fetch_financials(ticker)
        except Exception as e:
            print(f"    [financial] attempt {attempt+1} failed for {ticker}: {e}")
            if attempt == 0:
                time.sleep(2)
    return dict(_EMPTY)
