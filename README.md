# IB Research Engine

> Full-spectrum investment banking research tool. Ticker → 21-column enriched output with CEO/CFO emails + phones, financial metrics, and IR data.
> Pipeline: **Ticker → yfinance → LinkedIn/DuckDuckGo → SalesQL → xlsx**

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Verify API key in .env (already pre-filled)
cat .env   # should show SALESQL_API_KEY=...

# 3. Launch the web UI
streamlit run app.py
```

Then open **http://localhost:8501** in your browser.

---

## Output Schema (21 columns, exact order)

| # | Column | Description |
|---|--------|-------------|
| 1 | Company Name | Full legal name |
| 2 | Ticker | Exchange ticker |
| 3 | Industry | Sector/industry |
| 4 | Exchange | NASDAQ / NYSE / US OTC / TSX |
| 5 | Stock Price (Most Recent) | Latest close price |
| 6 | Market Cap (Most Recent) | Raw market cap in $ |
| 7 | Cash (Latest K) | Annual cash, $M |
| 8 | Cash (Latest Q) | Quarterly cash, $M |
| 9 | 1M Share Volume | 30-day total volume |
| 10 | 1D $ Share Volume | Latest day volume × price |
| 11 | Cash from Ops (Latest K) | Annual operating cash flow, $M |
| 12 | Cash from Ops (Latest Q) | Quarterly operating cash flow, $M |
| 13 | CEO | Full name + credentials |
| 14 | CFO | Full name + credentials |
| 15 | CEO EMAIL | Work > personal (flagged) > Not found |
| 16 | CEO NUMBER | direct/work prefix, or mobile as-is |
| 17 | CFO EMAIL | Same priority as CEO EMAIL |
| 18 | CFO NUMBER | Same priority as CEO NUMBER |
| 19 | IR Email | IR page email or third-party IR firm email |
| 20 | IR Contact | Named IR person + firm |
| 21 | IR Page | Full IR URL |

---

## Phone Priority (updated)

Phone numbers from SalesQL are selected in this order:
1. **Direct** dial (highest priority — labeled `work +1 ...` in output)
2. **Work** / office line (labeled `work +1 ...`)
3. **Mobile** / personal (no prefix — stored as-is)

---

## Email Priority

1. Work email → stored clean: `john@corp.com`
2. Personal only → flagged: `john@gmail.com (no work provided)`
3. None found → `Not found`
4. Not on LinkedIn → `Not on LinkedIn`

---

## IR Contact Logic

IR data is sourced from **company IR pages** (web scraping) — SalesQL is NOT called for IR contacts unless:
- A named IR contact is found on the page
- BUT no email is available (e.g., only a name is listed)

In that case the contact's name is run through **LinkedIn → SalesQL** to find their email.

---

## SalesQL API — confirmed endpoint

```
GET  https://api-public.salesql.com/v1/persons/enrich/
     ?linkedin_url=https://linkedin.com/in/profile-slug    ← URL lookup
     ?first_name=John&last_name=Smith&organization_name=Acme  ← name lookup
Header: x-api-key: YOUR_KEY
        Authorization: Bearer YOUR_KEY
```

---

## File structure

```
ib-contact-lookup/
├── app.py                      # Streamlit web UI (main entry point)
├── main.py                     # CLI entry point (legacy)
├── reference_data.json         # 95-row manually validated reference sheet
├── .env                        # API keys (never commit)
├── requirements.txt
└── lookup/
    ├── ticker_resolver.py      # yfinance → company + exec names
    ├── linkedin_finder.py      # name + company → LinkedIn URL (DuckDuckGo)
    ├── salesql_enricher.py     # SalesQL API: email + phone enrichment
    ├── financial_fetcher.py    # yfinance → 12 financial columns
    ├── ir_finder.py            # IR page scraping → IR email/contact/page
    ├── schema_builder.py       # Assembles the 21-column output row
    ├── email_pattern.py        # Pattern inference for missing emails
    ├── output_formatter.py     # Rich terminal table / JSON / CSV
    └── excel_writer.py         # xlsx export utilities
```

---

## Error handling

| Scenario | Behaviour |
|----------|-----------|
| Invalid ticker | Writes error row, continues |
| yfinance returns no officers | Falls back to Yahoo Finance JSON API |
| LinkedIn not found | Skips LinkedIn step, tries name+company SalesQL |
| SalesQL 429 rate limit | Sleeps 60s, retries once |
| SalesQL 401 | Logs auth error, marks fields as API Error |
| Network timeout | Retries once, then marks as API Error |
| IR page not reachable | Leaves IR fields blank |

---

## Validation mode

The **Validate vs Reference** tab in the web UI runs the pipeline against all 95 manually-verified reference tickers and reports field-level accuracy. Target: ≥ 95% match rate.

You can also upload a previously-exported xlsx to validate without re-running the pipeline.
