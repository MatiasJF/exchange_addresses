# BSV Top 1000 Address Monitor

Monitoring dashboard for BSV's top 1000 richest addresses, tracking balance fluctuations over time (similar to nodecharts.com).

## Data Source

Bitails API: `GET https://api.bitails.io/analytics/address/rich`
- Returns all 1000 addresses in a single call
- Free tier: 10 TPS, 1000 requests/day
- Daily collector uses **1 request/day**

## Setup

```bash
cd /Users/matiasjackson/Documents/Proyects/exchanges_listings/exchange_addresses
python3 -m venv .venv
.venv/bin/pip install pandas pyarrow streamlit
```

## Usage

### Fetch a snapshot
```bash
.venv/bin/python collector.py                  # today
.venv/bin/python collector.py --date 2026-04-08 # specific date
```

### Run the dashboard
```bash
.venv/bin/streamlit run app.py
```
Password: `bsv2026`

### Install daily cron
```bash
bash install_cron.sh
```

## Project Structure

```
exchange_addresses/
├── collector.py          # Daily snapshot fetcher
├── analysis.py           # Fluctuation detection, aggregation
├── app.py                # Streamlit dashboard (6 tabs)
├── install_cron.sh       # Cron installer helper
├── data/
│   ├── snapshots/        # Daily JSON files (YYYY-MM-DD.json)
│   ├── labels.json       # Known exchange/entity labels
│   ├── timeseries.parquet # Rebuilt from snapshots
│   └── collector.log     # Cron log (created on first run)
└── .venv/
```

## Dashboard Tabs

1. **Overview** — KPIs, top 1000 table, 24h/7d changes, top 20 bar chart
2. **Big Movers** — Addresses exceeding configurable thresholds (1d/7d/30d windows)
3. **Address Detail** — Search address, view historical chart, add/edit labels
4. **Aggregate Trends** — Total BSV, Herfindahl concentration, address tier counts
5. **Labels** — Manage known exchange/entity labels, grouped totals
6. **Raw Data** — Browse/download raw snapshot JSON and timeseries CSV

## Key Design Notes

- **Snapshots are source of truth.** `timeseries.parquet` is rebuilt from all snapshots on every collector run.
- **Address churn handled** — addresses entering/leaving the top 1000 appear as NaN in the timeseries.
- **No enrichment in daily run** — `/address/{addr}/details` would require 1000 API calls and exhaust the daily quota. Build a separate enrichment script if needed.
