# Architecture

NVDA shares tracking dashboard. Runs as two Docker containers (primary; see [`docker-compose.yml`](docker-compose.yml)):

- `backend` -> [`dashboard/backend/server.py`](dashboard/backend/server.py) (Flask API, port 8080)
- `frontend` -> [`dashboard/serve-prod.js`](dashboard/serve-prod.js) serving the built Angular app on port 4201, proxying `/api` to the backend

Run / deploy after changes (from repo root):

```bash
sudo docker compose up -d --build     # build + run
sudo docker compose logs -f           # logs
sudo docker compose down              # stop
```

Or `./start.sh` (auto-detects sudo). pm2 ([`ecosystem.config.js`](ecosystem.config.js)) remains as an
alternative runner but should not run at the same time as Docker (port/DB conflict).

## Layout

```
configs/                 # nvShares.db (SQLite), tax_config.json, templates/, historic_data.json
helpers/                 # shared data layer (importable by backend + scripts)
  config.py              # centralized config + secrets (env-overridable; loads dashboard/.env)
  db.py                  # DB class (SQLite tables: NSU/ESPP/SellOut)
  market.py              # RupeeConv: USD->INR FX + NVDA stock/live/pre-post prices
  stock_data.py          # OwnStockData: builds holdings/sold display dataframes
  tax.py                 # Tax: slabs + ITR foreign-asset JSON
  cleaning.py            # DataCleaner
  eks.py                 # EksHelper (legacy ELK push)
  gather_data.py         # back-compat facade re-exporting the above
dashboard/
  backend/               # Flask API (entrypoint dir; on sys.path when server.py runs)
    server.py            # thin entrypoint: sys.path + create_app() + start_recorder()
    app.py               # application factory (registers blueprints) + recorder startup
    db.py                # get_db() context manager + TABLE_COLUMNS + DB_PATH
    routes/              # one Blueprint per API area
      tables.py          #   /api/tables*
      market.py          #   /api/market-status
      live_price.py      #   /api/live-price, /api/live-price-history (GET/POST/DELETE)
      portfolio.py       #   /api/dashboard, /api/holdings, /api/sold, /api/mark-sold*
      tax.py             #   /api/tax-config, /api/generate-tax-doc, /api/export-fa-a3
      breeze.py          #   /api/breeze/*
      news.py            #   /api/news (NVIDIA news + sentiment)
    services/            # business logic (no Flask request handling)
      market.py          #   market hours, USD/INR helpers, OHLC rollups, recorder loop
      portfolio.py       #   dashboard/holdings/sold builders, mark-sold undo stack
      tax_export.py      #   foreign-asset (Schedule FA) row builder + paths
      breeze.py          #   ICICI Breeze helpers
      news.py            #   Finnhub company-news fetch + VADER sentiment (cached)
    breeze_icici.py      # Breeze SDK wrapper (optional)
    serve-prod.js        # static file server + /api reverse proxy (zero deps)
  src/app/
    core/
      models/            # TypeScript response/types
      services/          # dashboard.service.ts (HTTP)
      utils/             # format.util.ts (pure currency/date/CSV formatters)
    features/dashboard/
      dashboard.component.*   # shell + per-tab content (routed by :tab)
      data-tab/              # Data tab (iframe) standalone component
      news-tab/              # News tab (NVIDIA news + sentiment) standalone component
      financial-planning/    # Financial planning tab standalone component
      overview-time-card/, stat-card/, live-price-extended-hint  # presentational components
legacy/                  # retired CLI / old web app (not used by pm2) - see legacy/README.md
```

## Data + price history

- Raw live ticks (every 14s when market open) -> `live_price_history` (capped at 5 GB; oldest pruned).
- Continuous OHLC rollups -> `live_price_ohlc_1m` / `_1h` / `_1d`, maintained incrementally and backfilled from raw on first run.
- The chart endpoint (`/api/live-price-history`) aggregates the smallest suitable rollup into <= `maxPoints` buckets so any range stays fast.

## Config / secrets

All secrets and external endpoints live in [`helpers/config.py`](helpers/config.py) with env overrides
(set in `dashboard/.env`; see `dashboard/.env.example`). Defaults preserve prior behavior.

## Docker

Two containers via [`docker-compose.yml`](docker-compose.yml):

- `backend` ([`Dockerfile.backend`](Dockerfile.backend)) - Flask API on 8080. Installs `en_IN`/`en_US`
  locales (required by `print_rupees`). The host `./configs` (SQLite DB, tax config, FA template)
  is bind-mounted so data persists and the recorder keeps the same DB.
- `frontend` ([`dashboard/Dockerfile`](dashboard/Dockerfile)) - multi-stage Angular build, served by
  `serve-prod.js` on 4201 with `API_TARGET=http://backend:8080` (proxies `/api`).

```bash
docker compose up -d --build      # build + run
docker compose logs -f            # logs
docker compose down               # stop
```

Secrets are optional (config.py has defaults); override via a `.env` next to the compose file
(`FINNHUB_TOKEN`, `FMV_API_KEY`, `BREEZE_API_KEY`, ...). Note: pm2 and Docker are alternative run
methods - don't run both against the same ports/DB simultaneously. If a backend pip wheel fails to
build on this arch, add build tools to `Dockerfile.backend` (`apt-get install -y build-essential`).

## Notes / follow-ups

The Angular `dashboard.component.ts` is still large. Shared formatters were extracted to
`core/utils/format.util.ts` and the Data tab to its own component. Splitting the remaining
entangled tabs (Holdings/Sold/Playground/Overview), which share live-price state and change
detection, is best done incrementally with UI QA.
