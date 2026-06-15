# Architecture

NVDA shares tracking dashboard. Runs under pm2 (see [`ecosystem.config.js`](ecosystem.config.js)):

- `shares-backend` -> [`dashboard/backend/server.py`](dashboard/backend/server.py) (Flask API, port 8080)
- `shares-dashboard` -> [`dashboard/serve-prod.js`](dashboard/serve-prod.js) (serves the built Angular app on port 4201, proxies `/api` to the backend)

Deploy after changes (from `dashboard/`):

- `npm run deploy` - build + restart the dashboard
- `npm run deploy:all` - build + restart dashboard and backend

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
    services/            # business logic (no Flask request handling)
      market.py          #   market hours, USD/INR helpers, OHLC rollups, recorder loop
      portfolio.py       #   dashboard/holdings/sold builders, mark-sold undo stack
      tax_export.py      #   foreign-asset (Schedule FA) row builder + paths
      breeze.py          #   ICICI Breeze helpers
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

## Notes / follow-ups

The Angular `dashboard.component.ts` is still large. Shared formatters were extracted to
`core/utils/format.util.ts` and the Data tab to its own component. Splitting the remaining
entangled tabs (Holdings/Sold/Playground/Overview), which share live-price state and change
detection, is best done incrementally with UI QA.
