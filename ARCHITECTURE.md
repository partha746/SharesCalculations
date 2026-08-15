# Architecture

Internals of the NVDA shares tracking dashboard. For setup, configuration and usage, see
[`README.md`](README.md).

Runs as two Docker containers (primary; see [`docker-compose.yml`](docker-compose.yml)):

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
  eks.py                 # EksHelper (legacy ELK push; needs `pip install elasticsearch pytz`)
  gather_data.py         # back-compat facade re-exporting the above (EksHelper resolved lazily)
dashboard/
  serve-prod.js          # static file server + /api reverse proxy (zero deps, optional TLS)
  backend/               # Flask API (entrypoint dir; on sys.path when server.py runs)
    server.py            # thin entrypoint: sys.path + create_app() + start_recorder()
    app.py               # application factory (registers blueprints) + recorder startup
    db.py                # get_db() context manager + TABLE_COLUMNS + DB_PATH
    routes/              # one Blueprint per API area (12 total, all registered without url_prefix)
      tables.py          #   /api/tables*
      market.py          #   /api/market-status
      live_price.py      #   /api/live-price, /api/live-price-history (GET/POST/DELETE)
      portfolio.py       #   /api/dashboard, /api/holdings, /api/sold, /api/mark-sold*
      tax.py             #   /api/tax-config, /api/generate-tax-doc, /api/export-fa-a3
      breeze.py          #   /api/breeze/* (accounts, OAuth callback, holdings, positions)
      news.py            #   /api/news (NVIDIA news + sentiment)
      mf.py              #   /api/mf/* (AMFI scheme search, holdings CRUD, CSV import)
      networth.py        #   /api/networth/items (CRUD)
      income.py          #   /api/income/resolve, /api/income/overrides/<key>
      earmarks.py        #   /api/earmarks (lots reserved at a target sell price)
      finance_plan.py    #   /api/finance-plan (GET/POST; plan stored as a JSON blob)
    services/            # business logic (no Flask request handling)
      market.py          #   market hours, USD/INR helpers, OHLC rollups, recorder loop
      portfolio.py       #   dashboard/holdings/sold builders, mark-sold undo stack
      tax_export.py      #   foreign-asset (Schedule FA) row builder + paths
      breeze.py          #   ICICI Breeze helpers
      news.py            #   Finnhub company-news fetch + VADER sentiment (cached)
      mf.py              #   AMFI/mfapi.in NAV lookup + scheme-name resolution
      networth.py        #   networth_items CRUD
      income.py          #   dividend/REIT payout resolution (yfinance) + ICICI->NSE symbol map
      earmarks.py        #   earmarks CRUD
      finance_plan.py    #   finance_plan persistence
    breeze_icici.py      # Breeze SDK wrapper (optional)
  src/app/
    app.routes.ts        # '' -> /holdings, ':tab' -> DashboardComponent (lazy), '**' -> /holdings
    core/
      models/            # dashboard.types.ts (TypeScript response/types)
      services/          # dashboard.service.ts (HTTP)
      utils/             # format.util.ts (pure currency/date/CSV formatters)
      directives/        # indian-number.directive.ts (lakh/crore grouping in text inputs)
    features/dashboard/
      dashboard.component.*   # shell + per-tab content (routed by :tab)
      countdown-utils.ts     # market open/close countdown helpers
      data-tab/              # Data tab (iframe) standalone component
      news-tab/              # News tab (NVIDIA news + sentiment) standalone component
      financial-planning/    # Financial planning tab standalone component
      networth-tab/          # Net worth tab (tracked + manual assets, expected income)
      mf-tab/                # Mutual-fund holdings management (surfaced inside the ICICI tab)
      stat-card/, overview-time-card, live-price-extended-hint  # presentational components
legacy/                  # retired CLI / old web app (not used by pm2) - see legacy/README.md
```

## Data + price history

Single SQLite file (`configs/nvShares.db` by default, `NVSHARES_DB_PATH` to override). Both
`dashboard/backend/db.py:get_db()` and `helpers/db.py:DB` resolve through `helpers/config.py:DB_PATH`,
so every reader and writer shares one path.

Price history:

- Raw live ticks (every 14s when market open) -> `live_price_history` (capped at 5 GB; oldest pruned).
  The backend recorder is the only writer; the frontend does not POST ticks.
- Continuous OHLC rollups -> `live_price_ohlc_1m` / `_1h` / `_1d`, maintained incrementally and backfilled from raw on first run.
- The chart endpoint (`/api/live-price-history`) aggregates the smallest suitable rollup into <= `maxPoints` buckets so any range stays fast.

Other tables: `NSU` / `ESPP` / `SellOut` / `Split` (lots and sales), `mf_holdings`,
`networth_items`, `earmarks`, `finance_plan`, `income_overrides`, `breeze_accounts` and
`breeze_account_names` (so holder names survive restarts and disconnects).

## Frontend build

Angular 21 built by `@angular/build` (esbuild/vite), not the older webpack-based
`@angular-devkit/build-angular`. That package is deliberately absent: the app never needed webpack,
and dropping it removed a large transitive subtree (863 -> 494 packages) that carried most of the
dependency advisories.

`angular.json` sets `outputPath.browser` to `""` so the bundle lands flat in `dist/dashboard`, which
is where [`serve-prod.js`](dashboard/serve-prod.js) and the Dockerfile expect `index.html`. The
`application` builder would otherwise nest it under `dist/dashboard/browser`.

Requires Node `^20.19` / `^22.12` / `>=24` and TypeScript `>=5.9 <6.0`. Moving to Angular 22 would
additionally require Node 22.22+ and TypeScript 6.

## Config / secrets

All secrets and external endpoints live in [`helpers/config.py`](helpers/config.py) with env overrides
(set in `dashboard/.env`; see `dashboard/.env.example`). Defaults preserve prior behavior.

## Docker

Two containers via [`docker-compose.yml`](docker-compose.yml):

- `backend` ([`Dockerfile.backend`](Dockerfile.backend)) - Flask API on 8080. Installs `en_IN`/`en_US`
  locales (required by `print_rupees`). The host `./configs` (SQLite DB, tax config, FA template)
  is bind-mounted so data persists and the recorder keeps the same DB.
- `frontend` ([`dashboard/Dockerfile`](dashboard/Dockerfile)) - multi-stage Angular build on
  `node:22-alpine`, served by `serve-prod.js` on 4201 with `API_TARGET=http://backend:8080`
  (proxies `/api`). BuildKit cache mounts persist the npm and Angular caches across builds.
  `serve-prod.js` forwards the browser's `Host` plus `X-Forwarded-*`, and Flask is wrapped in
  `ProxyFix`, so absolute URLs (e.g. the ICICI OAuth callback) use the origin the browser used.

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

The Angular `dashboard.component.ts` is still large (~180 KB). Shared formatters were extracted to
`core/utils/format.util.ts`, and Data/News/Financial planning/Net worth/MF now have their own
components. Splitting the remaining entangled tabs (Holdings/Sold/Playground/Overview), which share
live-price state and change detection, is best done incrementally with UI QA.

`dashboard.component.scss` exceeds the 40 KB `anyComponentStyle` warning budget (errors at 60 KB).
Extracting per-tab styles alongside the component split would clear it.

`helpers/eks.py` contains a hardcoded Elasticsearch API key. The repository is public, so that
credential should be rotated and moved into `helpers/config.py` as an env override.
