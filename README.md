# SharesCalculations

A self-hosted dashboard for tracking a NVIDIA (NVDA) equity-compensation portfolio from an Indian
tax perspective, alongside Indian brokerage holdings and overall net worth.

It answers the questions an Indian resident holding US RSUs/ESPP actually has to deal with: what are
my lots worth in rupees today, at which USD/INR rate did each one vest, what tax will I owe if I sell
now, and what goes into Schedule FA when filing. On top of that it aggregates ICICI Direct equity and
mutual fund holdings so the whole picture lives in one place.

Built for a home network — a Flask API and an Angular single-page app, run as two Docker containers,
storing everything in a local SQLite file. There is no multi-user support, no authentication, and no
cloud component by design.

## What it tracks

The UI is a set of tabs, each addressable directly as `/<slug>`:

| Tab | Slug | What it does |
| --- | --- | --- |
| Holdings | `holdings` | NSU/RSU and ESPP lots with per-lot buy rate, live value, gain, and tax owed. Mark lots sold (with undo), earmark lots for a target sell price, export CSV or Schedule FA. |
| Sold Shares | `sold` | Realised positions with per-lot buy/sell FX rates and capital gains, filterable by Indian financial year. |
| Playground | `playground` | Model proceeds across multiple hypothetical prices, including unvested-share simulation with tax deduction. |
| Net worth | `networth` | Assets and liabilities by category and liquidity. NVDA, mutual funds and ICICI equity auto-populate; everything else is manual. Also projects expected monthly income from dividends, REITs and InvITs. |
| Financial planning | `financial` | Long-range cash-flow projection with per-row annual growth on recurring expenses. Saved server-side. |
| News | `news` | NVIDIA company news from Finnhub with VADER sentiment scoring. |
| ICICI Direct | `icici` | Connect one or more ICICI Direct accounts via the Breeze API; combined equity and mutual fund holdings with search, sort and grouping. |
| Tax Config | `tax` | Per-financial-year tax slabs, and Schedule FA (A3) export into a ClearTax template. |
| Data | `data` | Direct browse and edit of the underlying SQLite tables. |

Prices come from Finnhub, USD/INR from Frankfurter (with an ExchangeRate-API fallback), mutual fund
NAVs from AMFI via `mfapi.in`, and dividend history from Yahoo Finance.

## Quick start

Requires Docker with Compose. From the repo root:

```bash
./start.sh
```

That builds and starts both containers and prints the URLs. Equivalent to:

```bash
docker compose up -d --build
```

The dashboard is on port **4201** and the API on **8080**. If `certs/cert.pem` and `certs/key.pem`
exist, the frontend serves HTTPS and you'll reach it at `https://<host>:4201` — browsers will warn
about the self-signed certificate, which is expected. Delete or unset the `TLS_*` variables in
`docker-compose.yml` to fall back to plain HTTP.

Day-to-day commands:

```bash
docker compose ps          # status
docker compose logs -f     # follow logs
docker compose down        # stop
```

Rebuild after code changes with `docker compose up -d --build`. `dashboard/package.json` also exposes
`npm run docker:deploy` to rebuild only the frontend, which is faster when you've only touched the UI.

## Configuration

Every secret and endpoint has a working default in [`helpers/config.py`](helpers/config.py), so the
app runs with no configuration at all. To override anything, copy the example file and edit it:

```bash
cp dashboard/.env.example dashboard/.env
```

`dashboard/.env` is read by both the containers and local runs. The keys that matter most:

| Variable | Purpose |
| --- | --- |
| `FINNHUB_TOKEN` | Live NVDA price and company news |
| `FMV_API_KEY` | Historical fair-market-value lookups |
| `NVSHARES_DB_PATH` | Move the SQLite database off the default `configs/nvShares.db` |
| `PORT` | Backend port (default `8080`) |
| `BREEZE_API_KEY`, `BREEZE_API_SECRET` | ICICI Direct Breeze API credentials |
| `BREEZE_PUBLIC_BASE_URL` | Public origin for the ICICI OAuth callback — use the **frontend** origin, not the API port |

For ICICI Direct, register the redirect URL in your Breeze app as
`${BREEZE_PUBLIC_BASE_URL}/api/breeze/callback`. Pointing it at port 8080 completes the login but
lands the browser on the API, which doesn't serve the UI.

## Local development

Without Docker you need Python 3.11+ and a Node version Angular 21 accepts — 20.19+, 22.12+, or 24+:

```bash
pip install -r requirements.txt
cd dashboard && npm ci
npm run start:all          # Flask API + Angular dev server together
```

The dev server runs on 4201 and proxies `/api` to the backend via `proxy.conf.json`. Other useful
scripts: `npm run build` for a production bundle, `npm run watch` for incremental rebuilds, and
`npm run serve:prod` to serve an existing build through the same static server Docker uses.

PM2 remains as an alternative runner ([`ecosystem.config.js`](ecosystem.config.js)), but don't run it
alongside Docker — they'd contend for the same ports and database file.

## Layout

```
helpers/            Shared data layer: FX/price fetching, SQLite access, tax slabs, dataframe builders
dashboard/backend/  Flask API — app factory, one Blueprint per area under routes/, logic in services/
dashboard/src/app/  Angular app — core/ for models and services, features/dashboard/ for the tabs
configs/            SQLite database, tax config, and export templates (bind-mounted into the backend)
legacy/             Retired CLI and earlier web app, kept for reference
```

[`ARCHITECTURE.md`](ARCHITECTURE.md) covers the internals in detail — module responsibilities, the
price-history rollup design, and the reasoning behind the current structure.

## API

All endpoints live under `/api` and are reachable through the frontend's proxy. Grouped by area:

| Area | Endpoints |
| --- | --- |
| Portfolio | `/api/dashboard`, `/api/holdings`, `/api/sold`, `/api/mark-sold`, `/api/mark-sold-undo`, `/api/mark-sold-can-undo` |
| Prices | `/api/live-price`, `/api/live-price-history`, `/api/market-status` |
| Tax | `/api/tax-config`, `/api/generate-tax-doc`, `/api/export-fa-a3` |
| ICICI Breeze | `/api/breeze/status`, `/api/breeze/accounts`, `/api/breeze/callback`, `/api/breeze/portfolio-holdings/<acct>`, `/api/breeze/demat-holdings/<acct>`, `/api/breeze/mf-holdings/<acct>`, `/api/breeze/disconnect/<acct>` |
| Mutual funds | `/api/mf/search`, `/api/mf/holdings`, `/api/mf/import` |
| Net worth | `/api/networth/items` |
| Income | `/api/income/resolve`, `/api/income/overrides/<key>` |
| Planning | `/api/finance-plan`, `/api/earmarks` |
| Tables | `/api/tables`, `/api/tables/<table_name>` |

## Data and storage

Everything persists in one SQLite file, `configs/nvShares.db`, bind-mounted from the host so it
survives container rebuilds.

While the US market is open, a background recorder in the backend samples the live price every 14
seconds into `live_price_history`. That raw table is capped at 5 GB with the oldest rows pruned, and
is continuously rolled up into 1-minute, 1-hour and 1-day OHLC tables. Chart requests are served from
the smallest rollup that satisfies the range and downsampled server-side, which keeps a two-week
chart as responsive as a one-day one.

Because the database holds real financial history, back it up before experimenting — the Data tab
writes straight to the live tables.

## Troubleshooting

**ICICI Direct shows "not configured".** The backend didn't see your Breeze credentials. Confirm
`dashboard/.env` exists with `BREEZE_API_KEY` and `BREEZE_API_SECRET`, then rebuild so the container
picks it up.

**Login redirects to a page that won't load.** `BREEZE_PUBLIC_BASE_URL` is pointing at the API port.
Set it to the origin you actually open in the browser.

**Mutual fund holdings return "facility not enabled".** An entitlement limitation on ICICI's side,
not a bug here. NAV still resolves through AMFI, so values remain correct.

**External API calls fail inside Docker.** The backend service pins public DNS resolvers in
`docker-compose.yml`, because this host's `systemd-resolved` stub exposes no upstream file the
container's resolver can read. Remove those only if your host resolves DNS for containers normally.

## Notes

This is a personal project shaped around one specific situation: NVDA equity compensation held by an
Indian resident. Tax slabs, the 37.14% deduction on unvested shares, financial-year boundaries and
the Schedule FA output all encode Indian rules and one person's circumstances. Treat the tax figures
as bookkeeping aids, not filing advice.
