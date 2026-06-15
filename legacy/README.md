# Legacy (retired)

These files are no longer part of the running application. The live system runs via
pm2 using [`../ecosystem.config.js`](../ecosystem.config.js):

- `shares-backend` -> `dashboard/backend/server.py`
- `shares-dashboard` -> `dashboard/serve-prod.js` (serves the built Angular app + proxies `/api`)

Retired here for reference only:

- `main.py` - old CLI (`-elk` to push to Elasticsearch, `-itr` to generate ITR JSON). Superseded by the dashboard backend (`/api/generate-tax-doc`, `/api/export-fa-a3`).
- `web_app.py` + `web_static/` - the original standalone Flask "Data Manager" UI. Superseded by `dashboard/backend/server.py` (`/api/tables*`) and the embedded Data tab (`dashboard/src/assets/web-app/`).
- `windows_run.bat` - Windows launcher for the old CLI.
- `test.py`, `update_split.py` - one-off scripts.

Nothing in the active codebase imports these. Safe to delete entirely if not needed.
