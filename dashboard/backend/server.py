"""
Backend entrypoint for the Angular dashboard (nvShares.db).
Run: python3 dashboard/backend/server.py

Thin launcher: wires sys.path, builds the app via the factory (app.py), and starts
the background recorder. Routes live in routes/*, business logic in services/*,
DB access in db.py, and secrets/paths in helpers/config.py.
"""
import os
import sys

_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(_BACKEND_DIR))
# REPO_ROOT must be sys.path[0]: helpers.DB derives the db path from sys.path[0].
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
# The backend dir is the script dir (already importable); ensure it stays on the path
# so `from db import ...`, `from services...`, `from routes...` resolve.
if _BACKEND_DIR not in sys.path:
    sys.path.append(_BACKEND_DIR)

from helpers import config  # noqa: E402  (after sys.path setup)
from app import create_app, start_recorder  # noqa: E402

app = create_app()
PORT = config.BACKEND_PORT

if __name__ == "__main__":
    if not os.path.isfile(config.DB_PATH):
        sys.exit(f"Database not found: {config.DB_PATH}")
    start_recorder()
    print(f"Backend API at http://127.0.0.1:{PORT} (configs at {REPO_ROOT})")
    print("Live price recorder: running in background (records every 14s when market open)")
    # use_reloader=False: pm2 manages restarts; the reloader would spawn duplicate recorder threads.
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
