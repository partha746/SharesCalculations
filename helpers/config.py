"""Centralized configuration and secrets, overridable via environment variables.

Shared by the Flask backend (dashboard/backend) and the data layer (helpers/*).
Values fall back to the previously hardcoded defaults so behavior is unchanged
when no environment is provided. Set overrides in dashboard/.env (git-ignored).
"""
import os

_HELPERS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(_HELPERS_DIR)

# Load dashboard/.env once if python-dotenv is available (no-op if missing).
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(REPO_ROOT, "dashboard", ".env"))
except Exception:
    pass


def _env(key, default):
    v = os.environ.get(key)
    return v if v not in (None, "") else default


# --- Secrets / API tokens ---
FINNHUB_TOKEN = _env("FINNHUB_TOKEN", "cvsfdk9r01qhup0qfks0cvsfdk9r01qhup0qfksg")
# Key used by OwnStockData.fetch_max_high_and_closing (financial data provider).
FMV_API_KEY = _env("FMV_API_KEY", "db623c532e3e4568aad28629c00b574e")

# --- External data sources (USD->INR + stock) ---
FRANKFURTER_BASE = _env("FRANKFURTER_BASE", "https://api.frankfurter.app")
FRANKFURTER_LATEST_URL = _env("FRANKFURTER_LATEST_URL", "https://api.frankfurter.dev/v1/latest")
ER_API_URL = _env("ER_API_URL", "https://open.er-api.com/v6/latest/USD")

# --- Elasticsearch (legacy ELK push via EksHelper) ---
ELK_HOST = _env("ELK_HOST", "192.168.1.235")
ELK_PORT = _env("ELK_PORT", "9200")

# --- Database & backend server ---
DB_PATH = _env("NVSHARES_DB_PATH", os.path.join(REPO_ROOT, "configs", "nvShares.db"))
BACKEND_PORT = int(_env("PORT", "8080"))
