"""Daily NVDA closes from Twelve Data, for the Playground's year-by-year chart."""
import threading
import time
from datetime import datetime, timezone

import requests

from helpers import config

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

_TIME_SERIES_URL = "https://api.twelvedata.com/time_series"
# The whole range is a single request, but the latest bar keeps moving while the market is
# open, so a copy is only reused for this long.
_TTL_SEC = 15 * 60
_cache = {}
# Simultaneous page loads wait for one upstream request instead of each making their own.
_lock = threading.Lock()


def _et_year():
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/New_York")).year
    return datetime.now(timezone.utc).year


def daily_closes(symbol="NVDA", years=5):
    """Split-adjusted daily closes covering the last `years` calendar years, oldest first.

    The series starts in the December before `firstYear`, so the first year's change can be
    measured from the previous year's last close like every other year's. When a refresh
    fails the previous copy is returned with `stale` set; with nothing cached it raises.
    """
    symbol = symbol.upper()
    first_year = _et_year() - years + 1
    key = (symbol, first_year)
    with _lock:
        cached = _cache.get(key)
        if cached and time.time() - cached["fetchedAt"] / 1000 < _TTL_SEC:
            return cached
        try:
            dates, closes = _fetch(symbol, f"{first_year - 1}-12-01")
        except Exception as e:
            if not cached:
                raise
            print(f"[price-history] refresh failed, serving the copy from {cached['fetchedAt']}: {e}", flush=True)
            return {**cached, "stale": True}
        entry = {
            "symbol": symbol,
            "firstYear": first_year,
            "dates": dates,
            "closes": closes,
            "fetchedAt": int(time.time() * 1000),
            "stale": False,
        }
        _cache[key] = entry
        return entry


def _fetch(symbol, start_date):
    try:
        r = requests.get(
            _TIME_SERIES_URL,
            params={"symbol": symbol, "interval": "1day", "start_date": start_date, "outputsize": 5000},
            # Not a query parameter: requests repeats the full URL in its exception messages.
            headers={"Authorization": f"apikey {config.FMV_API_KEY}"},
            timeout=20,
        )
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        raise RuntimeError(f"Twelve Data did not answer ({type(e).__name__})") from None
    # Errors arrive as {"status": "error", "code": ..., "message": ...}, sometimes with HTTP 200.
    if not isinstance(data, dict) or data.get("status") != "ok" or not data.get("values"):
        detail = data.get("message") if isinstance(data, dict) else None
        raise RuntimeError(detail or f"Twelve Data returned HTTP {r.status_code}")
    rows = sorted((v["datetime"][:10], round(float(v["close"]), 4)) for v in data["values"])
    return [d for d, _ in rows], [c for _, c in rows]
