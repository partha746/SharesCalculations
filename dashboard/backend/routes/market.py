"""/api/market-status blueprint."""
import requests
from flask import Blueprint, jsonify

from services.market import (
    _FINNHUB_TOKEN, _is_nasdaq_open_et, _is_premarket_et,
    _is_postmarket_et, _next_market_open_close_et,
)

bp = Blueprint("market", __name__)

@bp.route("/api/market-status", methods=["GET"])
def get_market_status():
    """Return whether US market is open. Tries Finnhub first; falls back to ET time window."""
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/market-status",
            params={"exchange": "US", "token": _FINNHUB_TOKEN},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                # Finnhub returns isOpen + session (see /stock/market-status); older clients used marketOpen/status.
                if "isOpen" in data:
                    market_open = bool(data["isOpen"])
                elif data.get("exchange") and "status" in data:
                    status = str(data.get("status", "")).lower()
                    market_open = status == "open"
                elif "marketOpen" in data:
                    market_open = bool(data["marketOpen"])
                elif data.get("session") in ("pre-market", "regular", "post-market"):
                    market_open = True
                else:
                    market_open = _is_nasdaq_open_et()
            else:
                market_open = _is_nasdaq_open_et()
        else:
            market_open = _is_nasdaq_open_et()
    except Exception:
        market_open = _is_nasdaq_open_et()
    next_open_ts, next_close_ts, next_pre_ts = _next_market_open_close_et()
    return jsonify({
        "marketOpen": market_open,
        "isPreMarketSession": _is_premarket_et(),
        "isPostMarketSession": _is_postmarket_et(),
        "nextOpen": next_open_ts * 1000,
        "nextClose": next_close_ts * 1000,
        "nextPreMarketStart": next_pre_ts * 1000,
    })

