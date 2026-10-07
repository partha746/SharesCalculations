"""/api/market-status + /api/extended-session blueprint."""
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Blueprint, jsonify

from services.market import (
    _FINNHUB_TOKEN, _is_nasdaq_open_et, _is_premarket_et,
    _is_postmarket_et, _next_market_open_close_et, recorded_session_open,
)

bp = Blueprint("market", __name__)

# {"state", "at", "stats"} for Nasdaq's extended-trading tables. They are static outside the
# pre/post sessions; during one, the TTL stays below the dashboard's 14s poll.
_EXTENDED_STATS_CACHE = {}
_EXTENDED_STATS_TTL_IN_SESSION_SEC = 10
_EXTENDED_STATS_TTL_IDLE_SEC = 120


def _extended_session_stats():
    """{'pre': stats or None, 'post': stats or None} from Nasdaq."""
    from helpers import gather_data

    state = (_is_premarket_et(), _is_postmarket_et(), _is_nasdaq_open_et())
    ttl = _EXTENDED_STATS_TTL_IN_SESSION_SEC if state[0] or state[1] else _EXTENDED_STATS_TTL_IDLE_SEC
    cached = _EXTENDED_STATS_CACHE.get("entry")
    if cached and cached["state"] == state and time.monotonic() - cached["at"] < ttl:
        return cached["stats"]
    rupee_conv_obj = gather_data.RupeeConv()
    # Nasdaq takes 1-3s per request whether or not the connection is reused, so all three go at
    # once. The post figures are measured against the quote's regular close; requesting the quote
    # alongside means extended_session_stats('post') finds it in the quote cache.
    with ThreadPoolExecutor(max_workers=3) as pool:
        pool.submit(rupee_conv_obj.nasdaq_quote, "NVDA")
        futures = {s: pool.submit(rupee_conv_obj.extended_session_stats, s) for s in ("pre", "post")}
        stats = {s: f.result() for s, f in futures.items()}
    # None can also mean Nasdaq failed; caching it would blank the card until the entry expired.
    if all(stats.values()):
        _EXTENDED_STATS_CACHE["entry"] = {"state": state, "at": time.monotonic(), "stats": stats}
    return stats

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
    next_open_ts, next_close_ts, next_pre_ts, next_post_end_ts = _next_market_open_close_et()
    return jsonify({
        "marketOpen": market_open,
        "isPreMarketSession": _is_premarket_et(),
        "isPostMarketSession": _is_postmarket_et(),
        "nextOpen": next_open_ts * 1000,
        "nextClose": next_close_ts * 1000,
        "nextPreMarketStart": next_pre_ts * 1000,
        # Post-market runs from the close to 8 PM ET, so nextClose is also its start.
        "nextPostMarketEnd": next_post_end_ts * 1000,
    })


@bp.route("/api/extended-session", methods=["GET"])
def get_extended_session():
    """Pre- and post-market open/high/low/last/volume for NVDA.

    High, low, last and volume come from Nasdaq's extended-trading feed. The open is the
    first tick this app recorded for the session, because Nasdaq does not publish it — so
    it is absent until the session has started with the recorder running, and is flagged
    `openIsRecorded` to keep it distinguishable from an official figure.
    """
    stats_by_session = _extended_session_stats()
    sessions = {}
    for session in ("pre", "post"):
        stats = stats_by_session.get(session) or {}
        recorded = recorded_session_open(session) or {}
        if not stats and not recorded:
            sessions[session] = None
            continue
        sessions[session] = {
            "session": session,
            "open": recorded.get("open"),
            "openAtMs": recorded.get("openAtMs"),
            "openIsRecorded": recorded.get("open") is not None,
            "recordedTicks": recorded.get("ticks", 0),
            "last": stats.get("last"),
            "change": stats.get("change"),
            "changePct": stats.get("changePct"),
            "high": stats.get("high"),
            "highAt": stats.get("highAt"),
            "low": stats.get("low"),
            "lowAt": stats.get("lowAt"),
            "volume": stats.get("volume"),
            "prevClose": stats.get("prevClose"),
            "asOf": stats.get("asOf"),
            "asOfMs": stats.get("asOfMs"),
            "asOfDate": stats.get("asOfDate"),
        }
    return jsonify({
        "isPreMarketSession": _is_premarket_et(),
        "isPostMarketSession": _is_postmarket_et(),
        "marketOpen": _is_nasdaq_open_et(),
        "pre": sessions.get("pre"),
        "post": sessions.get("post"),
    })

