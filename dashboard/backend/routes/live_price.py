"""/api/live-price + /api/live-price-history blueprint."""
import time
from datetime import datetime

from flask import Blueprint, jsonify, request

from db import get_db
from services.market import (
    _AGG_BUCKETS_MS, _ensure_live_price_history_table, _update_rollups,
    _is_premarket_et, _is_postmarket_et, _log_premarket_miss_once,
)

bp = Blueprint("live_price", __name__)

@bp.route("/api/live-price-history", methods=["GET"])
def get_live_price_history():
    """Server-aggregated OHLC history for charts. Params: days (1..370), maxPoints (100..5000).
    Picks a bucket size so the response stays <= maxPoints, sourced from the smallest suitable rollup table.
    Returns ascending points: {timestamp, open, high, low, livePriceUsd(=close), usdToInrRate(=close), avg, n}.
    """
    days = min(370, max(1, int(request.args.get("days", 7))))
    max_points = min(5000, max(100, int(request.args.get("maxPoints", 1500))))
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 86400000
    span_ms = max(days * 86400000, 1)
    target = span_ms / max_points
    bucket = next((b for b in _AGG_BUCKETS_MS if b >= target), _AGG_BUCKETS_MS[-1])

    with get_db() as conn:
        _ensure_live_price_history_table(conn)
        if bucket >= 86400000:
            rows = conn.execute(
                "SELECT bucket_ms, open, high, low, close, sum_price, n, rate_close FROM live_price_ohlc_1d WHERE bucket_ms >= ? ORDER BY bucket_ms ASC",
                (start_ms,),
            ).fetchall()
            src = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in rows]
        else:
            table = "live_price_ohlc_1h" if bucket >= 3600000 else "live_price_ohlc_1m"
            rows = conn.execute(
                f"SELECT bucket_ms, open, high, low, close, sum_price, n, rate_close FROM {table} WHERE bucket_ms >= ? ORDER BY bucket_ms ASC",
                (start_ms,),
            ).fetchall()
            src = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in rows]

    # Re-bucket the source rollup rows (already time-ordered) into the target bucket size.
    agg = {}
    order = []
    for (bm, o, h, l, c, sp, n, rc) in src:
        k = (bm // bucket) * bucket if bucket < 86400000 else bm
        e = agg.get(k)
        if e is None:
            agg[k] = [o, h, l, c, sp, n, rc, bm]
            order.append(k)
        else:
            if h > e[1]:
                e[1] = h
            if l < e[2]:
                e[2] = l
            e[4] += sp
            e[5] += n
            if bm >= e[7]:
                e[3] = c
                e[6] = rc
                e[7] = bm

    out = []
    for k in order:
        e = agg[k]
        avg = e[4] / e[5] if e[5] else e[3]
        out.append({
            "timestamp": k,
            "open": round(e[0], 2),
            "high": round(e[1], 2),
            "low": round(e[2], 2),
            "livePriceUsd": round(e[3], 2),
            "avg": round(avg, 2),
            "usdToInrRate": round(e[6], 2),
            "n": e[5],
        })
    return jsonify(out)


@bp.route("/api/live-price-history", methods=["DELETE"])
def clear_live_price_history():
    """Delete all rows from live_price_history (graph data). Returns count of deleted rows."""
    with get_db() as conn:
        _ensure_live_price_history_table(conn)
        cur = conn.execute("DELETE FROM live_price_history")
        deleted = cur.rowcount
        for tbl in ("live_price_ohlc_1m", "live_price_ohlc_1h", "live_price_ohlc_1d"):
            conn.execute(f"DELETE FROM {tbl}")
    return jsonify({"ok": True, "deleted": deleted})


@bp.route("/api/live-price-history", methods=["POST"])
def append_live_price_history():
    """Append one live price point (from frontend poll). Body: { timestamp, livePriceUsd, usdToInrRate }."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON body required"}), 400
    ts = data.get("timestamp")
    price = data.get("livePriceUsd")
    rate = data.get("usdToInrRate")
    if ts is None or not isinstance(price, (int, float)) or not isinstance(rate, (int, float)):
        return jsonify({"error": "timestamp, livePriceUsd, usdToInrRate required"}), 400
    ts_ms = int(ts)
    with get_db() as conn:
        _ensure_live_price_history_table(conn)
        conn.execute(
            "INSERT INTO live_price_history (timestamp_ms, live_price_usd, usd_to_inr_rate) VALUES (?, ?, ?)",
            (ts_ms, float(price), float(rate)),
        )
        _update_rollups(conn, ts_ms, float(price), float(rate))
    return jsonify({"ok": True}), 201


@bp.route("/api/live-price", methods=["GET"])
def get_live_price():
    """Lightweight endpoint: NVDA price + USD/INR rate. When in pre-market (4–9:30 AM ET), includes preMarketPriceUsd from yfinance."""
    try:
        from helpers import gather_data

        rupee_conv_obj = gather_data.RupeeConv()
        live_price, todays_rp, open_price, prev_close = rupee_conv_obj.get_live_price()
        if live_price is None or todays_rp is None:
            return jsonify({"error": "Could not fetch live price or USD/INR rate"}), 503
        payload = {
            "livePriceUsd": round(live_price, 2),
            "usdToInrRate": round(float(todays_rp), 4),
            "lastUpdated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        }
        if open_price is not None:
            payload["openPriceUsd"] = round(open_price, 2)
        if prev_close is not None:
            payload["previousCloseUsd"] = round(prev_close, 2)
        if _is_premarket_et():
            premarket = rupee_conv_obj.get_premarket_price("NVDA")
            if premarket is not None:
                payload["preMarketPriceUsd"] = round(premarket, 2)
            else:
                _log_premarket_miss_once()
        if _is_postmarket_et():
            postmarket = rupee_conv_obj.get_postmarket_price("NVDA")
            if postmarket is not None:
                payload["postMarketPriceUsd"] = round(postmarket, 2)
        resp = jsonify(payload)
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 503

