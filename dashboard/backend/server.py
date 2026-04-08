"""
Flask API backend for the Angular dashboard (nvShares.db).
Run from dashboard folder: python backend/server.py
Uses repo root (parent of dashboard) for configs/ and helpers/.
"""
import os
import sys
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime

# Repo root = parent of dashboard (dashboard/backend/server.py -> backend -> dashboard -> repo)
_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
_DASHBOARD_DIR = os.path.dirname(_BACKEND_DIR)
REPO_ROOT = os.path.dirname(_DASHBOARD_DIR)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import requests

from flask import Flask, jsonify, request

app = Flask(__name__)

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # Python < 3.9
DB_PATH = os.path.join(REPO_ROOT, "configs", "nvShares.db")

TABLE_COLUMNS = {
    "NSU": ["Buy_Date", "Available_Sell", "Price_Bought", "RupeeRate"],
    "ESPP": ["Buy_Date", "Available_Sell", "Price_Bought", "RupeeRate", "TDS_Price"],
    "SellOut": [
        "Sell_Date", "Buy_Date", "Qty_Sold", "Price_Bought", "Price_Sell",
        "BuyRupeeRate", "SellRupeeRate", "Type",
    ],
    "Split": ["date", "split_ratio"],
}


@contextmanager
def get_db():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


@app.route("/api/tables", methods=["GET"])
def list_tables():
    with get_db() as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        names = [r[0] for r in cur.fetchall()]
    return jsonify(names)


@app.route("/api/tables/<table_name>", methods=["GET"])
def get_table_data(table_name):
    if table_name not in TABLE_COLUMNS:
        return jsonify({"error": f"Unknown table: {table_name}"}), 400
    with get_db() as conn:
        cur = conn.execute(f'SELECT rowid, * FROM "{table_name}"')
        rows = [dict(r) for r in cur.fetchall()]
    return jsonify(rows)


def _update_rupee_rates_for_table(table_name):
    """Run rupee rate update for the given table (NSU, ESPP, SellOut) so new/updated rows get rates."""
    from helpers import gather_data
    db_obj = gather_data.DB()
    if not os.path.isfile(db_obj.db_path):
        return
    db_obj.ensure_tables()
    rupee_conv_obj = gather_data.RupeeConv()
    if table_name == "NSU":
        rupee_conv_obj.update_null_rupees_rate("NSU", "Buy_Date", "RupeeRate")
    elif table_name == "ESPP":
        rupee_conv_obj.update_null_rupees_rate("ESPP", "Buy_Date", "RupeeRate")
    elif table_name == "SellOut":
        rupee_conv_obj.update_null_rupees_rate("SellOut", "Buy_Date", "BuyRupeeRate")
        rupee_conv_obj.update_null_rupees_rate("SellOut", "Sell_Date", "SellRupeeRate")


@app.route("/api/tables/<table_name>", methods=["POST"])
def add_row(table_name):
    if table_name not in TABLE_COLUMNS:
        return jsonify({"error": f"Unknown table: {table_name}"}), 400
    cols = TABLE_COLUMNS[table_name]
    data = request.get_json() or {}
    values = [data.get(c) if data.get(c) != "" else None for c in cols]
    placeholders = ", ".join("?" * len(cols))
    col_list = ", ".join(f'"{c}"' for c in cols)
    with get_db() as conn:
        conn.execute(f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})', values)
        rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    if table_name in ("NSU", "ESPP", "SellOut"):
        try:
            _update_rupee_rates_for_table(table_name)
        except Exception as e:
            print(f"[add_row] rupee rate update: {e}", flush=True)
    return jsonify({"rowid": rowid, "message": "Added"}), 201


@app.route("/api/tables/<table_name>/<int:rowid>", methods=["PUT"])
def update_row(table_name, rowid):
    if table_name not in TABLE_COLUMNS:
        return jsonify({"error": f"Unknown table: {table_name}"}), 400
    data = request.get_json() or {}
    cols = TABLE_COLUMNS[table_name]
    set_parts = []
    values = []
    for c in cols:
        if c in data:
            set_parts.append(f'"{c}" = ?')
            values.append(data[c] if data[c] != "" else None)
    if not set_parts:
        return jsonify({"error": "No columns to update"}), 400
    values.append(rowid)
    with get_db() as conn:
        cur = conn.execute(
            f'UPDATE "{table_name}" SET {", ".join(set_parts)} WHERE rowid = ?', values
        )
        if cur.rowcount == 0:
            return jsonify({"error": "Row not found"}), 404
    if table_name in ("NSU", "ESPP", "SellOut"):
        try:
            _update_rupee_rates_for_table(table_name)
        except Exception as e:
            print(f"[update_row] rupee rate update: {e}", flush=True)
    return jsonify({"message": "Updated"})


@app.route("/api/tables/<table_name>/<int:rowid>", methods=["DELETE"])
def delete_row(table_name, rowid):
    if table_name not in TABLE_COLUMNS:
        return jsonify({"error": f"Unknown table: {table_name}"}), 400
    with get_db() as conn:
        cur = conn.execute(f'DELETE FROM "{table_name}" WHERE rowid = ?', (rowid,))
        if cur.rowcount == 0:
            return jsonify({"error": "Row not found"}), 404
    return jsonify({"message": "Deleted"})


_DASHBOARD_CACHE = {}
_DASHBOARD_CACHE_TTL = 120

# Finnhub API key (same as in gather_data.RupeeConv.get_stock_price)
_FINNHUB_TOKEN = "cvsfdk9r01qhup0qfks0cvsfdk9r01qhup0qfksg"


def _is_nasdaq_open_et():
    """True if current time in Eastern is Mon-Fri 9:30 AM - 4:00 PM (regular session)."""
    if ZoneInfo is not None:
        et = datetime.now(ZoneInfo("America/New_York"))
    else:
        # Python < 3.9: approximate ET as UTC-5 (ignores DST)
        from datetime import timezone, timedelta
        et = datetime.now(timezone.utc) - timedelta(hours=5)
    if et.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    t = et.time()
    open_t = datetime.strptime("09:30", "%H:%M").time()
    close_t = datetime.strptime("16:00", "%H:%M").time()
    return open_t <= t < close_t


_last_premarket_miss_log = 0.0


def _log_premarket_miss_once():
    """Log once per minute when pre-market has no price (avoids log spam)."""
    global _last_premarket_miss_log
    now = time.time()
    if now - _last_premarket_miss_log >= 60:
        _last_premarket_miss_log = now
        print("[live-price] pre-market: no price from yfinance (rate limit, SSL, or no data)", flush=True)


def _is_premarket_et():
    """True if current time in Eastern is Mon-Fri 4:00 AM - 9:30 AM (pre-market session)."""
    if ZoneInfo is not None:
        et = datetime.now(ZoneInfo("America/New_York"))
    else:
        from datetime import timezone, timedelta
        et = datetime.now(timezone.utc) - timedelta(hours=5)
    if et.weekday() >= 5:
        return False
    t = et.time()
    premarket_start = datetime.strptime("04:00", "%H:%M").time()
    regular_open = datetime.strptime("09:30", "%H:%M").time()
    return premarket_start <= t < regular_open


def _is_postmarket_et():
    """True if current time in Eastern is Mon-Fri 4:00 PM - 8:00 PM (post-market session)."""
    if ZoneInfo is not None:
        et = datetime.now(ZoneInfo("America/New_York"))
    else:
        from datetime import timezone, timedelta
        et = datetime.now(timezone.utc) - timedelta(hours=5)
    if et.weekday() >= 5:
        return False
    t = et.time()
    close_t = datetime.strptime("16:00", "%H:%M").time()
    postmarket_end = datetime.strptime("20:00", "%H:%M").time()
    return close_t <= t < postmarket_end


def _seconds_until_next_market_open_et():
    """Seconds until next 9:30 AM ET (Mon-Fri). Used to sleep when market is closed."""
    if ZoneInfo is not None:
        et = datetime.now(ZoneInfo("America/New_York"))
    else:
        from datetime import timezone, timedelta
        et = datetime.now(timezone.utc) - timedelta(hours=5)
    from datetime import timedelta as td
    open_t = datetime.strptime("09:30", "%H:%M").time()
    # next open: today 9:30 if before 9:30 and weekday, else next weekday 9:30
    if et.weekday() < 5 and et.time() < open_t:
        next_open = datetime.combine(et.date(), open_t)
        if ZoneInfo is not None:
            next_open = next_open.replace(tzinfo=ZoneInfo("America/New_York"))
        delta = (next_open - et).total_seconds()
        return max(0, int(delta))
    # advance to next day (or Monday if Fri evening / weekend)
    days = 1
    if et.weekday() == 4 and et.time() >= datetime.strptime("16:00", "%H:%M").time():
        days = 3  # Fri 4pm -> Monday
    elif et.weekday() == 5:  # Saturday
        days = 2  # Monday
    elif et.weekday() == 6:  # Sunday
        days = 1  # Monday
    next_day = et.date() + td(days=days)
    next_open = datetime.combine(next_day, open_t)
    if ZoneInfo is not None:
        next_open = next_open.replace(tzinfo=ZoneInfo("America/New_York"))
    delta = (next_open - et).total_seconds()
    return max(0, int(delta))


def _next_market_open_close_et():
    """Return (next_open_utc_ts_sec, next_close_utc_ts_sec, next_premarket_utc_ts_sec) for 9:30 AM, 4:00 PM, and 4:00 AM ET (Mon-Fri). Pre-market = 4:00 AM–9:30 AM ET."""
    from datetime import timedelta as td
    if ZoneInfo is not None:
        et_now = datetime.now(ZoneInfo("America/New_York"))
        tz_et = ZoneInfo("America/New_York")
    else:
        from datetime import timezone
        et_now = datetime.now(timezone.utc) - td(hours=5)
        tz_et = None
    open_t = datetime.strptime("09:30", "%H:%M").time()
    close_t = datetime.strptime("16:00", "%H:%M").time()
    premarket_t = datetime.strptime("04:00", "%H:%M").time()

    def to_ts(d_naive_et):
        if tz_et is not None:
            d = d_naive_et.replace(tzinfo=tz_et)
            return int(d.timestamp())
        from datetime import timezone
        et_fixed = timezone(td(hours=-5))
        return int(d_naive_et.replace(tzinfo=et_fixed).timestamp())

    # Next 9:30 AM ET (regular open)
    if et_now.weekday() < 5 and et_now.time() < open_t:
        next_open = datetime.combine(et_now.date(), open_t)
    else:
        days = 1
        if et_now.weekday() == 4 and et_now.time() >= close_t:
            days = 3
        elif et_now.weekday() == 5:
            days = 2
        elif et_now.weekday() == 6:
            days = 1
        next_open = datetime.combine(et_now.date() + td(days=days), open_t)
    next_open_ts = to_ts(next_open)

    # Next 4:00 PM ET (regular close)
    if et_now.weekday() < 5 and et_now.time() < close_t:
        next_close = datetime.combine(et_now.date(), close_t)
    else:
        days = 1
        if et_now.weekday() == 4:
            days = 3
        elif et_now.weekday() == 5:
            days = 2
        elif et_now.weekday() == 6:
            days = 1
        next_close = datetime.combine(et_now.date() + td(days=days), close_t)
    next_close_ts = to_ts(next_close)

    # Next 4:00 AM ET (pre-market start, Mon–Fri)
    if et_now.weekday() < 5 and et_now.time() < premarket_t:
        next_pre = datetime.combine(et_now.date(), premarket_t)
    else:
        days = 1
        if et_now.weekday() == 4:
            days = 3
        elif et_now.weekday() == 5:
            days = 2
        elif et_now.weekday() == 6:
            days = 1
        next_pre = datetime.combine(et_now.date() + td(days=days), premarket_t)
    next_pre_ts = to_ts(next_pre)
    return (next_open_ts, next_close_ts, next_pre_ts)


@app.route("/api/market-status", methods=["GET"])
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
                if data.get("exchange") and "status" in data:
                    status = str(data.get("status", "")).lower()
                    market_open = status == "open"
                elif "marketOpen" in data:
                    market_open = bool(data["marketOpen"])
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


def _ensure_live_price_history_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS live_price_history (
           timestamp_ms INTEGER NOT NULL,
           live_price_usd REAL NOT NULL,
           usd_to_inr_rate REAL NOT NULL
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_live_price_history_ts ON live_price_history(timestamp_ms)"
    )


@app.route("/api/live-price-history", methods=["GET"])
def get_live_price_history():
    """Return stored live price history for the last N days (default 7). Max 5000 points."""
    days = min(7, max(1, int(request.args.get("days", 7))))
    cutoff_ms = int(time.time() * 1000) - (days * 24 * 60 * 60 * 1000)
    with get_db() as conn:
        _ensure_live_price_history_table(conn)
        cur = conn.execute(
            "SELECT timestamp_ms, live_price_usd, usd_to_inr_rate FROM live_price_history WHERE timestamp_ms >= ? ORDER BY timestamp_ms ASC LIMIT 5001",
            (cutoff_ms,),
        )
        rows = cur.fetchall()
    out = [
        {"timestamp": r[0], "livePriceUsd": round(r[1], 2), "usdToInrRate": round(r[2], 2)}
        for r in rows
    ]
    return jsonify(out)


@app.route("/api/live-price-history", methods=["DELETE"])
def clear_live_price_history():
    """Delete all rows from live_price_history (graph data). Returns count of deleted rows."""
    with get_db() as conn:
        _ensure_live_price_history_table(conn)
        cur = conn.execute("DELETE FROM live_price_history")
        deleted = cur.rowcount
    return jsonify({"ok": True, "deleted": deleted})


@app.route("/api/live-price-history", methods=["POST"])
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
    return jsonify({"ok": True}), 201


def _record_live_price_to_history():
    """Fetch current NVDA price + USD/INR and append one row to live_price_history. Swallows errors."""
    try:
        from helpers import gather_data
        rupee_conv_obj = gather_data.RupeeConv()
        live_price, todays_rp, _ = rupee_conv_obj.get_live_price()
        if live_price is None or todays_rp is None:
            return
        ts_ms = int(time.time() * 1000)
        with get_db() as conn:
            _ensure_live_price_history_table(conn)
            conn.execute(
                "INSERT INTO live_price_history (timestamp_ms, live_price_usd, usd_to_inr_rate) VALUES (?, ?, ?)",
                (ts_ms, round(float(live_price), 2), round(float(todays_rp), 2)),
            )
    except Exception as e:
        print(f"[live-price-recorder] {e}", flush=True)


def _live_price_recorder_loop():
    """Background loop: only when market is open (ET), record live price every 14s. When closed, sleep until next open."""
    RECORDER_INTERVAL_SEC = 14
    while True:
        try:
            if _is_nasdaq_open_et():
                _record_live_price_to_history()
                time.sleep(RECORDER_INTERVAL_SEC)
            else:
                sec = _seconds_until_next_market_open_et()
                if sec > 0:
                    time.sleep(sec)
                else:
                    time.sleep(60)
        except Exception as e:
            print(f"[live-price-recorder] loop error: {e}", flush=True)
            time.sleep(60)


@app.route("/api/live-price", methods=["GET"])
def get_live_price():
    """Lightweight endpoint: NVDA price + USD/INR rate. When in pre-market (4–9:30 AM ET), includes preMarketPriceUsd from yfinance."""
    try:
        from helpers import gather_data

        rupee_conv_obj = gather_data.RupeeConv()
        live_price, todays_rp, open_price = rupee_conv_obj.get_live_price()
        if live_price is None or todays_rp is None:
            return jsonify({"error": "Could not fetch live price or USD/INR rate"}), 503
        payload = {
            "livePriceUsd": round(live_price, 2),
            "usdToInrRate": round(todays_rp, 2),
            "lastUpdated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        }
        if open_price is not None:
            payload["openPriceUsd"] = round(open_price, 2)
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
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 503


def _build_dashboard_response():
    from helpers import gather_data

    db_obj = gather_data.DB()
    if not os.path.isfile(db_obj.db_path):
        return None
    db_obj.ensure_tables()
    db_status = db_obj.check_for_empty_db()
    rupee_conv_obj = gather_data.RupeeConv()
    if db_status["NSU"]:
        rupee_conv_obj.update_null_rupees_rate("NSU", "Buy_Date", "RupeeRate")
    if db_status["ESPP"]:
        rupee_conv_obj.update_null_rupees_rate("ESPP", "Buy_Date", "RupeeRate")
    if db_status["SellOut"]:
        rupee_conv_obj.update_null_rupees_rate("SellOut", "Buy_Date", "BuyRupeeRate")
        rupee_conv_obj.update_null_rupees_rate("SellOut", "Sell_Date", "SellRupeeRate")

    live_price, todays_rp, open_price = rupee_conv_obj.get_live_price()
    if live_price is None or todays_rp is None:
        raise ValueError("Could not fetch live price or USD/INR rate")

    all_qty = 0
    all_profit_after_tax = 0.0
    all_tax = 0.0
    nsu = None
    espp = None

    for stock_type in ("NSU", "ESPP"):
        if not db_status[stock_type]:
            continue
        (_, _, _, total_qty, total_capital_gain, total_tds, avg_buy_price, avg_profit_percent) = (
            gather_data.OwnStockData().generate_display_data(type=stock_type)
        )
        all_qty += total_qty
        all_profit_after_tax += total_capital_gain
        all_tax += total_tds
        blob = {
            "qty": total_qty,
            "profitAfterTax": round(total_capital_gain, 2),
            "avgBuyPrice": round(avg_buy_price, 2),
            "avgProfitPercent": round(avg_profit_percent, 1),
        }
        nsu = blob if stock_type == "NSU" else nsu
        espp = blob if stock_type == "ESPP" else espp

    total_value_usd = round(all_qty * live_price, 2) if live_price else 0
    total_value_inr = round(all_qty * live_price * todays_rp, 2) if live_price and todays_rp else 0
    gain_before_tax = all_profit_after_tax + all_tax
    if gain_before_tax > total_value_inr and total_value_inr > 0:
        scale = total_value_inr / gain_before_tax
        all_tax = round(all_tax * scale, 2)
        all_profit_after_tax = round(total_value_inr - all_tax, 2)
        gain_before_tax = total_value_inr
    net_in_bank_if_sell_now = round(total_value_inr - all_tax, 2) if total_value_inr else 0

    realised_profit = None
    sold_total_qty = None
    sold_total_value_inr = None
    sold_value_rsu_inr = None
    sold_value_espp_inr = None
    if db_status["SellOut"]:
        (_, sell_profit, total_qty_sold, total_sell_inr, total_sell_rsu_inr, total_sell_espp_inr) = (
            gather_data.OwnStockData().generate_sellout_display_data()
        )
        realised_profit = round(sell_profit, 2)
        sold_total_qty = int(total_qty_sold)
        sold_total_value_inr = round(total_sell_inr, 2)
        sold_value_rsu_inr = round(total_sell_rsu_inr, 2)
        sold_value_espp_inr = round(total_sell_espp_inr, 2)

    payload = {
        "livePriceUsd": round(live_price, 2),
        "usdToInrRate": round(todays_rp, 2),
        "totalShares": all_qty,
        "totalValueUsd": total_value_usd,
        "totalValueInr": total_value_inr,
        "unrealisedProfitAfterTax": round(all_profit_after_tax, 2),
        "unrealisedProfitBeforeTax": round(gain_before_tax, 2),
        "totalTaxToPay": round(all_tax, 2),
        "netInBankIfSellNow": net_in_bank_if_sell_now,
        "realisedProfit": realised_profit,
        "soldTotalQty": sold_total_qty,
        "soldTotalValueInr": sold_total_value_inr,
        "soldValueRsuInr": sold_value_rsu_inr,
        "soldValueEsppInr": sold_value_espp_inr,
        "nsu": nsu,
        "espp": espp,
        "canUndoMarkSold": len(_MARK_SOLD_UNDO_STACK) > 0,
    }
    if open_price is not None:
        payload["openPriceUsd"] = round(open_price, 2)
    if _is_premarket_et():
        premarket = rupee_conv_obj.get_premarket_price("NVDA")
        if premarket is not None:
            payload["preMarketPriceUsd"] = round(premarket, 2)
    if _is_postmarket_et():
        postmarket = rupee_conv_obj.get_postmarket_price("NVDA")
        if postmarket is not None:
            payload["postMarketPriceUsd"] = round(postmarket, 2)
    return payload


def _build_holdings_response():
    from helpers import gather_data

    db_obj = gather_data.DB()
    if not os.path.isfile(db_obj.db_path):
        return None
    db_obj.ensure_tables()
    db_status = db_obj.check_for_empty_db()
    rupee_conv_obj = gather_data.RupeeConv()
    if db_status["NSU"]:
        rupee_conv_obj.update_null_rupees_rate("NSU", "Buy_Date", "RupeeRate")
    if db_status["ESPP"]:
        rupee_conv_obj.update_null_rupees_rate("ESPP", "Buy_Date", "RupeeRate")

    rows = []
    for stock_type in ("NSU", "ESPP"):
        if not db_status[stock_type]:
            continue
        df, *_ = gather_data.OwnStockData().generate_display_data(type=stock_type)
        price_col = "TDS_Price_raw" if stock_type == "ESPP" else "Price_Bought_raw"
        for _, r in df.iterrows():
            try:
                qty = int(float(r["Available_Sell"])) if r["Available_Sell"] is not None else 0
            except (TypeError, ValueError):
                qty = 0
            buy_date = r.get("Buy_Date")
            buy_date_str = buy_date.isoformat() if hasattr(buy_date, "isoformat") else (str(buy_date) if buy_date else "")
            value_today_inr = float(r["TodaysValue_raw"])
            tax_to_pay_inr = float(r["TaxNeedtoPay_raw"])
            net_if_sell_today_inr = value_today_inr - tax_to_pay_inr
            tax_slab_pct = float(r.get("TaxSlab", 0) * 100) if "TaxSlab" in r else 0
            row_data = {
                "type": stock_type,
                "buyDate": buy_date_str,
                "qty": qty,
                "buyPriceUsd": float(r[price_col]),
                "totalPurchaseInr": float(r["InitialValue_raw"]),
                "netIfSellTodayInr": net_if_sell_today_inr,
                "profitPercent": round(float(r["ProfitPercent"]), 1),
                "taxToPayInr": tax_to_pay_inr,
                "taxPercent": tax_slab_pct,
            }
            if stock_type == "ESPP":
                row_data["priceBoughtUsd"] = float(r["Price_Bought_raw"])
            rows.append(row_data)
    return rows


def _parse_date(s, fmt="%m/%d/%Y"):
    if s is None or (isinstance(s, str) and not s.strip()):
        return None
    if isinstance(s, date):
        return s
    if hasattr(s, "date") and callable(getattr(s, "date")):
        return s.date()
    s = str(s).strip()[:10]
    try:
        return datetime.strptime(s, fmt).date()
    except ValueError:
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            return None


def _build_sold_response():
    from helpers import gather_data

    db_obj = gather_data.DB()
    if not os.path.isfile(db_obj.db_path):
        return None
    db_obj.ensure_tables()
    db_status = db_obj.check_for_empty_db()
    if not db_status.get("SellOut"):
        return []
    rupee_conv_obj = gather_data.RupeeConv()
    rupee_conv_obj.update_null_rupees_rate("SellOut", "Buy_Date", "BuyRupeeRate")
    rupee_conv_obj.update_null_rupees_rate("SellOut", "Sell_Date", "SellRupeeRate")
    tax_obj = gather_data.Tax()
    df = db_obj.get_table_data("SellOut")
    rows = []
    for _, r in df.iterrows():
        try:
            qty = int(float(r.get("Qty_Sold") or 0))
        except (TypeError, ValueError):
            qty = 0
        price_bought = float(r.get("Price_Bought") or 0)
        price_sell = float(r.get("Price_Sell") or 0)
        buy_rate = float(r.get("BuyRupeeRate") or 0)
        sell_rate = float(r.get("SellRupeeRate") or 0)
        buy_date = _parse_date(str(r.get("Buy_Date", "")) if r.get("Buy_Date") is not None else "")
        sell_date = _parse_date(str(r.get("Sell_Date", "")) if r.get("Sell_Date") is not None else "")
        typ = (str(r.get("Type") or "NSU")).strip()
        buy_at_r = qty * price_bought * buy_rate
        sell_at_r = qty * price_sell * sell_rate
        gain_before_tax = sell_at_r - buy_at_r
        tax_slab = tax_obj.get_tax_slab(buy_date) if buy_date else tax_obj.fix_tax_slab
        tax_paid = round(gain_before_tax * tax_slab, 2)
        profit_pct = round((gain_before_tax - tax_paid) / buy_at_r * 100, 1) if buy_at_r else 0
        rows.append({
            "sellDate": sell_date.isoformat() if sell_date else "",
            "buyDate": buy_date.isoformat() if buy_date else "",
            "qtySold": qty,
            "type": typ,
            "priceBoughtUsd": price_bought,
            "priceSellUsd": price_sell,
            "buyValueInr": buy_at_r,
            "sellValueInr": sell_at_r,
            "gainBeforeTaxInr": gain_before_tax,
            "taxPaidInr": tax_paid,
            "profitPercent": profit_pct,
            "taxPercent": tax_slab * 100,
        })
    return rows


@app.route("/api/dashboard", methods=["GET"])
def get_dashboard():
    skip_cache = request.args.get("refresh") == "1"
    now = time.time()
    cached = _DASHBOARD_CACHE.get("data")
    cached_at = _DASHBOARD_CACHE.get("at", 0)
    if not skip_cache and cached is not None and (now - cached_at) < _DASHBOARD_CACHE_TTL:
        return jsonify(cached)
    try:
        data = _build_dashboard_response()
        if data is None:
            return jsonify({"error": "Database not found"}), 404
        _DASHBOARD_CACHE["data"] = data
        _DASHBOARD_CACHE["at"] = now
        return jsonify(data)
    except Exception as e:
        _DASHBOARD_CACHE.pop("data", None)
        _DASHBOARD_CACHE.pop("at", None)
        return jsonify({"error": str(e)}), 503


@app.route("/api/holdings", methods=["GET"])
def get_holdings():
    try:
        rows = _build_holdings_response()
        if rows is None:
            return jsonify({"error": "Database not found"}), 404
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 503


@app.route("/api/sold", methods=["GET"])
def get_sold():
    try:
        rows = _build_sold_response()
        if rows is None:
            return jsonify({"error": "Database not found"}), 404
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 503


# In-memory undo stack for last mark-sold: list of { "sellout_rowids": [...], "operations": [{ type, buy_date_str, buy_price_usd, price_bought_usd, qty }, ...] }
_MARK_SOLD_UNDO_STACK = []


@app.route("/api/mark-sold", methods=["POST"])
def mark_sold():
    from helpers import gather_data

    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON body required"}), 400
    sell_date_str = (data.get("sellDate") or "").strip()[:10]
    price_sell_usd = data.get("priceSellUsd")
    items = data.get("items") or []
    sell_date = _parse_date(sell_date_str)
    if not sell_date:
        return jsonify({"error": "Invalid sell date"}), 400
    if not isinstance(price_sell_usd, (int, float)) or price_sell_usd <= 0:
        return jsonify({"error": "Invalid sell price"}), 400
    if not items:
        return jsonify({"error": "No items"}), 400

    rupee_conv = gather_data.RupeeConv()
    # get_rupee_rate expects datetime or str (not date); use YYYY-MM-DD
    sell_rate = rupee_conv.get_rupee_rate(sell_date.strftime("%Y-%m-%d"))
    if sell_rate is None:
        return jsonify({"error": "Could not get exchange rate for sell date"}), 400

    validated = []
    for it in items:
        typ = (it.get("type") or "").strip().upper()
        if typ not in ("NSU", "ESPP"):
            return jsonify({"error": f"Invalid type: {typ}"}), 400
        buy_date_str = (it.get("buyDate") or "").strip()[:10]
        buy_date = _parse_date(buy_date_str)
        if not buy_date:
            return jsonify({"error": f"Invalid buy date: {buy_date_str}"}), 400
        buy_price_usd = it.get("buyPriceUsd")
        if not isinstance(buy_price_usd, (int, float)) or buy_price_usd < 0:
            return jsonify({"error": "Invalid buyPriceUsd"}), 400
        price_bought_usd = it.get("priceBoughtUsd") if typ == "ESPP" else buy_price_usd
        if typ == "ESPP" and (not isinstance(price_bought_usd, (int, float)) or price_bought_usd < 0):
            return jsonify({"error": "Invalid priceBoughtUsd for ESPP"}), 400
        qty = int(it.get("qtyToSell") or 0)
        if qty <= 0:
            return jsonify({"error": "qtyToSell must be positive"}), 400
        buy_rate = rupee_conv.get_rupee_rate(buy_date.strftime("%Y-%m-%d"))
        if buy_rate is None:
            return jsonify({"error": f"Could not get exchange rate for buy date {buy_date_str}"}), 400
        # DB stores Buy_Date as MM/DD/YYYY (gather_data); frontend sends YYYY-MM-DD
        buy_date_db = buy_date.strftime("%m/%d/%Y")
        validated.append({
            "type": typ,
            "buy_date_str": buy_date_str,
            "buy_date_db": buy_date_db,
            "buy_price_usd": float(buy_price_usd),
            "price_bought_usd": float(price_bought_usd),
            "qty": qty,
            "buy_rate": buy_rate,
        })

    # Tolerance for float match (DB may store 177.19, frontend sends 177.2)
    _PRICE_EPS = 0.006

    sellout_rowids = []
    with get_db() as conn:
        for v in validated:
            table = "NSU" if v["type"] == "NSU" else "ESPP"
            row = None
            for try_date in (v["buy_date_db"], v["buy_date_str"]):
                if table == "NSU":
                    cur = conn.execute(
                        "SELECT Available_Sell, Price_Bought FROM NSU WHERE Buy_Date = ? AND ABS(Price_Bought - ?) < ?",
                        (try_date, v["buy_price_usd"], _PRICE_EPS),
                    )
                else:
                    cur = conn.execute(
                        """SELECT Available_Sell, Price_Bought, TDS_Price FROM ESPP
                           WHERE Buy_Date = ? AND ABS(Price_Bought - ?) < ? AND ABS(TDS_Price - ?) < ?""",
                        (try_date, v["price_bought_usd"], _PRICE_EPS, v["buy_price_usd"], _PRICE_EPS),
                    )
                row = cur.fetchone()
                if row:
                    v["_match_buy_date"] = try_date
                    v["_match_price_bought"] = float(row[1]) if row[1] is not None else (v["price_bought_usd"] if table == "ESPP" else v["buy_price_usd"])
                    if table == "ESPP":
                        v["_match_tds_price"] = float(row[2]) if row[2] is not None else v["buy_price_usd"]
                    break
            if not row:
                return jsonify({"error": f"Lot not found: {v['type']} {v['buy_date_str']}"}), 400
            avail = int(row[0]) if row[0] is not None else 0
            if avail < v["qty"]:
                return jsonify({
                    "error": f"Insufficient available qty for lot {v['buy_date_str']}: has {avail}, need {v['qty']}",
                }), 400

        inserted = 0
        price_sell_val = float(price_sell_usd)
        for v in validated:
            match_date = v["_match_buy_date"]
            cur = conn.execute(
                """INSERT INTO SellOut (Sell_Date, Buy_Date, Qty_Sold, Price_Bought, Price_Sell, BuyRupeeRate, SellRupeeRate, Type)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    sell_date.strftime("%m/%d/%Y"),
                    match_date,
                    v["qty"],
                    v["price_bought_usd"],
                    price_sell_val,
                    v["buy_rate"],
                    sell_rate,
                    v["type"],
                ),
            )
            rowid = cur.lastrowid
            if rowid is not None:
                sellout_rowids.append(rowid)
            inserted += 1
            if v["type"] == "NSU":
                conn.execute(
                    "UPDATE NSU SET Available_Sell = Available_Sell - ? WHERE Buy_Date = ? AND ABS(Price_Bought - ?) < ?",
                    (v["qty"], match_date, v["_match_price_bought"], _PRICE_EPS),
                )
            else:
                conn.execute(
                    "UPDATE ESPP SET Available_Sell = Available_Sell - ? WHERE Buy_Date = ? AND ABS(Price_Bought - ?) < ? AND ABS(TDS_Price - ?) < ?",
                    (v["qty"], match_date, v["_match_price_bought"], _PRICE_EPS, v["_match_tds_price"], _PRICE_EPS),
                )

        if sellout_rowids:
            undo_ops = [
                {
                    "type": v["type"],
                    "buy_date_db": v["_match_buy_date"],
                    "buy_price_usd": v["_match_tds_price"] if v["type"] == "ESPP" else v["_match_price_bought"],
                    "price_bought_usd": v["_match_price_bought"],
                    "qty": v["qty"],
                }
                for v in validated
            ]
            _MARK_SOLD_UNDO_STACK.append({"sellout_rowids": list(sellout_rowids), "operations": undo_ops})

    _DASHBOARD_CACHE.pop("data", None)
    _DASHBOARD_CACHE.pop("at", None)
    return jsonify({"success": True, "inserted": inserted})


@app.route("/api/mark-sold-can-undo", methods=["GET"])
def mark_sold_can_undo():
    """Lightweight check for undo availability (not cached with dashboard)."""
    return jsonify({"canUndo": len(_MARK_SOLD_UNDO_STACK) > 0})


@app.route("/api/mark-sold-undo", methods=["POST"])
def mark_sold_undo():
    if not _MARK_SOLD_UNDO_STACK:
        return jsonify({"error": "Nothing to undo"}), 404
    entry = _MARK_SOLD_UNDO_STACK.pop()
    sellout_rowids = entry["sellout_rowids"]
    operations = entry["operations"]
    with get_db() as conn:
        if sellout_rowids:
            placeholders = ",".join("?" * len(sellout_rowids))
            conn.execute(f"DELETE FROM SellOut WHERE rowid IN ({placeholders})", sellout_rowids)
        for op in operations:
            typ = op["type"]
            buy_date_db = op.get("buy_date_db")
            if not buy_date_db:
                d = _parse_date(op.get("buy_date_str", ""))
                buy_date_db = d.strftime("%m/%d/%Y") if d else op.get("buy_date_str", "")
            buy_price_usd = op["buy_price_usd"]
            price_bought_usd = op["price_bought_usd"]
            qty = op["qty"]
            if typ == "NSU":
                conn.execute(
                    "UPDATE NSU SET Available_Sell = Available_Sell + ? WHERE Buy_Date = ? AND Price_Bought = ?",
                    (qty, buy_date_db, buy_price_usd),
                )
            else:
                conn.execute(
                    "UPDATE ESPP SET Available_Sell = Available_Sell + ? WHERE Buy_Date = ? AND Price_Bought = ? AND TDS_Price = ?",
                    (qty, buy_date_db, price_bought_usd, buy_price_usd),
                )
    _DASHBOARD_CACHE.pop("data", None)
    _DASHBOARD_CACHE.pop("at", None)
    return jsonify({"success": True, "undone": len(sellout_rowids)})


_TAX_CONFIG_PATH = os.path.join(REPO_ROOT, "configs", "tax_config.json")


@app.route("/api/tax-config", methods=["GET"])
def get_tax_config():
    import json as _json

    if not os.path.isfile(_TAX_CONFIG_PATH):
        return jsonify({"error": "tax_config.json not found"}), 404
    try:
        with open(_TAX_CONFIG_PATH, "r") as f:
            data = _json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tax-config", methods=["PUT"])
def put_tax_config():
    import json as _json

    body = request.get_json()
    if not body:
        return jsonify({"error": "JSON body required"}), 400
    try:
        with open(_TAX_CONFIG_PATH, "w") as f:
            _json.dump(body, f, indent=2)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/generate-tax-doc", methods=["GET"])
def generate_tax_doc():
    """Generate the ITR foreign-asset schedule JSON from holdings data + tax_config template.
    Query param `fy` = assessment year (e.g. 2026 for AY 2026–27, FY starting 1 Apr 2025).
    Defaults to current calendar year.
    """
    import json as _json
    from helpers import gather_data

    fy_year = request.args.get("fy", type=int) or date.today().year
    fy_start = datetime(fy_year - 1, 4, 1)
    fy_end = datetime(fy_year, 3, 31)

    db_obj = gather_data.DB()
    if not os.path.isfile(db_obj.db_path):
        return jsonify({"error": "Database not found"}), 404
    db_obj.ensure_tables()
    db_status = db_obj.check_for_empty_db()
    rupee_conv_obj = gather_data.RupeeConv()
    if db_status.get("NSU"):
        rupee_conv_obj.update_null_rupees_rate("NSU", "Buy_Date", "RupeeRate")
    if db_status.get("ESPP"):
        rupee_conv_obj.update_null_rupees_rate("ESPP", "Buy_Date", "RupeeRate")

    template = {}
    try:
        with open(_TAX_CONFIG_PATH, "r") as f:
            cfg = _json.load(f)
        template = cfg.get("foreignAssetTemplate", {})
    except Exception:
        pass

    datacleaner_obj = gather_data.DataCleaner()
    shares_list = []

    for stock_type in ("NSU", "ESPP"):
        if not db_status.get(stock_type):
            continue
        df, *_ = gather_data.OwnStockData().generate_display_data(type=stock_type)
        for _, row in df.iterrows():
            invest_date = datetime.strptime(row["Buy_Date_formatted"], "%d/%m/%Y")
            if invest_date > fy_end:
                continue
            entry = dict(template)
            entry["InterestAcquiringDate"] = invest_date.strftime("%Y-%m-%d")
            entry["InitialValOfInvstmnt"] = int(round(float(datacleaner_obj.convert_from_symbol(row["InitialValue"])), 0))
            entry["PeakBalanceDuringPeriod"] = int(round(float(datacleaner_obj.convert_from_symbol(row["Max_Value_FY"])), 0))
            entry["ClosingBalance"] = int(round(float(datacleaner_obj.convert_from_symbol(row["FY_Closing_Value"])), 0))
            shares_list.append(entry)

    return jsonify({"fyLabel": f"FY {fy_year - 1}–{str(fy_year)[-2:]} (AY {fy_year}–{str(fy_year + 1)[-2:]})", "rows": shares_list})


PORT = 8080

if __name__ == "__main__":
    if not os.path.isfile(DB_PATH):
        sys.exit(f"Database not found: {DB_PATH}")
    rec = threading.Thread(target=_live_price_recorder_loop, daemon=True)
    rec.start()
    print(f"Backend API at http://127.0.0.1:{PORT} (configs at {REPO_ROOT})")
    print("Live price recorder: running in background (records every 14s when market open)")
    app.run(host="0.0.0.0", port=PORT, debug=True)
