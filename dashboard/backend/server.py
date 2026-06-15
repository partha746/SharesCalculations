"""
Flask API backend for the Angular dashboard (nvShares.db).
Run from dashboard folder: python backend/server.py
Uses repo root (parent of dashboard) for configs/ and helpers/.
"""
import json
import math
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

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(_DASHBOARD_DIR, ".env"))
except ImportError:
    pass


def _reload_dashboard_env():
    """Re-read dashboard/.env so Breeze keys apply without restarting Flask."""
    try:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(_DASHBOARD_DIR, ".env"), override=True)
    except ImportError:
        pass


import requests

from flask import Flask, Response, jsonify, redirect, request, send_file

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
        # Naive "ET" wall clock (UTC−5, no DST) so next_open from combine() matches for subtraction.
        from datetime import timezone, timedelta

        et = (datetime.now(timezone.utc) - timedelta(hours=5)).replace(tzinfo=None)
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


def _get_previous_day_inr_rate():
    """Get the last stored USD→INR rate from before today (UTC midnight). Returns float or None."""
    import calendar
    today_start_ms = int(
        calendar.timegm(date.today().timetuple())
    ) * 1000
    try:
        with get_db() as conn:
            _ensure_live_price_history_table(conn)
            row = conn.execute(
                "SELECT usd_to_inr_rate FROM live_price_history WHERE timestamp_ms < ? ORDER BY timestamp_ms DESC LIMIT 1",
                (today_start_ms,),
            ).fetchone()
            if row:
                return float(row[0])
    except Exception:
        pass
    return None


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
    # OHLC rollups for fast long-range visualization. 1m/1h keyed by UTC-aligned bucket; 1d keyed by ET calendar date.
    for tbl in ("live_price_ohlc_1m", "live_price_ohlc_1h"):
        conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {tbl} (
               bucket_ms INTEGER PRIMARY KEY,
               open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
               sum_price REAL NOT NULL, n INTEGER NOT NULL, rate_close REAL NOT NULL,
               last_ts_ms INTEGER NOT NULL
            )"""
        )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS live_price_ohlc_1d (
           et_date TEXT PRIMARY KEY,
           bucket_ms INTEGER NOT NULL,
           open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
           sum_price REAL NOT NULL, n INTEGER NOT NULL, rate_close REAL NOT NULL,
           last_ts_ms INTEGER NOT NULL
        )"""
    )


def _et_date_str(ts_ms):
    """ET calendar date (YYYY-MM-DD) for a UTC ms timestamp."""
    from datetime import timezone, timedelta
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    if ZoneInfo is not None:
        dt = dt.astimezone(ZoneInfo("America/New_York"))
    else:
        dt = dt - timedelta(hours=5)
    return dt.strftime("%Y-%m-%d")


def _update_rollups(conn, ts_ms, price, rate):
    """Incrementally fold one tick into the 1m/1h/1d OHLC rollups (UPSERT)."""
    minute = (ts_ms // 60000) * 60000
    hour = (ts_ms // 3600000) * 3600000
    for tbl, bucket in (("live_price_ohlc_1m", minute), ("live_price_ohlc_1h", hour)):
        conn.execute(
            f"""INSERT INTO {tbl} (bucket_ms, open, high, low, close, sum_price, n, rate_close, last_ts_ms)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(bucket_ms) DO UPDATE SET
                  high=MAX(high, excluded.high),
                  low=MIN(low, excluded.low),
                  close=CASE WHEN excluded.last_ts_ms >= last_ts_ms THEN excluded.close ELSE close END,
                  rate_close=CASE WHEN excluded.last_ts_ms >= last_ts_ms THEN excluded.rate_close ELSE rate_close END,
                  open=CASE WHEN excluded.last_ts_ms < last_ts_ms AND excluded.bucket_ms = bucket_ms THEN excluded.open ELSE open END,
                  sum_price=sum_price + excluded.sum_price,
                  n=n + 1,
                  last_ts_ms=MAX(last_ts_ms, excluded.last_ts_ms)
            """,
            (bucket, price, price, price, price, price, rate, ts_ms),
        )
    et_date = _et_date_str(ts_ms)
    conn.execute(
        """INSERT INTO live_price_ohlc_1d (et_date, bucket_ms, open, high, low, close, sum_price, n, rate_close, last_ts_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(et_date) DO UPDATE SET
              high=MAX(high, excluded.high),
              low=MIN(low, excluded.low),
              close=CASE WHEN excluded.last_ts_ms >= last_ts_ms THEN excluded.close ELSE close END,
              rate_close=CASE WHEN excluded.last_ts_ms >= last_ts_ms THEN excluded.rate_close ELSE rate_close END,
              open=CASE WHEN excluded.bucket_ms < bucket_ms THEN excluded.open ELSE open END,
              bucket_ms=MIN(bucket_ms, excluded.bucket_ms),
              sum_price=sum_price + excluded.sum_price,
              n=n + 1,
              last_ts_ms=MAX(last_ts_ms, excluded.last_ts_ms)
        """,
        (et_date, ts_ms, price, price, price, price, price, rate, ts_ms),
    )


def _backfill_rollups_if_needed(conn):
    """Build OHLC rollups from existing raw history once (when rollups are empty but raw data exists)."""
    if conn.execute("SELECT 1 FROM live_price_ohlc_1m LIMIT 1").fetchone():
        return
    rows = conn.execute(
        "SELECT timestamp_ms, live_price_usd, usd_to_inr_rate FROM live_price_history ORDER BY timestamp_ms ASC"
    ).fetchall()
    if not rows:
        return

    def aggregate(key_fn):
        # value: [open, high, low, close, sum, n, rate, first_ts, last_ts, bucket_ms]
        out = {}
        for ts, price, rate in rows:
            key, bucket_ms = key_fn(ts)
            e = out.get(key)
            if e is None:
                out[key] = [price, price, price, price, price, 1, rate, ts, ts, bucket_ms]
            else:
                if price > e[1]:
                    e[1] = price
                if price < e[2]:
                    e[2] = price
                e[4] += price
                e[5] += 1
                if ts >= e[8]:
                    e[3] = price
                    e[6] = rate
                    e[8] = ts
                if ts < e[7]:
                    e[0] = price
                    e[7] = ts
                    e[9] = min(e[9], bucket_ms)
        return out

    m1 = aggregate(lambda ts: ((ts // 60000) * 60000, (ts // 60000) * 60000))
    h1 = aggregate(lambda ts: ((ts // 3600000) * 3600000, (ts // 3600000) * 3600000))
    d1 = aggregate(lambda ts: (_et_date_str(ts), ts))

    for tbl, agg in (("live_price_ohlc_1m", m1), ("live_price_ohlc_1h", h1)):
        conn.executemany(
            f"INSERT OR REPLACE INTO {tbl} (bucket_ms, open, high, low, close, sum_price, n, rate_close, last_ts_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(k, e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[8]) for k, e in agg.items()],
        )
    conn.executemany(
        "INSERT OR REPLACE INTO live_price_ohlc_1d (et_date, bucket_ms, open, high, low, close, sum_price, n, rate_close, last_ts_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(k, e[9], e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[8]) for k, e in d1.items()],
    )
    print(f"[rollups] backfilled from {len(rows)} raw rows: {len(m1)} 1m, {len(h1)} 1h, {len(d1)} 1d buckets", flush=True)


# Nice bucket sizes (ms) for chart aggregation, ascending.
_AGG_BUCKETS_MS = [
    60000, 120000, 300000, 600000, 900000, 1800000,
    3600000, 7200000, 14400000, 21600000, 43200000, 86400000,
]


@app.route("/api/live-price-history", methods=["GET"])
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


@app.route("/api/live-price-history", methods=["DELETE"])
def clear_live_price_history():
    """Delete all rows from live_price_history (graph data). Returns count of deleted rows."""
    with get_db() as conn:
        _ensure_live_price_history_table(conn)
        cur = conn.execute("DELETE FROM live_price_history")
        deleted = cur.rowcount
        for tbl in ("live_price_ohlc_1m", "live_price_ohlc_1h", "live_price_ohlc_1d"):
            conn.execute(f"DELETE FROM {tbl}")
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
        _update_rollups(conn, ts_ms, float(price), float(rate))
    return jsonify({"ok": True}), 201


def _record_live_price_to_history():
    """Fetch current NVDA price + USD/INR and append one row to live_price_history. Swallows errors."""
    try:
        from helpers import gather_data
        rupee_conv_obj = gather_data.RupeeConv()
        live_price, todays_rp, *_ = rupee_conv_obj.get_live_price()
        if live_price is None or todays_rp is None:
            return
        ts_ms = int(time.time() * 1000)
        price = round(float(live_price), 2)
        rate = round(float(todays_rp), 2)
        with get_db() as conn:
            _ensure_live_price_history_table(conn)
            conn.execute(
                "INSERT INTO live_price_history (timestamp_ms, live_price_usd, usd_to_inr_rate) VALUES (?, ?, ?)",
                (ts_ms, price, rate),
            )
            _update_rollups(conn, ts_ms, price, rate)
    except Exception as e:
        print(f"[live-price-recorder] {e}", flush=True)


_DB_SIZE_CAP_BYTES = 5 * 1024 * 1024 * 1024  # 5 GB: prune oldest raw ticks beyond this (rollups are tiny, kept).


def _prune_raw_if_over_cap():
    """If the DB exceeds the 5 GB cap, delete the oldest raw ticks (rollups are retained). Effectively never triggers."""
    try:
        with get_db() as conn:
            page_size = conn.execute("PRAGMA page_size").fetchone()[0]
            page_count = conn.execute("PRAGMA page_count").fetchone()[0]
            size = page_size * page_count
            if size <= _DB_SIZE_CAP_BYTES:
                return
            # Delete the oldest ~20% of raw rows to get back under the cap; keep rollups intact.
            total = conn.execute("SELECT COUNT(*) FROM live_price_history").fetchone()[0]
            to_delete = max(1, total // 5)
            cutoff = conn.execute(
                "SELECT timestamp_ms FROM live_price_history ORDER BY timestamp_ms ASC LIMIT 1 OFFSET ?",
                (to_delete,),
            ).fetchone()
            if cutoff:
                conn.execute("DELETE FROM live_price_history WHERE timestamp_ms < ?", (cutoff[0],))
                print(f"[live-price-recorder] DB over 5GB cap ({size} bytes); pruned ~{to_delete} oldest raw ticks", flush=True)
    except Exception as e:
        print(f"[live-price-recorder] prune error: {e}", flush=True)


def _live_price_recorder_loop():
    """Background loop: only when market is open (ET), record live price every 14s. When closed, sleep until next open."""
    RECORDER_INTERVAL_SEC = 14
    try:
        with get_db() as conn:
            _ensure_live_price_history_table(conn)
            _backfill_rollups_if_needed(conn)
    except Exception as e:
        print(f"[live-price-recorder] rollup init error: {e}", flush=True)
    iters = 0
    while True:
        try:
            if _is_nasdaq_open_et():
                _record_live_price_to_history()
                iters += 1
                if iters % 200 == 0:  # ~ every 47 min of trading
                    _prune_raw_if_over_cap()
                time.sleep(RECORDER_INTERVAL_SEC)
            else:
                _prune_raw_if_over_cap()
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

    live_price, todays_rp, open_price, prev_close = rupee_conv_obj.get_live_price()
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
        "usdToInrRate": round(float(todays_rp), 4),
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
    if prev_close is not None:
        # Prefer Frankfurter historical (same family as live INR) for "yesterday's" rate; then DB; else today.
        prev_inr_rate = (
            rupee_conv_obj.get_usd_to_inr_for_prior_calendar_day()
            or _get_previous_day_inr_rate()
            or todays_rp
        )
        payload["previousCloseUsd"] = round(prev_close, 2)
        payload["previousCloseValueUsd"] = round(all_qty * prev_close, 2)
        payload["previousCloseValueInr"] = round(all_qty * prev_close * prev_inr_rate, 2)
        payload["previousCloseUsdToInrRate"] = round(prev_inr_rate, 2)
    if _is_premarket_et():
        premarket = rupee_conv_obj.get_premarket_price("NVDA")
        if premarket is not None:
            payload["preMarketPriceUsd"] = round(premarket, 2)
    if _is_postmarket_et():
        postmarket = rupee_conv_obj.get_postmarket_price("NVDA")
        if postmarket is not None:
            payload["postMarketPriceUsd"] = round(postmarket, 2)
    return payload


def _safe_float(v, default=0.0):
    """Convert to float, replacing NaN/Infinity with *default* so JSON stays valid."""
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


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
            if qty <= 0:
                continue
            buy_date = r.get("Buy_Date")
            buy_date_str = buy_date.isoformat() if hasattr(buy_date, "isoformat") else (str(buy_date) if buy_date else "")
            value_today_inr = _safe_float(r["TodaysValue_raw"])
            tax_to_pay_inr = _safe_float(r["TaxNeedtoPay_raw"])
            net_if_sell_today_inr = value_today_inr - tax_to_pay_inr
            tax_slab_pct = _safe_float(r.get("TaxSlab", 0) * 100) if "TaxSlab" in r else 0
            row_data = {
                "type": stock_type,
                "buyDate": buy_date_str,
                "qty": qty,
                "buyPriceUsd": _safe_float(r[price_col]),
                "totalPurchaseInr": _safe_float(r["InitialValue_raw"]),
                "netIfSellTodayInr": net_if_sell_today_inr,
                "profitPercent": round(_safe_float(r["ProfitPercent"]), 1),
                "taxToPayInr": tax_to_pay_inr,
                "taxPercent": tax_slab_pct,
            }
            if stock_type == "ESPP":
                row_data["priceBoughtUsd"] = _safe_float(r["Price_Bought_raw"])
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
            sell_buy_price = v["_match_tds_price"] if v["type"] == "ESPP" else v["price_bought_usd"]
            cur = conn.execute(
                """INSERT INTO SellOut (Sell_Date, Buy_Date, Qty_Sold, Price_Bought, Price_Sell, BuyRupeeRate, SellRupeeRate, Type)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    sell_date.strftime("%m/%d/%Y"),
                    match_date,
                    v["qty"],
                    sell_buy_price,
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


# --- ICICI Direct Breeze (optional; pip install breeze-connect + env BREEZE_API_KEY / BREEZE_API_SECRET) ---
try:
    import breeze_icici
except ImportError:
    breeze_icici = None  # type: ignore

def _extract_breeze_session_token():
    """ICICI may return the session via GET query string or POST body (form / JSON)."""
    keys = (
        "apisession", "API_Session", "api_session",
        "session_token", "Session_Token", "APISession",
    )
    for key in keys:
        v = request.values.get(key)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    if request.is_json:
        data = request.get_json(silent=True) or {}
        for key in keys:
            if key in data and data[key] is not None and str(data[key]).strip() != "":
                return str(data[key]).strip()
    return ""


def _breeze_public_base():
    base = (os.environ.get("BREEZE_PUBLIC_BASE_URL") or "").strip().rstrip("/")
    return base or request.url_root.rstrip("/")


def _breeze_callback_url_for_acct(acct):
    return f"{_breeze_public_base()}/api/breeze/callback/{acct}"


def _breeze_dashboard_url():
    base = (os.environ.get("BREEZE_DASHBOARD_URL") or "").strip().rstrip("/")
    if not base:
        base = _breeze_public_base()
    return base + "/?tab=icici"


def _breeze_js_redirect(url):
    return Response(
        f'<!DOCTYPE html><html><head><meta charset="utf-8"/>'
        f'<title>Redirecting…</title></head><body>'
        f'<p>Connecting… redirecting to dashboard.</p>'
        f'<script>window.location.replace({json.dumps(url)});</script>'
        f'<noscript><p><a href="{url}">Click here to continue</a></p></noscript>'
        f'</body></html>',
        status=200,
        mimetype="text/html; charset=utf-8",
        headers={"Cache-Control": "no-store"},
    )


@app.route("/api/breeze/callback/<acct>", methods=["GET", "POST"])
def breeze_oauth_callback(acct):
    _reload_dashboard_env()
    if breeze_icici is None or acct not in breeze_icici.VALID_ACCOUNT_IDS:
        return _breeze_js_redirect(_breeze_dashboard_url() + "&breeze_error=invalid_account")
    token = _extract_breeze_session_token()
    print(f"[breeze-callback/{acct}] token={'YES' if token else 'EMPTY'}, query={request.query_string.decode()}", flush=True)
    if token:
        try:
            breeze_icici.connect_session(token, acct)
            dest = _breeze_dashboard_url() + f"&breeze_connected={acct}"
            print(f"[breeze-callback/{acct}] connect_session OK, redirecting to: {dest}", flush=True)
            return _breeze_js_redirect(dest)
        except Exception as e:
            print(f"[breeze-callback/{acct}] connect_session FAILED: {e}", flush=True)
            import urllib.parse
            return _breeze_js_redirect(_breeze_dashboard_url() + "&breeze_error=" + urllib.parse.quote(str(e)))
    return _breeze_js_redirect(_breeze_dashboard_url() + "&breeze_error=no_token")


# Keep old path as alias for account 1
@app.route("/api/breeze/callback", methods=["GET", "POST"])
def breeze_oauth_callback_default():
    return breeze_oauth_callback("1")


@app.route("/api/breeze/status/<acct>", methods=["GET"])
def breeze_status(acct):
    _reload_dashboard_env()
    callback_url = _breeze_callback_url_for_acct(acct)
    if breeze_icici is None:
        return jsonify({
            "sdkInstalled": False, "configured": False, "connected": False,
            "loginUrl": None, "callbackUrl": callback_url,
            "message": "breeze_icici module not found",
        })
    if acct not in breeze_icici.VALID_ACCOUNT_IDS:
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    return jsonify({
        "sdkInstalled": breeze_icici.sdk_installed(),
        "configured": breeze_icici.is_configured(acct),
        "connected": breeze_icici.get_client(acct) is not None,
        "loginUrl": breeze_icici.login_url(acct),
        "callbackUrl": callback_url,
    })


@app.route("/api/breeze/status", methods=["GET"])
def breeze_status_all():
    """Combined status for all accounts."""
    _reload_dashboard_env()
    if breeze_icici is None:
        return jsonify({"accounts": {}})
    result = {}
    for acct in breeze_icici.VALID_ACCOUNT_IDS:
        result[acct] = {
            "sdkInstalled": breeze_icici.sdk_installed(),
            "configured": breeze_icici.is_configured(acct),
            "connected": breeze_icici.get_client(acct) is not None,
            "loginUrl": breeze_icici.login_url(acct),
            "callbackUrl": _breeze_callback_url_for_acct(acct),
        }
    return jsonify({"accounts": result})


@app.route("/api/breeze/disconnect/<acct>", methods=["POST"])
def breeze_disconnect(acct):
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    if acct not in breeze_icici.VALID_ACCOUNT_IDS:
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    breeze_icici.disconnect(acct)
    return jsonify({"success": True})


@app.route("/api/breeze/portfolio-holdings/<acct>", methods=["GET"])
def breeze_portfolio_holdings(acct):
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    if acct not in breeze_icici.VALID_ACCOUNT_IDS:
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    exchange_code = (request.args.get("exchange_code") or request.args.get("exchangeCode") or "").strip()
    if not exchange_code:
        return jsonify({"error": "exchange_code is required"}), 400
    from_date = (request.args.get("from_date") or request.args.get("fromDate") or "").strip()
    to_date = (request.args.get("to_date") or request.args.get("toDate") or "").strip()
    stock_code = (request.args.get("stock_code") or request.args.get("stockCode") or "").strip()
    portfolio_type = (request.args.get("portfolio_type") or request.args.get("portfolioType") or "").strip()
    try:
        return jsonify(
            breeze_icici.api_get_portfolio_holdings(exchange_code, from_date, to_date, stock_code, portfolio_type, acct=acct)
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/breeze/portfolio-positions/<acct>", methods=["GET"])
def breeze_portfolio_positions(acct):
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    if acct not in breeze_icici.VALID_ACCOUNT_IDS:
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    try:
        return jsonify(breeze_icici.api_get_portfolio_positions(acct))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/breeze/demat-holdings/<acct>", methods=["GET"])
def breeze_demat_holdings(acct):
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    if acct not in breeze_icici.VALID_ACCOUNT_IDS:
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    try:
        return jsonify(breeze_icici.api_get_demat_holdings(acct))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


def _build_foreign_asset_rows(fy_year, selected_keys=None):
    """Shared row builder for the foreign-asset (Schedule FA) data.
    Returns (rows, template, error_tuple). One entry per holding lot acquired on/before FY end.
    rows[i] has template keys plus InterestAcquiringDate, InitialValOfInvstmnt, PeakBalanceDuringPeriod, ClosingBalance.
    selected_keys: optional set of lot keys "YYYY-MM-DD|TYPE|qty" to restrict the export to checked holdings.
    """
    import json as _json
    from helpers import gather_data

    fy_end = datetime(fy_year, 3, 31)

    db_obj = gather_data.DB()
    if not os.path.isfile(db_obj.db_path):
        return None, None, ("Database not found", 404)
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
            if selected_keys is not None:
                try:
                    lot_qty = int(float(row["Available_Sell"])) if row["Available_Sell"] is not None else 0
                except (TypeError, ValueError):
                    lot_qty = 0
                lot_key = f"{invest_date.strftime('%Y-%m-%d')}|{stock_type}|{lot_qty}"
                if lot_key not in selected_keys:
                    continue
            entry = dict(template)
            entry["InterestAcquiringDate"] = invest_date.strftime("%Y-%m-%d")
            entry["InitialValOfInvstmnt"] = int(round(float(datacleaner_obj.convert_from_symbol(row["InitialValue"])), 0))
            entry["PeakBalanceDuringPeriod"] = int(round(float(datacleaner_obj.convert_from_symbol(row["Max_Value_FY"])), 0))
            entry["ClosingBalance"] = int(round(float(datacleaner_obj.convert_from_symbol(row["FY_Closing_Value"])), 0))
            # Skip lots with no remaining holding at period end (nothing to report).
            if entry["ClosingBalance"] == 0:
                continue
            shares_list.append(entry)

    # Order rows by acquisition date ascending (oldest first).
    shares_list.sort(key=lambda e: e.get("InterestAcquiringDate", ""))

    return shares_list, template, None


@app.route("/api/generate-tax-doc", methods=["GET"])
def generate_tax_doc():
    """Generate the ITR foreign-asset schedule JSON from holdings data + tax_config template.
    Query param `fy` = assessment year (e.g. 2026 for AY 2026–27, FY starting 1 Apr 2025).
    Defaults to current calendar year.
    """
    fy_year = request.args.get("fy", type=int) or date.today().year
    rows, _template, err = _build_foreign_asset_rows(fy_year)
    if err is not None:
        return jsonify({"error": err[0]}), err[1]
    return jsonify({"fyLabel": f"FY {fy_year - 1}–{str(fy_year)[-2:]} (AY {fy_year}–{str(fy_year + 1)[-2:]})", "rows": rows})


# Country dropdown values in the ClearTax FA template's Help sheet (Help!B2:B251) use underscore_caps names.
_FA_A3_COUNTRY_BY_CODE = {
    "2": "UNITED_STATES_OF_AMERICA",
}
_FA_TEMPLATE_PATH = os.path.join(REPO_ROOT, "configs", "templates", "cleartax_schedule_fa.xlsx")


@app.route("/api/export-fa-a3", methods=["GET"])
def export_fa_a3():
    """Fill the FA-A3 sheet of the ClearTax Schedule FA template with holdings data and return the xlsx.
    Query param `fy` = assessment year (defaults to current calendar year).
    """
    from io import BytesIO
    import openpyxl

    fy_year = request.args.get("fy", type=int) or date.today().year
    keys_param = request.args.get("keys", type=str)
    selected_keys = set(k for k in keys_param.split(";;") if k) if keys_param else None
    rows, template, err = _build_foreign_asset_rows(fy_year, selected_keys=selected_keys)
    if err is not None:
        return jsonify({"error": err[0]}), err[1]

    if not os.path.isfile(_FA_TEMPLATE_PATH):
        return jsonify({"error": f"FA template not found at {_FA_TEMPLATE_PATH}"}), 404

    template = template or {}
    code = str(template.get("CountryCodeExcludingIndia", "2"))
    country_a3 = _FA_A3_COUNTRY_BY_CODE.get(code, "UNITED_STATES_OF_AMERICA")
    name_of_entity = template.get("NameOfEntity", "")
    address = template.get("AddressOfEntity", "")
    zip_code = template.get("ZipCode", "")
    nature = template.get("NatureOfEntity", "Shares")
    gross_amt = template.get("TotGrossAmtPaidCredited", 0)
    proceeds = template.get("TotGrossProceeds", 0)

    wb = openpyxl.load_workbook(_FA_TEMPLATE_PATH)
    ws = wb["FA- A3"]

    # Data rows start at row 3 (row 1 = title, row 2 = headers). Columns A..K.
    start_row = 3
    for i, entry in enumerate(rows):
        r = start_row + i
        acq = entry.get("InterestAcquiringDate", "")
        try:
            acq_fmt = datetime.strptime(acq, "%Y-%m-%d").strftime("%d/%m/%Y")
        except Exception:
            acq_fmt = acq
        ws.cell(row=r, column=1, value=country_a3)
        ws.cell(row=r, column=2, value=name_of_entity)
        ws.cell(row=r, column=3, value=address)
        ws.cell(row=r, column=4, value=zip_code)
        ws.cell(row=r, column=5, value=nature)
        ws.cell(row=r, column=6, value=acq_fmt)
        ws.cell(row=r, column=7, value=entry.get("InitialValOfInvstmnt", 0))
        ws.cell(row=r, column=8, value=entry.get("PeakBalanceDuringPeriod", 0))
        ws.cell(row=r, column=9, value=entry.get("ClosingBalance", 0))
        ws.cell(row=r, column=10, value=gross_amt)
        ws.cell(row=r, column=11, value=proceeds)

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    fname = f"schedule_fa_a3_FY{fy_year - 1}-{str(fy_year)[-2:]}.xlsx"
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=fname,
    )


PORT = 8080

if __name__ == "__main__":
    if not os.path.isfile(DB_PATH):
        sys.exit(f"Database not found: {DB_PATH}")
    rec = threading.Thread(target=_live_price_recorder_loop, daemon=True)
    rec.start()
    print(f"Backend API at http://127.0.0.1:{PORT} (configs at {REPO_ROOT})")
    print("Live price recorder: running in background (records every 14s when market open)")
    # use_reloader=False: pm2 manages restarts; the reloader would spawn duplicate recorder threads (duplicate ticks).
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
