"""Market hours, USD/INR helpers, OHLC rollups, and the background price recorder."""
import time
from datetime import date, datetime

from helpers import config
from db import get_db

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

_FINNHUB_TOKEN = config.FINNHUB_TOKEN


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

