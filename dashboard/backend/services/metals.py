"""Gold/silver rate history, holdings lots and valuation.

Rates come from `helpers/metals.py` (Pune, GoodReturns). Snapshots are appended to
`metal_price_history` so the tab can draw charts and a day range the way the NVDA side does;
holdings are stored as purchase lots so gain is computed against what was actually paid
rather than a blended average.
"""
from datetime import datetime

from db import get_db

# One row per series, so charts and history queries stay a single column lookup.
METAL_KEYS = ("gold24k", "gold22k", "gold18k", "silver")
METAL_LABELS = {
    "gold24k": "Gold 24K",
    "gold22k": "Gold 22K",
    "gold18k": "Gold 18K",
    "silver": "Silver",
}


def _ensure_tables(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS metal_price_history (
           timestamp_ms INTEGER NOT NULL,
           metal TEXT NOT NULL,
           price_per_gram REAL NOT NULL,
           source TEXT
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_metal_price_hist ON metal_price_history(metal, timestamp_ms)"
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS metal_holdings (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           metal TEXT NOT NULL,
           grams REAL NOT NULL,
           price_paid_per_gram REAL NOT NULL,
           buy_date TEXT NOT NULL,
           note TEXT,
           created_at TEXT NOT NULL
        )"""
    )


def _rates_to_series(rates):
    """Flatten the helper's shape into {metal_key: price_per_gram}."""
    if not rates:
        return {}
    out = {}
    for karat, value in (rates.get("gold") or {}).items():
        key = "gold" + karat
        if key in METAL_KEYS and value:
            out[key] = float(value)
    if rates.get("silver"):
        out["silver"] = float(rates["silver"])
    return out


def record_rates():
    """Append one snapshot per series. Called by the background recorder."""
    from helpers import metals as metals_helper

    rates = metals_helper.get_rates()
    series = _rates_to_series(rates)
    if not series:
        return 0
    ts = int(datetime.now().timestamp() * 1000)
    source = rates.get("source") or ""
    with get_db() as conn:
        _ensure_tables(conn)
        # Skip when nothing moved since the last snapshot, so the table does not fill with
        # identical rows overnight and the charts stay readable.
        written = 0
        for metal, price in series.items():
            last = conn.execute(
                "SELECT price_per_gram FROM metal_price_history WHERE metal = ? "
                "ORDER BY timestamp_ms DESC LIMIT 1",
                (metal,),
            ).fetchone()
            if last and abs(float(last[0]) - price) < 0.005:
                continue
            conn.execute(
                "INSERT INTO metal_price_history (timestamp_ms, metal, price_per_gram, source) "
                "VALUES (?, ?, ?, ?)",
                (ts, metal, price, source),
            )
            written += 1
    return written


def _day_stats(conn, metal, since_ms):
    row = conn.execute(
        "SELECT MIN(price_per_gram), MAX(price_per_gram), COUNT(*) FROM metal_price_history "
        "WHERE metal = ? AND timestamp_ms >= ?",
        (metal, since_ms),
    ).fetchone()
    if not row or row[2] == 0:
        return None, None, 0
    return float(row[0]), float(row[1]), int(row[2])


def _previous_close(conn, metal, since_ms):
    """Last recorded price from before the window — the baseline for the day's change."""
    row = conn.execute(
        "SELECT price_per_gram FROM metal_price_history WHERE metal = ? AND timestamp_ms < ? "
        "ORDER BY timestamp_ms DESC LIMIT 1",
        (metal, since_ms),
    ).fetchone()
    return float(row[0]) if row else None


def current():
    """Live rates plus day range and change, per series."""
    from helpers import metals as metals_helper

    rates = metals_helper.get_rates()
    series = _rates_to_series(rates)
    now_ms = int(datetime.now().timestamp() * 1000)
    day_start_ms = now_ms - 86400000

    out = []
    with get_db() as conn:
        _ensure_tables(conn)
        for metal in METAL_KEYS:
            price = series.get(metal)
            low, high, samples = _day_stats(conn, metal, day_start_ms)
            prev = _previous_close(conn, metal, day_start_ms)
            change = round(price - prev, 2) if (price and prev) else None
            out.append({
                "metal": metal,
                "label": METAL_LABELS[metal],
                "pricePerGram": round(price, 2) if price else None,
                "pricePer10g": round(price * 10, 2) if price else None,
                "pricePerKg": round(price * 1000, 2) if price else None,
                "dayLow": round(low, 2) if low else None,
                "dayHigh": round(high, 2) if high else None,
                "samples": samples,
                "prevClose": round(prev, 2) if prev else None,
                "change": change,
                "changePct": round(change / prev * 100, 2) if (change is not None and prev) else None,
            })
    return {
        "source": (rates or {}).get("source"),
        "city": (rates or {}).get("city"),
        "fetchedAt": (rates or {}).get("fetchedAt"),
        "stale": bool((rates or {}).get("stale")),
        # The source states these are "indicative and do not include GST, TCS and other
        # levies", so they are the bare metal rate. Import duty is already inside any Indian
        # rate and cannot be stripped out; only international spot sits below this.
        "excludesGst": True,
        "excludesMakingCharges": True,
        "basis": "Bare metal rate, before GST and making charges",
        "metals": out,
    }


def history(metal, from_ms=None, to_ms=None, max_points=1500):
    """Recorded price points for one series, thinned to max_points."""
    if metal not in METAL_KEYS:
        raise ValueError("Unknown metal: {}".format(metal))
    now_ms = int(datetime.now().timestamp() * 1000)
    end = int(to_ms) if to_ms else now_ms
    start = int(from_ms) if from_ms else end - 30 * 86400000
    if start > end:
        start, end = end, start
    with get_db() as conn:
        _ensure_tables(conn)
        rows = conn.execute(
            "SELECT timestamp_ms, price_per_gram FROM metal_price_history "
            "WHERE metal = ? AND timestamp_ms BETWEEN ? AND ? ORDER BY timestamp_ms ASC",
            (metal, start, end),
        ).fetchall()
    points = [{"timestamp": int(r[0]), "pricePerGram": float(r[1])} for r in rows]
    if len(points) > max_points:
        step = len(points) / max_points
        thinned = [points[int(i * step)] for i in range(max_points)]
        thinned[-1] = points[-1]  # always keep the latest
        points = thinned
    return points


# --- Holdings (purchase lots) ---

def _holding_row(r):
    return {
        "id": r["id"],
        "metal": r["metal"],
        "label": METAL_LABELS.get(r["metal"], r["metal"]),
        "grams": float(r["grams"]),
        "pricePaidPerGram": float(r["price_paid_per_gram"]),
        "buyDate": r["buy_date"],
        "note": r["note"] or "",
    }


def list_holdings():
    """Lots enriched with current value and gain against what was actually paid."""
    live = {m["metal"]: m["pricePerGram"] for m in current()["metals"]}
    with get_db() as conn:
        _ensure_tables(conn)
        rows = conn.execute(
            "SELECT id, metal, grams, price_paid_per_gram, buy_date, note FROM metal_holdings "
            "ORDER BY buy_date ASC, id ASC"
        ).fetchall()
    out, totals = [], {"invested": 0.0, "value": 0.0}
    for r in rows:
        h = _holding_row(r)
        price = live.get(h["metal"])
        invested = h["grams"] * h["pricePaidPerGram"]
        value = h["grams"] * price if price else None
        h["investedInr"] = round(invested, 2)
        h["currentPricePerGram"] = round(price, 2) if price else None
        h["valueInr"] = round(value, 2) if value is not None else None
        h["gainInr"] = round(value - invested, 2) if value is not None else None
        h["gainPct"] = round((value - invested) / invested * 100, 2) if (value is not None and invested) else None
        totals["invested"] += invested
        if value is not None:
            totals["value"] += value
        out.append(h)
    gain = totals["value"] - totals["invested"]
    return {
        "holdings": out,
        "totals": {
            "investedInr": round(totals["invested"], 2),
            "valueInr": round(totals["value"], 2),
            "gainInr": round(gain, 2),
            "gainPct": round(gain / totals["invested"] * 100, 2) if totals["invested"] else None,
            "grams": round(sum(h["grams"] for h in out), 3),
        },
    }


def add_holding(metal, grams, price_paid_per_gram, buy_date, note=""):
    if metal not in METAL_KEYS:
        raise ValueError("Pick a metal: {}".format(", ".join(METAL_KEYS)))
    try:
        g = float(grams)
        paid = float(price_paid_per_gram)
    except (TypeError, ValueError):
        raise ValueError("Grams and price paid must be numbers")
    if g <= 0:
        raise ValueError("Grams must be greater than 0")
    if paid <= 0:
        raise ValueError("Price paid must be greater than 0")
    day = str(buy_date or "").strip()[:10]
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Buy date must be YYYY-MM-DD")
    with get_db() as conn:
        _ensure_tables(conn)
        cur = conn.execute(
            "INSERT INTO metal_holdings (metal, grams, price_paid_per_gram, buy_date, note, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (metal, g, paid, day, (note or "").strip(),
             datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
        )
        return cur.lastrowid


def update_holding(holding_id, metal=None, grams=None, price_paid_per_gram=None,
                   buy_date=None, note=None):
    """Patch one lot. Only the fields supplied are touched."""
    fields, params = [], []
    if metal is not None:
        if metal not in METAL_KEYS:
            raise ValueError("Pick a metal: {}".format(", ".join(METAL_KEYS)))
        fields.append("metal = ?")
        params.append(metal)
    if grams is not None:
        try:
            g = float(grams)
        except (TypeError, ValueError):
            raise ValueError("Grams must be a number")
        if g <= 0:
            raise ValueError("Grams must be greater than 0")
        fields.append("grams = ?")
        params.append(g)
    if price_paid_per_gram is not None:
        try:
            paid = float(price_paid_per_gram)
        except (TypeError, ValueError):
            raise ValueError("Price paid must be a number")
        if paid <= 0:
            raise ValueError("Price paid must be greater than 0")
        fields.append("price_paid_per_gram = ?")
        params.append(paid)
    if buy_date is not None:
        day = str(buy_date).strip()[:10]
        try:
            datetime.strptime(day, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Buy date must be YYYY-MM-DD")
        fields.append("buy_date = ?")
        params.append(day)
    if note is not None:
        fields.append("note = ?")
        params.append(str(note).strip())
    if not fields:
        return 0
    params.append(int(holding_id))
    with get_db() as conn:
        _ensure_tables(conn)
        cur = conn.execute(
            "UPDATE metal_holdings SET {} WHERE id = ?".format(", ".join(fields)), params
        )
        return cur.rowcount


def delete_holding(holding_id):
    with get_db() as conn:
        _ensure_tables(conn)
        cur = conn.execute("DELETE FROM metal_holdings WHERE id = ?", (int(holding_id),))
        return cur.rowcount
