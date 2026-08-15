"""Manual net-worth line items.

Assets the app already tracks (NVDA position, mutual funds, ICICI equity) are computed live on the
frontend from existing data, so only the things we cannot know — bank balances, property, gold,
loans, etc. — are stored here. Each row carries a liquidity bucket (liquid / illiquid) and a kind
(asset / liability) so the tab can show a true net worth alongside a liquidity split.
"""
import time

from db import get_db

LIQUIDITY = ("liquid", "illiquid")
KINDS = ("asset", "liability")


def _ensure_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS networth_items (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           label TEXT NOT NULL,
           category TEXT NOT NULL DEFAULT 'Other',
           liquidity TEXT NOT NULL DEFAULT 'liquid',
           kind TEXT NOT NULL DEFAULT 'asset',
           value_inr REAL NOT NULL DEFAULT 0,
           note TEXT,
           updated_at TEXT NOT NULL
        )"""
    )


def _row_to_dict(r):
    return {
        "id": r["id"],
        "label": r["label"],
        "category": r["category"],
        "liquidity": r["liquidity"],
        "kind": r["kind"],
        "valueInr": r["value_inr"],
        "note": r["note"] or "",
        "updatedAt": r["updated_at"],
    }


def _clean(label, category, liquidity, kind, value_inr, note):
    lbl = (label or "").strip()
    if not lbl:
        raise ValueError("A label is required")
    cat = (category or "Other").strip() or "Other"
    liq = (liquidity or "liquid").strip().lower()
    if liq not in LIQUIDITY:
        raise ValueError(f"liquidity must be one of {LIQUIDITY}")
    knd = (kind or "asset").strip().lower()
    if knd not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}")
    try:
        val = float(value_inr or 0)
    except (TypeError, ValueError):
        raise ValueError("value must be a number")
    return lbl, cat, liq, knd, val, (note or "").strip()


def list_items():
    with get_db() as conn:
        _ensure_table(conn)
        rows = conn.execute(
            "SELECT id, label, category, liquidity, kind, value_inr, note, updated_at "
            "FROM networth_items ORDER BY kind ASC, category ASC, id ASC"
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def add_item(label, category="Other", liquidity="liquid", kind="asset", value_inr=0, note=""):
    lbl, cat, liq, knd, val, nte = _clean(label, category, liquidity, kind, value_inr, note)
    updated_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute(
            "INSERT INTO networth_items (label, category, liquidity, kind, value_inr, note, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (lbl, cat, liq, knd, val, nte, updated_at),
        )
        return cur.lastrowid


def update_item(item_id, label, category, liquidity, kind, value_inr, note=""):
    lbl, cat, liq, knd, val, nte = _clean(label, category, liquidity, kind, value_inr, note)
    updated_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute(
            "UPDATE networth_items SET label = ?, category = ?, liquidity = ?, kind = ?, "
            "value_inr = ?, note = ?, updated_at = ? WHERE id = ?",
            (lbl, cat, liq, knd, val, nte, updated_at, item_id),
        )
        return cur.rowcount


def delete_item(item_id):
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute("DELETE FROM networth_items WHERE id = ?", (item_id,))
        return cur.rowcount
