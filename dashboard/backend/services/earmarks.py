"""Earmarks: shares reserved (planned) for sale at a target price.

An earmark "batch" is created from a set of holding lots plus a target price: the requested total
quantity is distributed across the selected lots (the frontend computes the per-lot allocation).
Each lot allocation is stored as one row sharing a batch_id, so a batch can span several lots and a
single lot can carry multiple earmarks at different prices.

Lots are referenced by the frontend's stable lot key: "buyDate|type|price|qty|total".
"""
import time
import uuid

from db import get_db


def _ensure_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS earmarks (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           batch_id TEXT NOT NULL,
           lot_key TEXT NOT NULL,
           qty INTEGER NOT NULL,
           price_usd REAL NOT NULL,
           label TEXT,
           created_at TEXT NOT NULL
        )"""
    )


def _row_to_dict(r):
    return {
        "id": r["id"],
        "batchId": r["batch_id"],
        "lotKey": r["lot_key"],
        "qty": r["qty"],
        "priceUsd": r["price_usd"],
        "label": r["label"] or "",
        "createdAt": r["created_at"],
    }


def list_earmarks():
    with get_db() as conn:
        _ensure_table(conn)
        rows = conn.execute(
            "SELECT id, batch_id, lot_key, qty, price_usd, label, created_at FROM earmarks ORDER BY created_at DESC, id ASC"
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def add_earmarks(price_usd, allocations, label=""):
    """Create one earmark batch. `allocations` = [{lotKey, qty}, ...] (qty > 0). Returns {batchId, count}."""
    try:
        price = float(price_usd)
    except (TypeError, ValueError):
        raise ValueError("A numeric target price is required")
    if price <= 0:
        raise ValueError("Target price must be greater than 0")

    clean = []
    for a in allocations or []:
        lot_key = (a.get("lotKey") or "").strip()
        try:
            qty = int(a.get("qty"))
        except (TypeError, ValueError):
            qty = 0
        if lot_key and qty > 0:
            clean.append((lot_key, qty))
    if not clean:
        raise ValueError("Select at least one lot with a quantity to earmark")

    batch_id = uuid.uuid4().hex
    created_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    lbl = (label or "").strip()
    with get_db() as conn:
        _ensure_table(conn)
        conn.executemany(
            "INSERT INTO earmarks (batch_id, lot_key, qty, price_usd, label, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            [(batch_id, lot_key, qty, price, lbl, created_at) for (lot_key, qty) in clean],
        )
    return {"batchId": batch_id, "count": len(clean)}


def delete_batch(batch_id):
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute("DELETE FROM earmarks WHERE batch_id = ?", (batch_id,))
        return cur.rowcount


def delete_earmark(earmark_id):
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute("DELETE FROM earmarks WHERE id = ?", (earmark_id,))
        return cur.rowcount
