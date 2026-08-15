"""Persisted financial-planning state (a single JSON snapshot of the planner inputs).

Stored as one JSON blob per named plan (default: "default"), so the planner survives reloads and is
shared across devices on the LAN. The shape is owned by the frontend; the backend just round-trips it.
"""
import json
import time

from db import get_db


def _ensure_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS finance_plan (
           name TEXT PRIMARY KEY,
           data TEXT NOT NULL,
           updated_at TEXT NOT NULL
        )"""
    )


def get_plan(name="default"):
    with get_db() as conn:
        _ensure_table(conn)
        row = conn.execute("SELECT data, updated_at FROM finance_plan WHERE name = ?", (name,)).fetchone()
    if not row:
        return {"plan": None, "updatedAt": None}
    try:
        data = json.loads(row["data"])
    except Exception:
        data = None
    return {"plan": data, "updatedAt": row["updated_at"]}


def save_plan(data, name="default"):
    if not isinstance(data, dict):
        raise ValueError("data must be an object")
    payload = json.dumps(data)
    updated_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    with get_db() as conn:
        _ensure_table(conn)
        conn.execute(
            "INSERT INTO finance_plan (name, data, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at",
            (name, payload, updated_at),
        )
    return {"ok": True, "updatedAt": updated_at}
