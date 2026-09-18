"""Advance-tax payments actually made, recorded per Indian financial year.

Storage only: the quarterly schedule is derived from the sold lots and these payments, so
nothing computed is persisted and editing a sale reshapes the schedule automatically.

A payment belongs to the FY it was paid *for* (`fy_start_year`, e.g. 2026 = FY 2026-27) and
carries the date it was paid on, because which instalment it counts towards depends on that
date, not on when it was entered.
"""
from datetime import datetime

from db import get_db


def _ensure_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS advance_tax_payments (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           fy_start_year INTEGER NOT NULL,
           paid_on TEXT NOT NULL,
           amount_inr REAL NOT NULL,
           note TEXT,
           created_at TEXT NOT NULL
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_advance_tax_fy ON advance_tax_payments(fy_start_year, paid_on)"
    )


def _row_to_dict(r):
    return {
        "id": r["id"],
        "fyStartYear": r["fy_start_year"],
        "paidOn": r["paid_on"],
        "amountInr": float(r["amount_inr"]),
        "note": r["note"] or "",
        "createdAt": r["created_at"],
    }


def _parse_iso_date(value):
    text = str(value or "").strip()[:10]
    if not text:
        raise ValueError("A payment date is required")
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("Payment date must be YYYY-MM-DD")


def fy_of(d):
    """Indian FY start year for a date: Apr 2026-Mar 2027 -> 2026."""
    return d.year if d.month >= 4 else d.year - 1


def list_payments(fy_start_year=None):
    with get_db() as conn:
        _ensure_table(conn)
        if fy_start_year is None:
            rows = conn.execute(
                "SELECT id, fy_start_year, paid_on, amount_inr, note, created_at "
                "FROM advance_tax_payments ORDER BY paid_on ASC, id ASC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, fy_start_year, paid_on, amount_inr, note, created_at "
                "FROM advance_tax_payments WHERE fy_start_year = ? ORDER BY paid_on ASC, id ASC",
                (int(fy_start_year),),
            ).fetchall()
    return [_row_to_dict(r) for r in rows]


def add_payment(paid_on, amount_inr, fy_start_year=None, note=""):
    """Record one payment. The FY defaults to the one the payment date falls in."""
    day = _parse_iso_date(paid_on)
    try:
        amount = float(amount_inr)
    except (TypeError, ValueError):
        raise ValueError("A numeric amount is required")
    if amount <= 0:
        raise ValueError("Amount must be greater than 0")
    fy = int(fy_start_year) if fy_start_year not in (None, "") else fy_of(day)
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute(
            "INSERT INTO advance_tax_payments (fy_start_year, paid_on, amount_inr, note, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (fy, day.isoformat(), amount, (note or "").strip(),
             datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
        )
        return cur.lastrowid


def update_payment(payment_id, paid_on=None, amount_inr=None, note=None, fy_start_year=None):
    fields, params = [], []
    if paid_on is not None:
        fields.append("paid_on = ?")
        params.append(_parse_iso_date(paid_on).isoformat())
    if amount_inr is not None:
        try:
            amount = float(amount_inr)
        except (TypeError, ValueError):
            raise ValueError("A numeric amount is required")
        if amount <= 0:
            raise ValueError("Amount must be greater than 0")
        fields.append("amount_inr = ?")
        params.append(amount)
    if note is not None:
        fields.append("note = ?")
        params.append(str(note).strip())
    if fy_start_year not in (None, ""):
        fields.append("fy_start_year = ?")
        params.append(int(fy_start_year))
    if not fields:
        return 0
    params.append(int(payment_id))
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute(
            "UPDATE advance_tax_payments SET {} WHERE id = ?".format(", ".join(fields)), params
        )
        return cur.rowcount


def delete_payment(payment_id):
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute("DELETE FROM advance_tax_payments WHERE id = ?", (int(payment_id),))
        return cur.rowcount
