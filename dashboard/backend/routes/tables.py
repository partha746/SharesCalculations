"""/api/tables CRUD blueprint."""
from flask import Blueprint, jsonify, request

from db import TABLE_COLUMNS, get_db

bp = Blueprint("tables", __name__)

@bp.route("/api/tables", methods=["GET"])
def list_tables():
    # Only expose editable user tables (TABLE_COLUMNS); internal time-series tables
    # (live_price_history, live_price_ohlc_*) are not hand-editable and are hidden here.
    with get_db() as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        existing = {r[0] for r in cur.fetchall()}
    names = [t for t in TABLE_COLUMNS if t in existing]
    return jsonify(names)


@bp.route("/api/tables/<table_name>", methods=["GET"])
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


@bp.route("/api/tables/<table_name>", methods=["POST"])
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


@bp.route("/api/tables/<table_name>/<int:rowid>", methods=["PUT"])
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


@bp.route("/api/tables/<table_name>/<int:rowid>", methods=["DELETE"])
def delete_row(table_name, rowid):
    if table_name not in TABLE_COLUMNS:
        return jsonify({"error": f"Unknown table: {table_name}"}), 400
    with get_db() as conn:
        cur = conn.execute(f'DELETE FROM "{table_name}" WHERE rowid = ?', (rowid,))
        if cur.rowcount == 0:
            return jsonify({"error": "Row not found"}), 404
    return jsonify({"message": "Deleted"})

