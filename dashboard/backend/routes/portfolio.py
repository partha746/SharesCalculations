"""/api/dashboard, /api/holdings, /api/sold, /api/mark-sold* blueprint."""
import time
from datetime import datetime

from flask import Blueprint, jsonify, request

from db import get_db
from services.portfolio import (
    _build_dashboard_response, _build_holdings_response, _build_sold_response,
    _parse_date, _MARK_SOLD_UNDO_STACK,
)

bp = Blueprint("portfolio", __name__)

_DASHBOARD_CACHE = {}


_DASHBOARD_CACHE_TTL = 120

# Finnhub API key (centralized in helpers/config; env override: FINNHUB_TOKEN)


@bp.route("/api/dashboard", methods=["GET"])
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


@bp.route("/api/holdings", methods=["GET"])
def get_holdings():
    try:
        rows = _build_holdings_response()
        if rows is None:
            return jsonify({"error": "Database not found"}), 404
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 503


@bp.route("/api/sold", methods=["GET"])
def get_sold():
    try:
        rows = _build_sold_response()
        if rows is None:
            return jsonify({"error": "Database not found"}), 404
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 503


# In-memory undo stack for last mark-sold: list of { "sellout_rowids": [...], "operations": [{ type, buy_date_str, buy_price_usd, price_bought_usd, qty }, ...] }


@bp.route("/api/mark-sold", methods=["POST"])
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
    requested_for_lot = {}
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
            # Several items in one request can resolve to the same lot; each would otherwise be
            # checked against the full availability (nothing is deducted until the insert loop
            # below), letting the combined quantity oversell it. Track the running total per lot.
            lot_id = (
                v["type"], v["_match_buy_date"], round(v["_match_price_bought"], 4),
                round(v.get("_match_tds_price", 0.0), 4),
            )
            requested_for_lot[lot_id] = requested_for_lot.get(lot_id, 0) + v["qty"]
            if avail < requested_for_lot[lot_id]:
                return jsonify({
                    "error": (
                        f"Insufficient available qty for lot {v['buy_date_str']}: "
                        f"has {avail}, need {requested_for_lot[lot_id]}"
                    ),
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


@bp.route("/api/mark-sold-can-undo", methods=["GET"])
def mark_sold_can_undo():
    """Lightweight check for undo availability (not cached with dashboard)."""
    return jsonify({"canUndo": len(_MARK_SOLD_UNDO_STACK) > 0})


@bp.route("/api/mark-sold-undo", methods=["POST"])
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

