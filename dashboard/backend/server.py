"""
Flask API backend for the Angular dashboard (nvShares.db).
Run from dashboard folder: python backend/server.py
Uses repo root (parent of dashboard) for configs/ and helpers/.
"""
import os
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime

# Repo root = parent of dashboard (dashboard/backend/server.py -> backend -> dashboard -> repo)
_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
_DASHBOARD_DIR = os.path.dirname(_BACKEND_DIR)
REPO_ROOT = os.path.dirname(_DASHBOARD_DIR)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from flask import Flask, jsonify, request

app = Flask(__name__)
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

    live_price, todays_rp = rupee_conv_obj.get_live_price()
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
            "avgProfitPercent": round(avg_profit_percent, 2),
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

    return {
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
    }


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
            qty = int(r["Available_Sell"])
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
                "buyPriceUsd": round(float(r[price_col]), 2),
                "totalPurchaseInr": round(float(r["InitialValue_raw"]), 2),
                "netIfSellTodayInr": round(net_if_sell_today_inr, 2),
                "profitPercent": round(float(r["ProfitPercent"]), 2),
                "taxToPayInr": round(tax_to_pay_inr, 2),
                "taxPercent": round(tax_slab_pct, 1),
            }
            if stock_type == "ESPP":
                row_data["priceBoughtUsd"] = round(float(r["Price_Bought_raw"]), 2)
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
        qty = int(r.get("Qty_Sold") or 0)
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
        profit_pct = round((gain_before_tax - tax_paid) / buy_at_r * 100, 2) if buy_at_r else 0
        rows.append({
            "sellDate": sell_date.isoformat() if sell_date else "",
            "buyDate": buy_date.isoformat() if buy_date else "",
            "qtySold": qty,
            "type": typ,
            "priceBoughtUsd": round(price_bought, 2),
            "priceSellUsd": round(price_sell, 2),
            "buyValueInr": round(buy_at_r, 2),
            "sellValueInr": round(sell_at_r, 2),
            "gainBeforeTaxInr": round(gain_before_tax, 2),
            "taxPaidInr": round(tax_paid, 2),
            "profitPercent": round(profit_pct, 2),
            "taxPercent": round(tax_slab * 100, 1),
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


PORT = 8080

if __name__ == "__main__":
    if not os.path.isfile(DB_PATH):
        sys.exit(f"Database not found: {DB_PATH}")
    print(f"Backend API at http://127.0.0.1:{PORT} (configs at {REPO_ROOT})")
    app.run(host="0.0.0.0", port=PORT, debug=True)
