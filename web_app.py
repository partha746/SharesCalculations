"""
Flask web app to view and edit nvShares.db (NSU, ESPP, SellOut, Split tables).
Run: python web_app.py
      python web_app.py --background   # run in background (PID in web_app.pid)
      python web_app.py --stop         # stop background process
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import sqlite3
import time
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

# Ensure project root is on path for helpers import
_script_dir = os.path.dirname(os.path.abspath(__file__))
if _script_dir not in sys.path:
    sys.path.insert(0, _script_dir)

from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder="web_static", static_url_path="")
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs", "nvShares.db")
# Angular build output (browser builder: dist/dashboard; application builder: dist/dashboard/browser)
_dashboard_dist_candidates = [
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard", "dist", "dashboard"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard", "dist", "dashboard", "browser"),
]


def _dashboard_dist():
    for d in _dashboard_dist_candidates:
        if os.path.isdir(d) and os.path.isfile(os.path.join(d, "index.html")):
            return d
    return None

# Table name -> list of column names (order matters for insert/update)
TABLE_COLUMNS = {
    "NSU": ["Buy_Date", "Available_Sell", "Price_Bought", "RupeeRate"],
    "ESPP": ["Buy_Date", "Available_Sell", "Price_Bought", "RupeeRate", "TDS_Price"],
    "SellOut": [
        "Sell_Date", "Buy_Date", "Qty_Sold", "Price_Bought", "Price_Sell",
        "BuyRupeeRate", "SellRupeeRate", "Type",
    ],
    "Split": ["date", "split_ratio"],
    "live_price_history": ["timestamp_ms", "live_price_usd", "usd_to_inr_rate"],
}


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def row_to_dict(row):
    return dict(row) if row else None


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/dashboard/", defaults={"path": ""})
@app.route("/dashboard/<path:path>")
def dashboard_app(path):
    """Serve the built Angular dashboard app (after ng build --base-href /dashboard/)."""
    dashboard_dist = _dashboard_dist()
    if not dashboard_dist:
        return jsonify({"error": "Dashboard not built. Run: cd dashboard && npm install && npm run build -- --base-href /dashboard/"}), 404
    if path and not path.startswith("."):
        full = os.path.join(dashboard_dist, path)
        if os.path.isfile(full):
            return send_from_directory(dashboard_dist, path)
    return send_from_directory(dashboard_dist, "index.html")


@app.route("/api/tables", methods=["GET"])
def list_tables():
    """Return list of table names."""
    with get_db() as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        names = [r[0] for r in cur.fetchall()]
    return jsonify(names)


@app.route("/api/tables/<table_name>", methods=["GET"])
def get_table_data(table_name):
    """Return all rows for the table. Each row includes rowid for updates/deletes."""
    if table_name not in TABLE_COLUMNS:
        return jsonify({"error": f"Unknown table: {table_name}"}), 400
    with get_db() as conn:
        cur = conn.execute(f"SELECT rowid, * FROM \"{table_name}\"")
        rows = [dict(r) for r in cur.fetchall()]
    return jsonify(rows)


@app.route("/api/tables/<table_name>", methods=["POST"])
def add_row(table_name):
    """Add a new row. Body: JSON object with column names as keys."""
    if table_name not in TABLE_COLUMNS:
        return jsonify({"error": f"Unknown table: {table_name}"}), 400
    cols = TABLE_COLUMNS[table_name]
    data = request.get_json() or {}
    values = []
    for c in cols:
        v = data.get(c)
        if v is None or v == "":
            values.append(None)
        else:
            values.append(v)
    placeholders = ", ".join("?" * len(cols))
    col_list = ", ".join(f'"{c}"' for c in cols)
    with get_db() as conn:
        conn.execute(f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})', values)
        rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    return jsonify({"rowid": rowid, "message": "Added"}), 201


@app.route("/api/tables/<table_name>/<int:rowid>", methods=["PUT"])
def update_row(table_name, rowid):
    """Update a row by rowid. Body: JSON object with column names to update."""
    if table_name not in TABLE_COLUMNS:
        return jsonify({"error": f"Unknown table: {table_name}"}), 400
    data = request.get_json() or {}
    cols = TABLE_COLUMNS[table_name]
    set_parts = []
    values = []
    for c in cols:
        if c in data:
            set_parts.append(f'"{c}" = ?')
            v = data[c]
            values.append(v if v != "" else None)
    if not set_parts:
        return jsonify({"error": "No columns to update"}), 400
    values.append(rowid)
    with get_db() as conn:
        cur = conn.execute(
            f'UPDATE "{table_name}" SET {", ".join(set_parts)} WHERE rowid = ?',
            values,
        )
        if cur.rowcount == 0:
            return jsonify({"error": "Row not found"}), 404
    return jsonify({"message": "Updated"})


@app.route("/api/tables/<table_name>/<int:rowid>", methods=["DELETE"])
def delete_row(table_name, rowid):
    """Delete a row by rowid."""
    if table_name not in TABLE_COLUMNS:
        return jsonify({"error": f"Unknown table: {table_name}"}), 400
    with get_db() as conn:
        cur = conn.execute(f'DELETE FROM "{table_name}" WHERE rowid = ?', (rowid,))
        if cur.rowcount == 0:
            return jsonify({"error": "Row not found"}), 404
    return jsonify({"message": "Deleted"})


# Dashboard API cache (TTL seconds)
_DASHBOARD_CACHE = {}
_DASHBOARD_CACHE_TTL = 120  # 2 minutes


def _build_dashboard_response():
    """Build dashboard JSON using helpers.gather_data. Raises on failure."""
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

    live_price, todays_rp, _ = rupee_conv_obj.get_live_price()
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
        (
            _df,
            _current_value,
            _tds_paid_on,
            total_qty,
            total_capital_gain,
            total_tds,
            avg_buy_price,
            avg_profit_percent,
        ) = gather_data.OwnStockData().generate_display_data(type=stock_type)
        all_qty += total_qty
        all_profit_after_tax += total_capital_gain
        all_tax += total_tds
        blob = {
            "qty": total_qty,
            "profitAfterTax": round(total_capital_gain, 2),
            "avgBuyPrice": round(avg_buy_price, 2),
            "avgProfitPercent": round(avg_profit_percent, 2),
        }
        if stock_type == "NSU":
            nsu = blob
        else:
            espp = blob

    total_value_usd = round(all_qty * live_price, 2) if live_price else 0
    total_value_inr = round(all_qty * live_price * todays_rp, 2) if live_price and todays_rp else 0

    # Gain cannot exceed total current value (gain = value - cost, cost >= 0)
    gain_before_tax = all_profit_after_tax + all_tax
    if gain_before_tax > total_value_inr and total_value_inr > 0:
        # Sanity cap: scale down gain and tax so before_tax <= total_value_inr
        scale = total_value_inr / gain_before_tax
        all_tax = round(all_tax * scale, 2)
        all_profit_after_tax = round(total_value_inr - all_tax, 2)
        gain_before_tax = total_value_inr

    # If you sell everything now: proceeds (total value) minus tax = amount in bank
    net_in_bank_if_sell_now = round(total_value_inr - all_tax, 2) if total_value_inr else 0

    realised_profit = None
    sold_total_qty = None
    sold_total_value_inr = None
    sold_value_rsu_inr = None
    sold_value_espp_inr = None
    if db_status["SellOut"]:
        _df_sell, sell_profit, total_qty_sold, total_sell_inr, total_sell_rsu_inr, total_sell_espp_inr = gather_data.OwnStockData().generate_sellout_display_data()
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
    """Build list of holding rows (NSU + ESPP) with buy price, type, totals, profit %, tax, etc."""
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
        # Use TDS price for ESPP, buy price for NSU
        price_col = "TDS_Price_raw" if stock_type == "ESPP" else "Price_Bought_raw"
        for _, r in df.iterrows():
            qty = int(r["Available_Sell"])
            buy_date = r.get("Buy_Date")
            if hasattr(buy_date, "isoformat"):
                buy_date_str = buy_date.isoformat()
            else:
                buy_date_str = str(buy_date) if buy_date else ""
            value_today_inr = float(r["TodaysValue_raw"])
            tax_to_pay_inr = float(r["TaxNeedtoPay_raw"])
            # If you sell this lot today: proceeds in hand = value today − tax
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
    """Parse date string or date to date object; return None if invalid."""
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
    """Build list of sold-share rows from SellOut table for Sold Shares tab."""
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


@app.route("/api/holdings", methods=["GET"])
def get_holdings():
    """Return row-level holdings (NSU + ESPP) for table view."""
    try:
        rows = _build_holdings_response()
        if rows is None:
            return jsonify({"error": "Database not found"}), 404
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 503


@app.route("/api/sold", methods=["GET"])
def get_sold():
    """Return sold shares (SellOut) for Sold Shares tab."""
    try:
        rows = _build_sold_response()
        if rows is None:
            return jsonify({"error": "Database not found"}), 404
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 503


@app.route("/api/dashboard", methods=["GET"])
def get_dashboard():
    """Return aggregated dashboard stats (live price, totals, NSU/ESPP breakdown). Cached 2 min."""
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


PID_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web_app.pid")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web_app.log")
PORT = 5000


def get_web_app_pids():
    """Return list of PIDs running web_app.py (excluding current process)."""
    my_pid = os.getpid()
    pids = []
    try:
        out = subprocess.run(
            ["pgrep", "-f", "web_app\\.py"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if out.returncode == 0 and out.stdout:
            pids = [int(x) for x in out.stdout.strip().split() if x and int(x) != my_pid]
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        pass
    if os.path.isfile(PID_FILE):
        try:
            with open(PID_FILE) as f:
                pid = int(f.read().strip())
            if pid != my_pid and pid not in pids:
                pids.append(pid)
        except (ValueError, OSError):
            pass
    return pids


def kill_web_app_processes():
    """Kill all processes running web_app.py and remove PID file."""
    pids = get_web_app_pids()
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    if pids:
        time.sleep(0.5)
    if os.path.isfile(PID_FILE):
        try:
            os.remove(PID_FILE)
        except OSError:
            pass


def port_in_use(port):
    """Return True if the given TCP port is in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", port))
            return False
        except OSError:
            return True


def daemonize():
    """Fork into background and detach from terminal."""
    if os.fork():
        sys.exit(0)
    os.setsid()
    if os.fork():
        sys.exit(0)
    # Keep working directory so paths and DB work
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))
    sys.stdin.close()
    log = open(LOG_FILE, "a", buffering=1)  # line-buffered
    sys.stdout = log
    sys.stderr = log


def stop_background():
    """Stop the background process using PID file."""
    if not os.path.isfile(PID_FILE):
        print("No PID file found; app is not running in background.")
        return
    with open(PID_FILE) as f:
        pid = int(f.read().strip())
    try:
        os.kill(pid, 15)
        os.remove(PID_FILE)
        print(f"Stopped process {pid}.")
    except ProcessLookupError:
        os.remove(PID_FILE)
        print("Process was not running; removed stale PID file.")
    except PermissionError:
        print(f"Permission denied stopping PID {pid}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="nvShares.db web app")
    parser.add_argument("-b", "--background", action="store_true", help="Run in background")
    parser.add_argument("--stop", action="store_true", help="Stop background process")
    args = parser.parse_args()

    if args.stop:
        stop_background()
        sys.exit(0)

    if not os.path.isfile(DB_PATH):
        raise SystemExit(f"Database not found: {DB_PATH}")

    # Stop any existing web_app processes and free the port
    kill_web_app_processes()
    if port_in_use(PORT):
        time.sleep(0.5)
        if port_in_use(PORT):
            print(f"Port {PORT} is still in use by another process. Free it and try again.")
            sys.exit(1)

    if args.background:
        daemonize()

    app.run(host="0.0.0.0", port=PORT, debug=not args.background)
