"""Builders for the dashboard, holdings, and sold responses."""
import math
import os
from datetime import date, datetime

from db import get_db
from services.market import (
    _ensure_live_price_history_table, _get_previous_day_inr_rate,
    _is_premarket_et, _is_postmarket_et,
)

_MARK_SOLD_UNDO_STACK = []


def _safe_float(v, default=0.0):
    """Convert to float, replacing NaN/Infinity with *default* so JSON stays valid."""
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


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

    live_price, todays_rp, open_price, prev_close = rupee_conv_obj.get_live_price()
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
            "avgProfitPercent": round(avg_profit_percent, 1),
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

    payload = {
        "livePriceUsd": round(live_price, 2),
        "usdToInrRate": round(float(todays_rp), 4),
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
        "canUndoMarkSold": len(_MARK_SOLD_UNDO_STACK) > 0,
    }
    if open_price is not None:
        payload["openPriceUsd"] = round(open_price, 2)
    if prev_close is not None:
        # Prefer Frankfurter historical (same family as live INR) for "yesterday's" rate; then DB; else today.
        prev_inr_rate = (
            rupee_conv_obj.get_usd_to_inr_for_prior_calendar_day()
            or _get_previous_day_inr_rate()
            or todays_rp
        )
        payload["previousCloseUsd"] = round(prev_close, 2)
        payload["previousCloseValueUsd"] = round(all_qty * prev_close, 2)
        payload["previousCloseValueInr"] = round(all_qty * prev_close * prev_inr_rate, 2)
        payload["previousCloseUsdToInrRate"] = round(prev_inr_rate, 2)
    if _is_premarket_et():
        premarket = rupee_conv_obj.get_premarket_price("NVDA")
        if premarket is not None:
            payload["preMarketPriceUsd"] = round(premarket, 2)
    if _is_postmarket_et():
        postmarket = rupee_conv_obj.get_postmarket_price("NVDA")
        if postmarket is not None:
            payload["postMarketPriceUsd"] = round(postmarket, 2)
    return payload


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
            try:
                qty = int(float(r["Available_Sell"])) if r["Available_Sell"] is not None else 0
            except (TypeError, ValueError):
                qty = 0
            if qty <= 0:
                continue
            buy_date = r.get("Buy_Date")
            buy_date_str = buy_date.isoformat() if hasattr(buy_date, "isoformat") else (str(buy_date) if buy_date else "")
            value_today_inr = _safe_float(r["TodaysValue_raw"])
            tax_to_pay_inr = _safe_float(r["TaxNeedtoPay_raw"])
            net_if_sell_today_inr = value_today_inr - tax_to_pay_inr
            tax_slab_pct = _safe_float(r.get("TaxSlab", 0) * 100) if "TaxSlab" in r else 0
            row_data = {
                "type": stock_type,
                "buyDate": buy_date_str,
                "qty": qty,
                "buyPriceUsd": _safe_float(r[price_col]),
                "totalPurchaseInr": _safe_float(r["InitialValue_raw"]),
                "netIfSellTodayInr": net_if_sell_today_inr,
                "profitPercent": round(_safe_float(r["ProfitPercent"]), 1),
                "taxToPayInr": tax_to_pay_inr,
                "taxPercent": tax_slab_pct,
            }
            if stock_type == "ESPP":
                row_data["priceBoughtUsd"] = _safe_float(r["Price_Bought_raw"])
            rows.append(row_data)
    return rows


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
        try:
            qty = int(float(r.get("Qty_Sold") or 0))
        except (TypeError, ValueError):
            qty = 0
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
        profit_pct = round((gain_before_tax - tax_paid) / buy_at_r * 100, 1) if buy_at_r else 0
        rows.append({
            "sellDate": sell_date.isoformat() if sell_date else "",
            "buyDate": buy_date.isoformat() if buy_date else "",
            "qtySold": qty,
            "type": typ,
            "priceBoughtUsd": price_bought,
            "priceSellUsd": price_sell,
            "buyValueInr": buy_at_r,
            "sellValueInr": sell_at_r,
            "gainBeforeTaxInr": gain_before_tax,
            "taxPaidInr": tax_paid,
            "profitPercent": profit_pct,
            "taxPercent": tax_slab * 100,
        })
    return rows

