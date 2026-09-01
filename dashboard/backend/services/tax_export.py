"""Tax config path + ITR foreign-asset (Schedule FA) row builder."""
import math
import os
from datetime import date, datetime

from helpers import config

REPO_ROOT = config.REPO_ROOT

_TAX_CONFIG_PATH = os.path.join(REPO_ROOT, "configs", "tax_config.json")


def default_assessment_year(today=None):
    """Assessment year of the *current* Indian financial year (Apr–Mar).

    The Indian FY rolls over on 1 April, so the calendar year alone is off by one from April
    onward: August 2026 sits in FY 2026–27, whose AY is 2027 (period ending 31 Mar 2027), not 2026.
    """
    d = today or date.today()
    return d.year + 1 if d.month >= 4 else d.year


def _lot_export_key(row, stock_type, invest_date):
    """Identify one holding lot as "YYYY-MM-DD|TYPE|price|qty|totalInr".

    Must stay byte-identical to holdingExportKey() in the Angular component, which builds the same
    string from the /api/holdings payload — hence the same source columns (ESPP is priced off
    TDS_Price, matching buyPriceUsd) and the same fixed formatting.
    """
    price_col = "TDS_Price_raw" if stock_type == "ESPP" else "Price_Bought_raw"
    try:
        price = float(row[price_col])
    except (TypeError, ValueError, KeyError):
        price = 0.0
    try:
        qty = int(float(row["Available_Sell"])) if row["Available_Sell"] is not None else 0
    except (TypeError, ValueError):
        qty = 0
    try:
        # floor(x + 0.5) matches JS Math.round for the positive values involved; Python's round()
        # is banker's rounding and would disagree on exact .5.
        total = int(math.floor(float(row["InitialValue_raw"]) + 0.5))
    except (TypeError, ValueError, KeyError):
        total = 0
    return f"{invest_date.strftime('%Y-%m-%d')}|{stock_type}|{price:.4f}|{qty}|{total}"


def _build_foreign_asset_rows(fy_year, selected_keys=None):
    """Shared row builder for the foreign-asset (Schedule FA) data.
    Returns (rows, template, error_tuple). One entry per holding lot acquired on/before FY end.
    rows[i] has template keys plus InterestAcquiringDate, InitialValOfInvstmnt, PeakBalanceDuringPeriod, ClosingBalance.
    selected_keys: optional set of lot keys "YYYY-MM-DD|TYPE|price|qty|totalInr" (see _lot_export_key)
    to restrict the export to checked holdings.
    """
    import json as _json
    from helpers import gather_data

    fy_end = datetime(fy_year, 3, 31)

    db_obj = gather_data.DB()
    if not os.path.isfile(db_obj.db_path):
        return None, None, ("Database not found", 404)
    db_obj.ensure_tables()
    db_status = db_obj.check_for_empty_db()
    rupee_conv_obj = gather_data.RupeeConv()
    if db_status.get("NSU"):
        rupee_conv_obj.update_null_rupees_rate("NSU", "Buy_Date", "RupeeRate")
    if db_status.get("ESPP"):
        rupee_conv_obj.update_null_rupees_rate("ESPP", "Buy_Date", "RupeeRate")

    template = {}
    try:
        with open(_TAX_CONFIG_PATH, "r") as f:
            cfg = _json.load(f)
        template = cfg.get("foreignAssetTemplate", {})
    except Exception:
        pass

    datacleaner_obj = gather_data.DataCleaner()
    shares_list = []

    unpriced = []
    for stock_type in ("NSU", "ESPP"):
        if not db_status.get(stock_type):
            continue
        stock_data = gather_data.OwnStockData()
        df, *_ = stock_data.generate_display_data(type=stock_type)
        for _, row in df.iterrows():
            invest_date = datetime.strptime(row["Buy_Date_formatted"], "%d/%m/%Y")
            if invest_date > fy_end:
                continue
            if selected_keys is not None:
                if _lot_export_key(row, stock_type, invest_date) not in selected_keys:
                    continue
            if bool(row.get("Price_Data_Missing")):
                unpriced.append(f"{stock_type} {invest_date.strftime('%Y-%m-%d')}")
                continue
            entry = dict(template)
            entry["InterestAcquiringDate"] = invest_date.strftime("%Y-%m-%d")
            entry["InitialValOfInvstmnt"] = int(round(float(datacleaner_obj.convert_from_symbol(row["InitialValue"])), 0))
            entry["PeakBalanceDuringPeriod"] = int(round(float(datacleaner_obj.convert_from_symbol(row["Max_Value_FY"])), 0))
            entry["ClosingBalance"] = int(round(float(datacleaner_obj.convert_from_symbol(row["FY_Closing_Value"])), 0))
            # Skip lots with no remaining holding at period end (nothing to report).
            if entry["ClosingBalance"] == 0:
                continue
            shares_list.append(entry)

    # A lot is dropped above when its closing balance is zero, which normally means it was
    # fully sold. An unpriced lot looks identical, so rather than hand back a quietly short
    # filing we fail when a lot that belongs in this year could not be priced.
    if unpriced:
        return None, None, (
            "No historical price data for {}. Peak and closing balances would be zero and "
            "those lots would be silently dropped, so the export was blocked. The provider "
            "rejected the request — check FMV_API_KEY in dashboard/.env.".format(", ".join(unpriced)),
            503,
        )

    # Order rows by acquisition date ascending (oldest first).
    shares_list.sort(key=lambda e: e.get("InterestAcquiringDate", ""))

    return shares_list, template, None


_FA_A3_COUNTRY_BY_CODE = {
    "2": "UNITED_STATES_OF_AMERICA",
}


_FA_TEMPLATE_PATH = os.path.join(REPO_ROOT, "configs", "templates", "cleartax_schedule_fa.xlsx")

