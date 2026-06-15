"""Tax config path + ITR foreign-asset (Schedule FA) row builder."""
import os
from datetime import date, datetime

from helpers import config

REPO_ROOT = config.REPO_ROOT

_TAX_CONFIG_PATH = os.path.join(REPO_ROOT, "configs", "tax_config.json")


def _build_foreign_asset_rows(fy_year, selected_keys=None):
    """Shared row builder for the foreign-asset (Schedule FA) data.
    Returns (rows, template, error_tuple). One entry per holding lot acquired on/before FY end.
    rows[i] has template keys plus InterestAcquiringDate, InitialValOfInvstmnt, PeakBalanceDuringPeriod, ClosingBalance.
    selected_keys: optional set of lot keys "YYYY-MM-DD|TYPE|qty" to restrict the export to checked holdings.
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

    for stock_type in ("NSU", "ESPP"):
        if not db_status.get(stock_type):
            continue
        df, *_ = gather_data.OwnStockData().generate_display_data(type=stock_type)
        for _, row in df.iterrows():
            invest_date = datetime.strptime(row["Buy_Date_formatted"], "%d/%m/%Y")
            if invest_date > fy_end:
                continue
            if selected_keys is not None:
                try:
                    lot_qty = int(float(row["Available_Sell"])) if row["Available_Sell"] is not None else 0
                except (TypeError, ValueError):
                    lot_qty = 0
                lot_key = f"{invest_date.strftime('%Y-%m-%d')}|{stock_type}|{lot_qty}"
                if lot_key not in selected_keys:
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

    # Order rows by acquisition date ascending (oldest first).
    shares_list.sort(key=lambda e: e.get("InterestAcquiringDate", ""))

    return shares_list, template, None


_FA_A3_COUNTRY_BY_CODE = {
    "2": "UNITED_STATES_OF_AMERICA",
}


_FA_TEMPLATE_PATH = os.path.join(REPO_ROOT, "configs", "templates", "cleartax_schedule_fa.xlsx")

