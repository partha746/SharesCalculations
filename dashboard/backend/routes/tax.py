"""/api/tax-config + /api/generate-tax-doc + /api/export-fa-a3 blueprint."""
import os
from datetime import date, datetime

from flask import Blueprint, jsonify, request, send_file

from services.tax_export import (
    _TAX_CONFIG_PATH, _FA_A3_COUNTRY_BY_CODE, _FA_TEMPLATE_PATH, _build_foreign_asset_rows,
)

bp = Blueprint("tax", __name__)

@bp.route("/api/tax-config", methods=["GET"])
def get_tax_config():
    import json as _json

    if not os.path.isfile(_TAX_CONFIG_PATH):
        return jsonify({"error": "tax_config.json not found"}), 404
    try:
        with open(_TAX_CONFIG_PATH, "r") as f:
            data = _json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/tax-config", methods=["PUT"])
def put_tax_config():
    import json as _json

    body = request.get_json()
    if not body:
        return jsonify({"error": "JSON body required"}), 400
    try:
        with open(_TAX_CONFIG_PATH, "w") as f:
            _json.dump(body, f, indent=2)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- ICICI Direct Breeze (optional; pip install breeze-connect + env BREEZE_API_KEY / BREEZE_API_SECRET) ---


@bp.route("/api/generate-tax-doc", methods=["GET"])
def generate_tax_doc():
    """Generate the ITR foreign-asset schedule JSON from holdings data + tax_config template.
    Query param `fy` = assessment year (e.g. 2026 for AY 2026–27, FY starting 1 Apr 2025).
    Defaults to current calendar year.
    """
    fy_year = request.args.get("fy", type=int) or date.today().year
    rows, _template, err = _build_foreign_asset_rows(fy_year)
    if err is not None:
        return jsonify({"error": err[0]}), err[1]
    return jsonify({"fyLabel": f"FY {fy_year - 1}–{str(fy_year)[-2:]} (AY {fy_year}–{str(fy_year + 1)[-2:]})", "rows": rows})


# Country dropdown values in the ClearTax FA template's Help sheet (Help!B2:B251) use underscore_caps names.


@bp.route("/api/export-fa-a3", methods=["GET"])
def export_fa_a3():
    """Fill the FA-A3 sheet of the ClearTax Schedule FA template with holdings data and return the xlsx.
    Query param `fy` = assessment year (defaults to current calendar year).
    """
    from io import BytesIO
    import openpyxl

    fy_year = request.args.get("fy", type=int) or date.today().year
    keys_param = request.args.get("keys", type=str)
    selected_keys = set(k for k in keys_param.split(";;") if k) if keys_param else None
    rows, template, err = _build_foreign_asset_rows(fy_year, selected_keys=selected_keys)
    if err is not None:
        return jsonify({"error": err[0]}), err[1]

    if not os.path.isfile(_FA_TEMPLATE_PATH):
        return jsonify({"error": f"FA template not found at {_FA_TEMPLATE_PATH}"}), 404

    template = template or {}
    code = str(template.get("CountryCodeExcludingIndia", "2"))
    country_a3 = _FA_A3_COUNTRY_BY_CODE.get(code, "UNITED_STATES_OF_AMERICA")
    name_of_entity = template.get("NameOfEntity", "")
    address = template.get("AddressOfEntity", "")
    zip_code = template.get("ZipCode", "")
    nature = template.get("NatureOfEntity", "Shares")
    gross_amt = template.get("TotGrossAmtPaidCredited", 0)
    proceeds = template.get("TotGrossProceeds", 0)

    wb = openpyxl.load_workbook(_FA_TEMPLATE_PATH)
    ws = wb["FA- A3"]

    # Data rows start at row 3 (row 1 = title, row 2 = headers). Columns A..K.
    start_row = 3
    for i, entry in enumerate(rows):
        r = start_row + i
        acq = entry.get("InterestAcquiringDate", "")
        try:
            acq_fmt = datetime.strptime(acq, "%Y-%m-%d").strftime("%d/%m/%Y")
        except Exception:
            acq_fmt = acq
        ws.cell(row=r, column=1, value=country_a3)
        ws.cell(row=r, column=2, value=name_of_entity)
        ws.cell(row=r, column=3, value=address)
        ws.cell(row=r, column=4, value=zip_code)
        ws.cell(row=r, column=5, value=nature)
        ws.cell(row=r, column=6, value=acq_fmt)
        ws.cell(row=r, column=7, value=entry.get("InitialValOfInvstmnt", 0))
        ws.cell(row=r, column=8, value=entry.get("PeakBalanceDuringPeriod", 0))
        ws.cell(row=r, column=9, value=entry.get("ClosingBalance", 0))
        ws.cell(row=r, column=10, value=gross_amt)
        ws.cell(row=r, column=11, value=proceeds)

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    fname = f"schedule_fa_a3_FY{fy_year - 1}-{str(fy_year)[-2:]}.xlsx"
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=fname,
    )

