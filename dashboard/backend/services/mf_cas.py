"""Folio numbers and realised capital gains from a consolidated account statement (CAS).

A detailed CAS from CAMS or KFintech lists every folio one PAN holds across all fund houses, with each
transaction since the start date chosen when requesting it. Summary CAS and NSDL/CDSL statements carry
holdings only. casparser reads the PDF offline.

The broker CSV stays the record for units, cost and XIRR: a statement only fills in folio numbers on
funds already in the list, and its transactions are kept (one detailed statement per account, without
PAN, contact details or nominees) to work out capital gains per financial year.
"""
import io
import json
import re
import time
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal

import requests

from db import get_db
from services import mf as mf_service

MAX_PDF_BYTES = 20 * 1024 * 1024
# Less than this is what a full redemption leaves behind through rounding.
_MIN_UNITS = Decimal("0.001")
# casparser's own test for a statement that starts after the first purchase.
_OPEN_UNITS_LIMIT = Decimal("0.01")
_ACCOUNT_RE = re.compile(r"^[A-Za-z0-9_-]{1,16}$")

# Holding-period rules for realised gains (Income-tax Act, as amended by the Finance Acts 2023 and 2024).
_GRANDFATHER_BUY_CUTOFF = date(2018, 1, 31)
_EQUITY_LTCG_TAXED_FROM = date(2018, 4, 1)
_DEBT_DEEMED_SHORT_FROM = date(2023, 4, 1)
_NEW_REGIME_FROM = date(2024, 7, 23)

_AMFI_NAV_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"
_AMFI_TTL_SEC = 24 * 3600
_amfi_isin_cache = {"at": 0.0, "codes": {}}

# (account, imported_at) -> parsed statement / computed gains.
_statement_cache = {}
_gains_cache = {}


def _ensure_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS mf_cas_statements (
           account TEXT PRIMARY KEY,
           imported_at REAL NOT NULL,
           issuer TEXT NOT NULL,
           period_from TEXT,
           period_to TEXT,
           investor_name TEXT,
           data TEXT NOT NULL
        )"""
    )


def _clean_account(account):
    acct = str(account or "1").strip() or "1"
    if not _ACCOUNT_RE.match(acct):
        raise ValueError("Invalid account")
    return acct


def _dec(v):
    return None if v is None else Decimal(str(v))


def _enum_name(v):
    # Statement models keep enum values (plain strings); nested transactions keep enum members.
    return getattr(v, "name", None) or str(v or "")


def _as_date(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    from dateutil.parser import parse as parse_date

    return parse_date(str(v)).date()


def _read_pdf(pdf_bytes, password):
    import casparser
    from casparser.exceptions import IncorrectPasswordError, ParserException

    if not pdf_bytes.startswith(b"%PDF"):
        raise ValueError("That file isn't a PDF. Upload the statement exactly as it arrived by email.")
    try:
        return casparser.read_cas_pdf(io.BytesIO(pdf_bytes), password or "")
    except IncorrectPasswordError:
        raise ValueError("Wrong password for this statement.") from None
    except ParserException as e:
        raise ValueError(f"Couldn't read this as a CAMS, KFintech, NSDL or CDSL statement ({e}).") from None


def _amfi_codes_by_isin():
    """ISIN -> AMFI scheme code from AMFI's daily NAV file, for schemes newer than casparser's ISIN list."""
    now = time.time()
    if _amfi_isin_cache["codes"] and now - _amfi_isin_cache["at"] < _AMFI_TTL_SEC:
        return _amfi_isin_cache["codes"]
    codes = {}
    try:
        r = requests.get(_AMFI_NAV_URL, timeout=25)
        r.raise_for_status()
        for line in r.text.splitlines():
            parts = line.split(";")
            if len(parts) >= 3 and parts[0].strip().isdigit():
                for isin in (parts[1].strip(), parts[2].strip()):
                    if len(isin) == 12:
                        codes[isin] = parts[0].strip()
    except Exception as e:
        print(f"[mf-cas] AMFI NAV file unavailable: {e}", flush=True)
        return _amfi_isin_cache["codes"]
    _amfi_isin_cache.update(at=now, codes=codes)
    return codes


def _held_funds(data):
    """Schemes the statement shows units in, merged across folios: key -> {name, isin, code, folios}."""
    from casparser.types import NSDLCASData

    if isinstance(data, NSDLCASData):
        entries = [
            (m.name or m.isin, m.isin, m.amfi, m.balance, m.folio)
            for acct in data.accounts for m in acct.mutual_funds
        ]
    else:
        entries = [
            (s.scheme, s.isin, s.amfi, s.close, folio.folio)
            for folio in data.folios for s in folio.schemes
        ]
    funds = {}
    for name, isin, code, units, folio in entries:
        if (_dec(units) or Decimal(0)) < _MIN_UNITS:
            continue
        f = funds.setdefault(code or isin or name, {"name": name, "isin": isin, "code": code, "folios": []})
        if folio and folio not in f["folios"]:
            f["folios"].append(folio)
    return funds


def _resolve_codes(funds):
    """Fill in missing AMFI codes: by ISIN from AMFI's file, then by name."""
    for f in funds.values():
        if not f["code"] and f["isin"]:
            f["code"] = _amfi_codes_by_isin().get(f["isin"])
        if not f["code"]:
            match = mf_service.resolve_scheme(f["name"])
            f["code"] = match["schemeCode"] if match else None


def _sanitise(data):
    """Detailed statement reduced to what capital gains need."""
    folios = []
    for folio in data.folios:
        schemes = []
        for s in folio.schemes:
            schemes.append({
                "name": s.scheme,
                "isin": s.isin,
                "type": s.type,
                "open": str(s.open),
                "close": str(s.close),
                "txns": [
                    [
                        _as_date(t.date).isoformat(), _enum_name(t.type),
                        None if t.amount is None else str(t.amount),
                        None if t.units is None else str(t.units),
                        None if t.nav is None else str(t.nav),
                        None if t.balance is None else str(t.balance),
                        t.description,
                    ]
                    for t in s.transactions
                ],
            })
        folios.append({"folio": folio.folio, "amc": folio.amc, "schemes": schemes})
    return {"v": 1, "folios": folios}


def _name_key(name):
    return " ".join(sorted(re.findall(r"[a-z]+", name.lower())))


def import_statement(pdf_bytes, password, account):
    """Fill in folio numbers for one account's funds and, from a detailed CAS, keep its transactions."""
    from casparser.types import NSDLCASData

    acct = _clean_account(account)
    data = _read_pdf(pdf_bytes, password)
    issuer = _enum_name(data.file_type)
    if isinstance(data, NSDLCASData):
        kind = "DEMAT"
    else:
        kind = "DETAILED" if _enum_name(data.cas_type) == "DETAILED" else "SUMMARY"
    funds = _held_funds(data)
    _resolve_codes(funds)
    investor = (data.investor_info.name or "").strip() if data.investor_info else ""
    period = data.statement_period

    warnings = list(data.parse_warnings or [])
    folios_set, not_in_list = [], []
    with get_db() as conn:
        _ensure_table(conn)
        mf_service._ensure_table(conn)
        previous = conn.execute("SELECT investor_name FROM mf_cas_statements WHERE account = ?", (acct,)).fetchone()
        prev_name = (previous["investor_name"] or "").strip() if previous else ""
        if prev_name and investor and _name_key(prev_name) != _name_key(investor):
            warnings.append(f"This account's previous statement was for {prev_name}; this one is for {investor}.")
        rows = conn.execute(
            "SELECT id, scheme_code, scheme_name, folio FROM mf_holdings WHERE account = ?", (acct,)
        ).fetchall()
        by_code = defaultdict(list)
        for r in rows:
            by_code[r["scheme_code"]].append(r)
        for f in sorted(funds.values(), key=lambda x: x["name"].lower()):
            matches = by_code.get(f["code"]) if f["code"] else None
            if not matches:
                not_in_list.append(f["name"])
                continue
            folio = ", ".join(f["folios"])
            for r in matches:
                if folio and folio != (r["folio"] or ""):
                    conn.execute("UPDATE mf_holdings SET folio = ? WHERE id = ?", (folio, r["id"]))
                    folios_set.append({"scheme": r["scheme_name"], "folio": folio})
        if kind == "DETAILED":
            conn.execute(
                "INSERT OR REPLACE INTO mf_cas_statements "
                "(account, imported_at, issuer, period_from, period_to, investor_name, data) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (acct, time.time(), issuer, period.from_ if period else "", period.to if period else "",
                 investor, json.dumps(_sanitise(data), separators=(",", ":"))),
            )
    gain_years = []
    if kind == "DETAILED":
        st = _load_statement(acct)
        gains = _gains(st)
        gain_years = [y["fy"] for y in gains["years"]]
        warnings.extend(_incomplete_warnings(st) + gains["errors"])
    print(f"[mf-cas] account {acct}: {issuer} {kind}, {len(funds)} funds held", flush=True)
    return {
        "issuer": issuer,
        "statementType": kind,
        "investorName": investor,
        "periodFrom": period.from_ if period else "",
        "periodTo": period.to if period else "",
        "foliosSet": folios_set,
        "notInList": not_in_list,
        "gainYears": gain_years,
        "warnings": warnings,
    }


def forget_statement(account):
    acct = _clean_account(account)
    with get_db() as conn:
        _ensure_table(conn)
        return conn.execute("DELETE FROM mf_cas_statements WHERE account = ?", (acct,)).rowcount


def _load_statement(account, row=None):
    """Stored statement for an account, parsed once per import, or None."""
    if row is None:
        with get_db() as conn:
            _ensure_table(conn)
            row = conn.execute(
                "SELECT account, imported_at, issuer, period_from, period_to, investor_name, data "
                "FROM mf_cas_statements WHERE account = ?",
                (account,),
            ).fetchone()
        if row is None:
            return None
    key = (row["account"], row["imported_at"])
    cached = _statement_cache.get(key)
    if cached:
        return cached
    data = json.loads(row["data"])
    schemes = []
    for folio in data.get("folios") or []:
        for s in folio.get("schemes") or []:
            opening = Decimal(s.get("open") or "0")
            schemes.append({
                "name": s.get("name") or "", "isin": s.get("isin") or "", "type": s.get("type") or "",
                "folio": folio.get("folio") or "",
                "complete": opening < _OPEN_UNITS_LIMIT,
                "txns": [
                    {
                        "date": date.fromisoformat(t[0]), "type": t[1], "amount": _dec(t[2]), "units": _dec(t[3]),
                        "nav": _dec(t[4]), "balance": _dec(t[5]), "description": t[6] or "",
                    }
                    for t in s.get("txns") or []
                ],
            })
    st = {
        "account": row["account"], "importedAt": row["imported_at"], "issuer": row["issuer"],
        "periodFrom": row["period_from"] or "", "periodTo": row["period_to"] or "",
        "investorName": row["investor_name"] or "", "schemes": schemes,
    }
    for k in [k for k in _statement_cache if k[0] == row["account"]]:
        _statement_cache.pop(k, None)
    _statement_cache[key] = st
    return st


def _incomplete_warnings(st):
    return [
        f"{s['name']}: the statement starts after its first purchase, so its capital gains are left out. "
        "Request a statement starting before your first investment."
        for s in st["schemes"]
        if not s["complete"] and s["txns"]
    ]


def _is_long_term(fund_type, bought, sold):
    from dateutil.relativedelta import relativedelta

    if fund_type == "EQUITY":
        return sold > bought + relativedelta(years=1)
    # Debt-type units bought from April 2023 are short-term however long they are held.
    if bought >= _DEBT_DEEMED_SHORT_FROM:
        return False
    months = 24 if sold >= _NEW_REGIME_FROM else 36
    return sold > bought + relativedelta(months=months)


def _taxable_long_term(g, cost):
    """Long-term gain after grandfathering (equity bought before Feb 2018) or indexation (debt sold before 23 Jul 2024)."""
    from casparser.analysis.utils import CII, get_fin_year

    if g.type == "EQUITY":
        if g.sale_date < _EQUITY_LTCG_TAXED_FROM:
            return Decimal(0)
        if g.purchase_date <= _GRANDFATHER_BUY_CUTOFF:
            return g.sale_value - (max(g.purchase_value, min(g.fmv, g.sale_value)) + g.stamp_duty)
        return g.sale_value - cost
    if g.sale_date < _NEW_REGIME_FROM:
        ratio = Decimal(CII[get_fin_year(g.sale_date)]) / Decimal(CII[get_fin_year(g.purchase_date)])
        return g.sale_value - cost * ratio
    return g.sale_value - cost


def _gains(st):
    """Realised gains per financial year, sales matched to purchases first-in first-out within each folio."""
    key = (st["account"], st["importedAt"])
    if key in _gains_cache:
        return _gains_cache[key]
    from casparser.analysis.gains import FIFOUnits, Fund
    from casparser.enums import TransactionType
    from casparser.types import TransactionData

    years = defaultdict(dict)
    errors = []
    for s in st["schemes"]:
        if not s["complete"] or not any(t["units"] is not None and t["units"] < 0 for t in s["txns"]):
            continue
        txns = [
            TransactionData(
                date=t["date"], description=t["description"], amount=t["amount"], units=t["units"],
                nav=t["nav"], balance=t["balance"], type=TransactionType[t["type"]],
            )
            for t in s["txns"]
        ]
        fund = Fund(scheme=s["name"], folio=s["folio"], isin=s["isin"], type=s["type"])
        try:
            lots = FIFOUnits(fund, txns).gains
        except Exception as e:
            errors.append(f"{s['name']}: couldn't match every sale to a purchase ({e}), so its gains are left out.")
            continue
        for g in lots:
            cost = g.purchase_value + g.stamp_duty
            gain = g.sale_value - cost
            row = years[g.fy].setdefault((s["name"], s["isin"]), {
                "scheme": s["name"], "isin": s["isin"], "folios": [], "type": g.type,
                "sold": Decimal(0), "cost": Decimal(0), "stcg": Decimal(0), "ltcg": Decimal(0), "ltcgTaxable": Decimal(0),
            })
            if s["folio"] and s["folio"] not in row["folios"]:
                row["folios"].append(s["folio"])
            row["sold"] += g.sale_value
            row["cost"] += cost
            if _is_long_term(g.type, g.purchase_date, g.sale_date):
                row["ltcg"] += gain
                row["ltcgTaxable"] += _taxable_long_term(g, cost)
            else:
                row["stcg"] += gain

    money = ("sold", "cost", "stcg", "ltcg", "ltcgTaxable")
    out = []
    for fy in sorted(years, reverse=True):
        rows = sorted(years[fy].values(), key=lambda r: r["scheme"].lower())
        totals = defaultdict(Decimal)
        for r in rows:
            prefix = "equity" if r["type"] == "EQUITY" else "debt"
            totals["sold"] += r["sold"]
            totals["cost"] += r["cost"]
            totals[prefix + "Stcg"] += r["stcg"]
            totals[prefix + "Ltcg"] += r["ltcg"]
            totals[prefix + "LtcgTaxable"] += r["ltcgTaxable"]
        out.append({
            "fy": fy,
            "rows": [{**r, "folios": ", ".join(r["folios"]), **{k: round(float(r[k]), 2) for k in money}} for r in rows],
            "totals": {k: round(float(totals[k]), 2) for k in (
                "sold", "cost", "equityStcg", "equityLtcg", "equityLtcgTaxable", "debtStcg", "debtLtcg", "debtLtcgTaxable",
            )},
        })
    result = {"years": out, "errors": errors}
    for k in [k for k in _gains_cache if k[0] == st["account"]]:
        _gains_cache.pop(k, None)
    _gains_cache[key] = result
    return result


def summary():
    """Each account's stored detailed statement with its realised capital gains by financial year."""
    with get_db() as conn:
        _ensure_table(conn)
        rows = conn.execute(
            "SELECT account, imported_at, issuer, period_from, period_to, investor_name, data "
            "FROM mf_cas_statements ORDER BY account"
        ).fetchall()
    out = []
    for row in rows:
        st = _load_statement(row["account"], row)
        gains = _gains(st)
        out.append({
            "account": st["account"],
            "issuer": st["issuer"],
            "investorName": st["investorName"],
            "periodFrom": st["periodFrom"],
            "periodTo": st["periodTo"],
            "importedAt": st["importedAt"],
            "gains": gains["years"],
            "warnings": _incomplete_warnings(st) + gains["errors"],
        })
    return {"statements": out}
