"""Manual mutual-fund holdings + live NAV.

ICICI Breeze's MF endpoints require a facility that isn't enabled for the account, so live NAV is
sourced from AMFI via the free mfapi.in API (no auth). Users add holdings manually (scheme + units
+ optional invested cost); we compute current value and P&L from the latest NAV.
"""
import csv as _csv
import io
import re
import time

import requests

from db import get_db

_SEARCH_URL = "https://api.mfapi.in/mf/search"
_NAV_URL = "https://api.mfapi.in/mf/{code}/latest"
_NAV_CACHE = {}  # scheme_code -> (fetched_at, {nav, date, name})
_NAV_TTL_SEC = 1800  # NAV updates once per day; 30 min cache is plenty


def _ensure_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS mf_holdings (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           scheme_code TEXT NOT NULL,
           scheme_name TEXT NOT NULL,
           units REAL NOT NULL,
           invested REAL,
           folio TEXT
        )"""
    )
    # Migration: add account column to pre-existing tables.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(mf_holdings)").fetchall()}
    if "account" not in cols:
        conn.execute("ALTER TABLE mf_holdings ADD COLUMN account TEXT NOT NULL DEFAULT '1'")


def search_schemes(query):
    q = (query or "").strip()
    if len(q) < 2:
        return []
    data = []
    for attempt in range(2):  # retry once for transient timeouts
        try:
            r = requests.get(_SEARCH_URL, params={"q": q}, timeout=15)
            r.raise_for_status()
            data = r.json() if r.content else []
            break
        except Exception:
            data = []
    out = []
    for x in data or []:
        out.append({"schemeCode": str(x.get("schemeCode")), "schemeName": x.get("schemeName") or ""})
        if len(out) >= 25:
            break
    return out


# Noise words to drop when a full-name search returns nothing (mfapi search is punctuation/word sensitive).
_NOISE_WORDS = {
    "fund", "regular", "direct", "plan", "growth", "option", "idcw", "dividend",
    "payout", "reinvestment", "reinvest", "and", "the", "scheme",
}


_COMPOUND_SPLITS = {
    "flexicap": "flexi cap", "smallcap": "small cap", "midcap": "mid cap",
    "largecap": "large cap", "multicap": "multi cap", "ultrashort": "ultra short",
}


def _clean_query(name):
    n = re.sub(r"[-/(),.]", " ", (name or "").lower())
    for k, v in _COMPOUND_SPLITS.items():
        n = n.replace(k, v)
    toks = [t for t in n.split() if t and t not in _NOISE_WORDS]
    return " ".join(toks)


def _search_with_fallback(name):
    cands = search_schemes(name)
    if cands:
        return cands
    cleaned = _clean_query(name)
    if cleaned and cleaned.lower() != (name or "").strip().lower():
        cands = search_schemes(cleaned)
        if cands:
            return cands
    toks = cleaned.split()
    if len(toks) > 4:  # try the most significant leading tokens
        cands = search_schemes(" ".join(toks[:4]))
    return cands


def live_nav(scheme_code):
    code = str(scheme_code)
    now = time.time()
    cached = _NAV_CACHE.get(code)
    if cached and (now - cached[0]) < _NAV_TTL_SEC:
        return cached[1]
    out = None
    for attempt in range(2):  # one retry for transient network/SSL hiccups
        try:
            r = requests.get(_NAV_URL.format(code=code), timeout=12)
            d = r.json() if r.content else {}
            latest = (d.get("data") or [{}])[0]
            meta = d.get("meta") or {}
            out = {
                "nav": float(latest.get("nav") or 0) or 0.0,
                "date": latest.get("date") or "",
                "name": meta.get("scheme_name") or "",
            }
            break
        except Exception:
            out = None
    if out is None:
        # Don't cache failures (so the next request retries); signal unavailable.
        return {"nav": 0.0, "date": "", "name": "", "unavailable": True}
    _NAV_CACHE[code] = (now, out)
    return out


def add_holding(scheme_code, scheme_name, units, invested=None, folio="", account="1"):
    code = str(scheme_code or "").strip()
    if not code:
        raise ValueError("scheme_code is required")
    u = float(units)
    if u <= 0:
        raise ValueError("units must be > 0")
    inv = None
    if invested not in (None, ""):
        inv = float(invested)
    acct = str(account or "1").strip() or "1"
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute(
            "INSERT INTO mf_holdings (scheme_code, scheme_name, units, invested, folio, account) VALUES (?, ?, ?, ?, ?, ?)",
            (code, (scheme_name or "").strip(), u, inv, (folio or "").strip(), acct),
        )
        return cur.lastrowid


def _norm_tokens(name):
    return set(re.findall(r"[a-z0-9]+", (name or "").lower()))


def _plan_flags(name):
    n = (name or "").lower()
    return {
        "direct": "direct" in n,
        "idcw": any(k in n for k in ("idcw", "dividend", "payout", "reinvest")),
    }


def resolve_scheme(name, nav_hint=None):
    """Best AMFI scheme code for a CSV scheme name.
    Scores candidates by token overlap + plan match (Direct/Regular, Growth/IDCW), then breaks ties by
    closeness of live NAV to nav_hint (disambiguates Regular vs Direct). Returns dict or None.
    """
    candidates = _search_with_fallback(name)
    if not candidates:
        return None
    target_tokens = _norm_tokens(name)
    tflags = _plan_flags(name)

    scored = []
    for c in candidates:
        ctok = _norm_tokens(c["schemeName"])
        cflags = _plan_flags(c["schemeName"])
        score = len(target_tokens & ctok)
        score += 3 if cflags["direct"] == tflags["direct"] else -3
        score += 2 if cflags["idcw"] == tflags["idcw"] else -2
        scored.append((score, c))
    scored.sort(key=lambda x: x[0], reverse=True)

    top = [c for _, c in scored[:4]]
    hint = None
    try:
        hint = float(nav_hint) if nav_hint not in (None, "") else None
    except (TypeError, ValueError):
        hint = None

    best = top[0]
    best_nav = live_nav(best["schemeCode"])
    if hint and len(top) > 1:
        # Pick the candidate whose live NAV is closest to the CSV's recorded NAV.
        best_diff = abs((best_nav.get("nav") or 0) - hint)
        for c in top[1:]:
            nv = live_nav(c["schemeCode"])
            diff = abs((nv.get("nav") or 0) - hint)
            if (nv.get("nav") or 0) > 0 and diff < best_diff:
                best, best_nav, best_diff = c, nv, diff
    return {"schemeCode": best["schemeCode"], "schemeName": best["schemeName"], "nav": best_nav.get("nav") or 0}


# CSV header aliases (case-insensitive) -> our fields.
_CSV_NAME_KEYS = ("scheme", "fund name", "scheme name")
_CSV_UNITS_KEYS = ("units held", "units", "unit")
_CSV_INVESTED_KEYS = ("value at cost", "cost value", "invested", "amount invested", "purchase value")
_CSV_NAVHINT_KEYS = ("last recorded nav", "current nav", "nav")
_CSV_FOLIO_KEYS = ("folio",)


def _pick_col(headers_lower, keys):
    # Prefer exact, then contains.
    for k in keys:
        if k in headers_lower:
            return headers_lower[k]
    for k in keys:
        for hl, orig in headers_lower.items():
            if k in hl:
                return orig
    return None


def import_csv(csv_text, account="1"):
    """Parse an ICICI-style MF portfolio CSV, resolve each scheme to AMFI, and upsert into mf_holdings.
    Upsert key = (account, scheme_code): existing rows get units/invested updated; new ones inserted.
    Returns {imported: [...], skipped: [...]}.
    """
    text = (csv_text or "").lstrip("\ufeff")
    reader = _csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSV has no header row")
    headers_lower = {h.strip().lower(): h for h in reader.fieldnames if h}
    name_col = _pick_col(headers_lower, _CSV_NAME_KEYS)
    units_col = _pick_col(headers_lower, _CSV_UNITS_KEYS)
    if not name_col or not units_col:
        raise ValueError(f"Could not find scheme/units columns. Headers: {list(reader.fieldnames)}")
    invested_col = _pick_col(headers_lower, _CSV_INVESTED_KEYS)
    navhint_col = _pick_col(headers_lower, _CSV_NAVHINT_KEYS)
    folio_col = _pick_col(headers_lower, _CSV_FOLIO_KEYS)
    # "Scheme" can collide with "scheme" of category; prefer a column literally named Scheme over Fund.
    acct = str(account or "1").strip() or "1"

    def _f(v):
        try:
            return float(str(v).replace(",", "").strip())
        except (TypeError, ValueError):
            return None

    imported, skipped = [], []
    with get_db() as conn:
        _ensure_table(conn)
        for row in reader:
            name = (row.get(name_col) or "").strip()
            units = _f(row.get(units_col))
            if not name or not units or units <= 0:
                continue
            invested = _f(row.get(invested_col)) if invested_col else None
            nav_hint = _f(row.get(navhint_col)) if navhint_col else None
            folio = (row.get(folio_col) or "").strip() if folio_col else ""
            match = resolve_scheme(name, nav_hint)
            if not match:
                skipped.append({"scheme": name, "reason": "no AMFI match"})
                continue
            code = match["schemeCode"]
            existing = conn.execute(
                "SELECT id FROM mf_holdings WHERE account = ? AND scheme_code = ?", (acct, code)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE mf_holdings SET units = ?, invested = ?, scheme_name = ?, folio = ? WHERE id = ?",
                    (units, invested, match["schemeName"], folio, existing[0]),
                )
                action = "updated"
            else:
                conn.execute(
                    "INSERT INTO mf_holdings (scheme_code, scheme_name, units, invested, folio, account) VALUES (?, ?, ?, ?, ?, ?)",
                    (code, match["schemeName"], units, invested, folio, acct),
                )
                action = "added"
            imported.append({
                "csvScheme": name,
                "matchedScheme": match["schemeName"],
                "schemeCode": code,
                "units": units,
                "invested": invested,
                "nav": match["nav"],
                "action": action,
            })
    return {"imported": imported, "skipped": skipped}


def delete_holding(holding_id):
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute("DELETE FROM mf_holdings WHERE id = ?", (int(holding_id),))
        return cur.rowcount


def list_holdings(account=None):
    acct = str(account).strip() if account not in (None, "", "all") else None
    with get_db() as conn:
        _ensure_table(conn)
        if acct:
            rows = conn.execute(
                "SELECT id, scheme_code, scheme_name, units, invested, folio, account FROM mf_holdings WHERE account = ? ORDER BY scheme_name",
                (acct,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, scheme_code, scheme_name, units, invested, folio, account FROM mf_holdings ORDER BY account, scheme_name"
            ).fetchall()

    holdings = []
    tot_value = 0.0
    tot_invested = 0.0
    have_invested = False
    for r in rows:
        nav = live_nav(r["scheme_code"])
        units = float(r["units"])
        nav_ok = nav.get("nav", 0) > 0 and not nav.get("unavailable")
        value = round(units * nav["nav"], 2) if nav_ok else None
        invested = float(r["invested"]) if r["invested"] is not None else None
        pnl = round(value - invested, 2) if (value is not None and invested is not None) else None
        pnl_pct = round((pnl / invested * 100), 2) if (pnl is not None and invested and invested > 0) else None
        if value is not None:
            tot_value += value
        if invested is not None:
            tot_invested += invested
            have_invested = True
        holdings.append({
            "id": r["id"],
            "account": r["account"],
            "schemeCode": r["scheme_code"],
            "schemeName": r["scheme_name"] or nav["name"],
            "units": units,
            "nav": nav["nav"] if nav_ok else None,
            "navDate": nav["date"],
            "value": value,
            "invested": invested,
            "pnl": pnl,
            "pnlPct": pnl_pct,
            "navUnavailable": not nav_ok,
            "folio": r["folio"] or "",
        })

    tot_pnl = round(tot_value - tot_invested, 2) if have_invested else None
    tot_pnl_pct = round((tot_pnl / tot_invested * 100), 2) if (have_invested and tot_invested > 0) else None
    return {
        "holdings": holdings,
        "totals": {
            "value": round(tot_value, 2),
            "invested": round(tot_invested, 2) if have_invested else None,
            "pnl": tot_pnl,
            "pnlPct": tot_pnl_pct,
        },
    }
