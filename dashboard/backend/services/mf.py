"""Manual mutual-fund holdings + live NAV.

ICICI Breeze's MF endpoints require a facility that isn't enabled for the account, so live NAV is
sourced from AMFI via the free mfapi.in API (no auth). Users add holdings manually (scheme + units
+ optional invested cost), import a CSV, or import a CAS statement (services/mf_cas.py); we compute
current value and P&L from the latest NAV.
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
    # XIRR as reported by the broker's CSV export, shown when no statement gives one.
    if "csv_xirr" not in cols:
        conn.execute("ALTER TABLE mf_holdings ADD COLUMN csv_xirr REAL")
        conn.execute("ALTER TABLE mf_holdings ADD COLUMN csv_xirr_date TEXT")


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


def _clean_query(name, split_compounds=True):
    n = re.sub(r"[-/(),.]", " ", (name or "").lower())
    if split_compounds:
        for k, v in _COMPOUND_SPLITS.items():
            n = n.replace(k, v)
    toks = [t for t in n.split() if t and t not in _NOISE_WORDS]
    return " ".join(toks)


def _broader_queries(name):
    """The name without plan words, most specific first."""
    out = []
    # AMFI spells compounds both ways ("Mid Cap Fund", "Large and Midcap Fund"), so try each.
    for split in (True, False):
        toks = _clean_query(name, split).split()
        # Then the most significant leading tokens.
        for q in (" ".join(toks), " ".join(toks[:4])):
            if q and q not in out:
                out.append(q)
    return out


def _search_with_fallback(name):
    cands = search_schemes(name)
    if cands:
        return cands
    tried = {(name or "").strip().lower()}
    for q in _broader_queries(name):
        if q not in tried:
            tried.add(q)
            cands = search_schemes(q)
            if cands:
                return cands
    return []


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


# A CSV's recorded NAV can be some days older than the latest one.
_NAV_TOLERANCE = 0.15


def _ranked(name, candidates):
    """(score, candidate) by token overlap + plan match (Direct/Regular, Growth/IDCW), best first."""
    target_tokens = _norm_tokens(name)
    tflags = _plan_flags(name)
    scored = []
    for c in candidates:
        cflags = _plan_flags(c["schemeName"])
        score = len(target_tokens & _norm_tokens(c["schemeName"]))
        score += 3 if cflags["direct"] == tflags["direct"] else -3
        score += 2 if cflags["idcw"] == tflags["idcw"] else -2
        scored.append((score, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored


def _nav_date_key(nav):
    try:
        d, m, y = (int(p) for p in (nav.get("date") or "").split("-"))
        return (y, m, d)
    except ValueError:
        return (0, 0, 0)


def _nav_fits(nav, hint):
    v = nav.get("nav") or 0
    return v > 0 and not nav.get("unavailable") and abs(v - hint) <= hint * _NAV_TOLERANCE


def _match_by_nav(name, candidates, hint):
    """Best-named candidate whose NAV fits the hint, also searching the name without plan words
    (AMFI drops them from some scheme names, e.g. "Motilal Oswal Midcap Fund")."""
    pool = {c["schemeCode"]: c for c in candidates}
    for q in _broader_queries(name):
        for c in search_schemes(q):
            pool.setdefault(c["schemeCode"], c)
    fits = []
    for score, c in _ranked(name, list(pool.values()))[:12]:
        nv = live_nav(c["schemeCode"])
        if _nav_fits(nv, hint):
            fits.append((-score, abs(nv["nav"] - hint), c, nv))
    if not fits:
        return None
    fits.sort(key=lambda f: f[:2])
    return fits[0][2], fits[0][3]


def resolve_scheme(name, nav_hint=None):
    """Best AMFI scheme code for a CSV scheme name.
    Scores candidates by token overlap + plan match, then breaks ties by closeness of live NAV to
    nav_hint (disambiguates Regular vs Direct). With a nav_hint, a fund whose NAV is far from it is
    never returned: the search widens, and None comes back when nothing fits. Returns dict or None.
    """
    candidates = _search_with_fallback(name)
    if not candidates:
        return None
    try:
        hint = float(nav_hint) if nav_hint not in (None, "") else None
    except (TypeError, ValueError):
        hint = None
    if hint is not None and hint <= 0:
        hint = None

    ranked = _ranked(name, candidates)
    top = [c for _, c in ranked[:4]]
    best = top[0]
    if not hint:
        tied = [c for s, c in ranked[:4] if s == ranked[0][0]]
        if len(tied) > 1:
            # Without a NAV to compare, a scheme that's still priced beats a wound-up plan.
            best = max(tied, key=lambda c: _nav_date_key(live_nav(c["schemeCode"])))
    best_nav = live_nav(best["schemeCode"])
    if hint and len(top) > 1:
        # Pick the candidate whose live NAV is closest to the CSV's recorded NAV.
        best_diff = abs((best_nav.get("nav") or 0) - hint)
        for c in top[1:]:
            nv = live_nav(c["schemeCode"])
            diff = abs((nv.get("nav") or 0) - hint)
            if (nv.get("nav") or 0) > 0 and diff < best_diff:
                best, best_nav, best_diff = c, nv, diff
    if hint and not _nav_fits(best_nav, hint):
        found = _match_by_nav(name, candidates, hint)
        if found is None:
            return None
        best, best_nav = found
    return {"schemeCode": best["schemeCode"], "schemeName": best["schemeName"], "nav": best_nav.get("nav") or 0}


# CSV header aliases (case-insensitive) -> our fields.
_CSV_NAME_KEYS = ("scheme", "fund name", "scheme name")
_CSV_UNITS_KEYS = ("units held", "units", "unit")
_CSV_INVESTED_KEYS = ("value at cost", "cost value", "invested", "amount invested", "purchase value")
_CSV_NAVHINT_KEYS = ("last recorded nav", "current nav", "nav")
_CSV_NAVDATE_KEYS = ("last recorded nav on", "nav date")
_CSV_XIRR_KEYS = ("xirr %", "xirr")
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
    """Parse an ICICI-style MF portfolio CSV, resolve each scheme to AMFI, and make the account match it.
    The CSV is the record for units, cost and XIRR. Key = (account, scheme_code): existing rows get those
    updated, new ones are inserted, and once every row has matched, the account's other funds are
    removed. Returns {imported: [...], skipped: [...], removed: [...]}.
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
    navdate_col = _pick_col(headers_lower, _CSV_NAVDATE_KEYS)
    xirr_col = _pick_col(headers_lower, _CSV_XIRR_KEYS)
    folio_col = _pick_col(headers_lower, _CSV_FOLIO_KEYS)
    # "Scheme" can collide with "scheme" of category; prefer a column literally named Scheme over Fund.
    acct = str(account or "1").strip() or "1"

    def _f(v):
        try:
            return float(str(v).replace(",", "").strip())
        except (TypeError, ValueError):
            return None

    parsed = []
    for row in reader:
        name = (row.get(name_col) or "").strip()
        units = _f(row.get(units_col))
        if not name or not units or units <= 0:
            continue
        invested = _f(row.get(invested_col)) if invested_col else None
        nav_hint = _f(row.get(navhint_col)) if navhint_col else None
        xirr = _f(row.get(xirr_col)) if xirr_col else None
        xirr_date = (row.get(navdate_col) or "").strip() if (navdate_col and xirr is not None) else None
        folio = (row.get(folio_col) or "").strip() if folio_col else ""
        parsed.append((name, units, invested, xirr, xirr_date, folio, resolve_scheme(name, nav_hint)))

    # Matching is network-bound, so it's done before the write transaction to keep the lock short.
    imported, skipped, removed = [], [], []
    kept = {}  # scheme_code -> id of the row this import wrote
    with get_db() as conn:
        _ensure_table(conn)
        for name, units, invested, xirr, xirr_date, folio, match in parsed:
            if not match:
                skipped.append({"scheme": name, "reason": "no AMFI fund with this name and NAV"})
                continue
            code = match["schemeCode"]
            existing = None if code in kept else conn.execute(
                "SELECT id FROM mf_holdings WHERE account = ? AND scheme_code = ?", (acct, code)
            ).fetchone()
            if code in kept:
                # The same fund on two rows (two folios) adds up; their XIRRs can't be combined.
                conn.execute(
                    "UPDATE mf_holdings SET units = units + ?, "
                    "invested = CASE WHEN invested IS NULL OR ? IS NULL THEN NULL ELSE invested + ? END, "
                    "csv_xirr = NULL, csv_xirr_date = NULL WHERE id = ?",
                    (units, invested, invested, kept[code]),
                )
                action = "combined"
            elif existing:
                # A CSV without folios keeps the folio numbers a statement filled in.
                conn.execute(
                    "UPDATE mf_holdings SET units = ?, invested = ?, scheme_name = ?, folio = COALESCE(NULLIF(?, ''), folio), "
                    "csv_xirr = ?, csv_xirr_date = ? WHERE id = ?",
                    (units, invested, match["schemeName"], folio, xirr, xirr_date, existing[0]),
                )
                kept[code] = existing[0]
                action = "updated"
            else:
                cur = conn.execute(
                    "INSERT INTO mf_holdings (scheme_code, scheme_name, units, invested, folio, account, csv_xirr, csv_xirr_date) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (code, match["schemeName"], units, invested, folio, acct, xirr, xirr_date),
                )
                kept[code] = cur.lastrowid
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
        if not imported and not skipped:
            raise ValueError("This CSV has no fund rows with units, so nothing was changed.")
        # A skipped row may be one of the account's funds, so only a fully matched CSV removes anything.
        if not skipped:
            kept_ids = set(kept.values())
            for r in conn.execute(
                "SELECT id, scheme_name FROM mf_holdings WHERE account = ? ORDER BY scheme_name", (acct,)
            ).fetchall():
                if r["id"] not in kept_ids:
                    conn.execute("DELETE FROM mf_holdings WHERE id = ?", (r["id"],))
                    removed.append({"id": r["id"], "scheme": r["scheme_name"]})
    return {"imported": imported, "skipped": skipped, "removed": removed}


def delete_holding(holding_id):
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute("DELETE FROM mf_holdings WHERE id = ?", (int(holding_id),))
        return cur.rowcount


def overlap(refresh=False):
    """Look-through overlap across every account's funds.

    Recomputed from the current mf_holdings rows on each call, so adding or removing a
    fund is reflected straight away; only the third-party portfolios are cached. NAV
    comes from the same cached AMFI lookup the holdings table uses, so fund values agree.
    """
    from helpers import mf_overlap

    return mf_overlap.compute(nav_lookup=live_nav, refresh=refresh)


def list_holdings(account=None):
    acct = str(account).strip() if account not in (None, "", "all") else None
    with get_db() as conn:
        _ensure_table(conn)
        cols = "id, scheme_code, scheme_name, units, invested, folio, account, csv_xirr, csv_xirr_date"
        if acct:
            rows = conn.execute(
                f"SELECT {cols} FROM mf_holdings WHERE account = ? ORDER BY scheme_name",
                (acct,),
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT {cols} FROM mf_holdings ORDER BY account, scheme_name"
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
            # As reported in the last CSV import; it isn't recomputed as the NAV moves.
            "xirr": r["csv_xirr"],
            "xirrDate": r["csv_xirr_date"] or "",
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
