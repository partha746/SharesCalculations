"""Expected income: dividends and REIT / InvIT distributions.

Payout per unit is the **sum of actual payouts over the trailing 12 months** (Yahoo, via yfinance).
That is deliberately not Yahoo's `dividendYield` / `trailingAnnualDividendRate`, which come back null
or 0 for several Indian InvITs even though the payout history is present.

ICICI Breeze returns its own internal stock codes rather than NSE symbols, so a resolved symbol (and
the payout itself) can be overridden per instrument; overrides always win over the fetched value.
"""
import json
import os
import time

from db import DB_PATH, get_db

# symbol -> (fetched_at, annual_payout | None). Payout history changes at most quarterly.
_CACHE = {}
_CACHE_TTL_SEC = 12 * 3600

# ICICI reports its own short codes (EMBOFF, MINBUS, ...) rather than NSE symbols. ICICI's own
# SecurityMaster maps ShortName -> ExchangeCode (EMBOFF -> EMBASSY), which is what Yahoo needs.
_SECURITY_MASTER_URL = "https://directlink.icicidirect.com/MotherAppMaster/SecurityMaster.zip"
_SCRIP_TTL_SEC = 7 * 24 * 3600
_scrip_map = None  # ICICI short name -> {"nse": symbol, "name": company}


def _scrip_cache_path():
    return os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "icici_nse_scrip_map.json")


def _download_scrip_map():
    try:
        import io
        import urllib.request
        from zipfile import ZipFile

        import pandas as pd

        resp = urllib.request.urlopen(_SECURITY_MASTER_URL, timeout=60)
        archive = ZipFile(io.BytesIO(resp.read()))
        df = pd.read_csv(archive.open("NSEScripMaster.txt"), sep=",", engine="python")
        cols = {str(c).strip().strip('"'): c for c in df.columns}
        short_c, exch_c, name_c = cols.get("ShortName"), cols.get("ExchangeCode"), cols.get("CompanyName")
        if not short_c or not exch_c:
            return {}
        out = {}
        names = df[name_c] if name_c else [""] * len(df)
        for short, exch, nm in zip(df[short_c], df[exch_c], names):
            s = str(short).strip().upper()
            e = str(exch).strip().upper()
            if s and e and e not in ("NAN", "NONE"):
                out[s] = {"nse": e, "name": str(nm).strip().title()}
        return out
    except Exception as e:
        print(f"[income] SecurityMaster download failed: {e}", flush=True)
        return {}


def _load_scrip_map():
    global _scrip_map
    if _scrip_map is not None:
        return _scrip_map
    path = _scrip_cache_path()
    try:
        if os.path.exists(path) and (time.time() - os.path.getmtime(path)) < _SCRIP_TTL_SEC:
            with open(path, "r") as f:
                _scrip_map = json.load(f)
                return _scrip_map
    except Exception:
        pass
    _scrip_map = _download_scrip_map()
    if _scrip_map:
        try:
            with open(path, "w") as f:
                json.dump(_scrip_map, f)
        except Exception:
            pass
    return _scrip_map


def nse_lookup(code):
    """ICICI short code -> (NSE symbol, company name). (None, None) when unknown."""
    entry = _load_scrip_map().get((code or "").strip().upper()) or {}
    return entry.get("nse"), entry.get("name")


# Distinguishes "field not sent" from "field sent as null/empty" in a partial update.
_UNSET = object()


def _ensure_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS income_overrides (
           key TEXT PRIMARY KEY,
           yahoo_symbol TEXT,
           annual_payout REAL,
           updated_at TEXT NOT NULL
        )"""
    )


def currency_for(symbol):
    """Indian listings are quoted in INR; anything else (e.g. NVDA) in USD."""
    s = (symbol or "").upper()
    if not s or s.endswith(".NS") or s.endswith(".BO"):
        return "INR"
    return "USD"


def list_overrides():
    with get_db() as conn:
        _ensure_table(conn)
        rows = conn.execute("SELECT key, yahoo_symbol, annual_payout, updated_at FROM income_overrides").fetchall()
    return {
        r["key"]: {
            "yahooSymbol": r["yahoo_symbol"] or "",
            "annualPayout": r["annual_payout"],
            "updatedAt": r["updated_at"],
        }
        for r in rows
    }


def set_override(key, yahoo_symbol=_UNSET, annual_payout=_UNSET):
    """Update an override, touching only the fields the caller actually sent.

    Omitting a field leaves it as-is; sending it as null/"" clears it. Writing both unconditionally
    would let a partial update silently wipe the other field.
    """
    k = (key or "").strip()
    if not k:
        raise ValueError("key is required")
    if yahoo_symbol is _UNSET and annual_payout is _UNSET:
        raise ValueError("provide yahooSymbol and/or annualPayout")

    sym = "" if yahoo_symbol is _UNSET else (yahoo_symbol or "").strip()
    payout = None
    if annual_payout is not _UNSET and annual_payout not in (None, ""):
        try:
            payout = float(annual_payout)
        except (TypeError, ValueError):
            raise ValueError("annualPayout must be a number")
        if payout < 0:
            raise ValueError("annualPayout cannot be negative")

    updates = ["updated_at = excluded.updated_at"]
    if yahoo_symbol is not _UNSET:
        updates.append("yahoo_symbol = excluded.yahoo_symbol")
    if annual_payout is not _UNSET:
        updates.append("annual_payout = excluded.annual_payout")

    with get_db() as conn:
        _ensure_table(conn)
        conn.execute(
            "INSERT INTO income_overrides (key, yahoo_symbol, annual_payout, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET " + ", ".join(updates),
            (k, sym, payout, time.strftime("%Y-%m-%dT%H:%M:%S")),
        )
    # A changed symbol must not keep serving the previous symbol's cached payout.
    if sym:
        _CACHE.pop(sym.upper(), None)
    return {"ok": True}


def delete_override(key):
    with get_db() as conn:
        _ensure_table(conn)
        cur = conn.execute("DELETE FROM income_overrides WHERE key = ?", ((key or "").strip(),))
        return cur.rowcount


def _fetch_trailing_payout(symbol):
    """Sum of dividends/distributions paid in the last 365 days, or None if the symbol has no history."""
    try:
        import pandas as pd
        import yfinance as yf

        divs = yf.Ticker(symbol).dividends
        if divs is None or len(divs) == 0:
            return None
        cutoff = pd.Timestamp.now(tz=divs.index.tz) - pd.Timedelta(days=365)
        recent = divs[divs.index >= cutoff]
        if len(recent) == 0:
            return 0.0
        return round(float(recent.sum()), 4)
    except Exception as e:
        print(f"[income] payout lookup failed for {symbol}: {e}", flush=True)
        return None


def _cached_payout(symbol):
    key = (symbol or "").upper()
    if not key:
        return None
    hit = _CACHE.get(key)
    if hit and (time.time() - hit[0]) < _CACHE_TTL_SEC:
        return hit[1]
    value = _fetch_trailing_payout(symbol)
    _CACHE[key] = (time.time(), value)
    return value


def resolve(items):
    """For each {key, symbolHint}, return the annual payout per unit and where it came from.

    source: 'manual' (user-entered payout), 'yahoo' (trailing 12m history), 'unresolved' (no data —
    the caller should prompt for a symbol or a payout).
    """
    overrides = list_overrides()
    out = []
    for it in items or []:
        key = str((it or {}).get("key") or "").strip()
        if not key:
            continue
        ov = overrides.get(key) or {}
        nse, company = nse_lookup(key)
        symbol = (ov.get("yahooSymbol") or "").strip()
        if not symbol:
            symbol = f"{nse}.NS" if nse else ((it or {}).get("symbolHint") or "").strip()
        manual = ov.get("annualPayout")
        if manual is not None:
            out.append({
                "key": key, "symbol": symbol, "name": company or "", "annualPayout": float(manual),
                "currency": currency_for(symbol), "source": "manual",
            })
            continue
        payout = _cached_payout(symbol) if symbol else None
        out.append({
            "key": key,
            "symbol": symbol,
            "name": company or "",
            "annualPayout": payout,
            "currency": currency_for(symbol),
            "source": "yahoo" if payout is not None else "unresolved",
        })
    return out
