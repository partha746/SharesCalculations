"""Look-through overlap analysis for the holdings in the `mf_holdings` table.

Fund portfolios come from FinAPI (finapi.upvaly.com), which republishes the monthly
stock-level portfolios every Indian AMC files with SEBI. Only the fields this module
needs are cached, keyed by AMFI scheme code, in `configs/mf_portfolio_cache.json`.

The analysis itself is recomputed from the current `mf_holdings` rows on every call,
so adding, editing or deleting a fund is reflected immediately; only the third-party
portfolio fetch is cached, and only for schemes already seen.

Two numbers matter here:

* Overlap between a pair of funds - sum over shared stocks of min(weight_a, weight_b),
  the usual portfolio-overlap measure. 40% means 40p of every rupee buys the same
  companies in the same proportions in both funds.
* Look-through exposure - each fund's current value split across its underlying stocks
  and summed, so two funds that both hold a stock reveal the real single-name risk.
"""
import collections
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

from helpers import config
from helpers.db import DB

_API = "https://finapi.upvaly.com/api/mf/scheme-code/{code}"
_CACHE_PATH = os.path.join(config.REPO_ROOT, "configs", "mf_portfolio_cache.json")
# AMCs publish portfolios monthly; a week keeps us close without hammering the API.
_CACHE_TTL_SEC = 7 * 24 * 3600
_FETCH_TIMEOUT_SEC = 25
# A fund is treated as equity only above this disclosed equity allocation. Debt and
# liquid funds list bond issuers whose names look like companies and whose disclosed
# weights do not sum to 100, so they must never enter an equity figure.
_EQUITY_ALLOC_MIN_PCT = 50.0

# Cash, repo, sovereign and housekeeping lines that are not company equity.
_NON_EQUITY = re.compile(
    r"treps|^trp[_ ]|tri[- ]party|repo|net receivab|net current asset|net payab|"
    r"^cash|cash margin|cash offset|clearing corp|^ccil|treasury bill|t[- ]bill|"
    r"govt stock|government of india|^g[- ]?sec|state develop|^sdl|margin deposit|"
    r"fixed deposit|^deposit|liquid fund|overnight fund|mutual fund unit|"
    r"^unclaimed|corporate debt|securitisation trust|commercial paper|"
    r"certificate of deposit|zero coupon|^nil$",
    re.I,
)

# Suffixes that differ between AMCs for the same company.
_SUFFIX = re.compile(
    r"\b(ltd|limited|ltd\.|pvt|private|the|co|corp|corporation|company|inc|plc|"
    r"class a|class b|ordinary shares|equity shares|shares|holdings?)\b",
    re.I,
)


def canonical(name):
    """'HDFC Bank Limited' / 'HDFC Bank Ltd.' -> 'hdfc bank' so AMCs can be compared."""
    n = re.sub(r"[*@#]+", " ", name or "")
    n = _SUFFIX.sub(" ", n.lower())
    n = re.sub(r"[^a-z0-9&]+", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def _as_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _load_cache():
    try:
        with open(_CACHE_PATH) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_cache(cache):
    try:
        tmp = _CACHE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(cache, f)
        os.replace(tmp, _CACHE_PATH)
    except OSError:
        pass  # a read-only configs mount just means we re-fetch next time


def _trim(payload):
    """Keep only the fields the analysis needs; the raw FinAPI response is ~30x larger."""
    alloc = (payload.get("portfolio") or {}).get("assetAllocation") or {}
    holdings = []
    for h in payload.get("holdings") or []:
        name = (h.get("name") or "").strip()
        weight = _as_float(h.get("weightage"))
        if not name or weight <= 0:
            continue
        holdings.append({"n": name, "s": (h.get("sector") or "").strip(), "w": weight})
    return {
        "fetchedAt": time.time(),
        "name": payload.get("schemeName") or "",
        "category": payload.get("schemeCategory") or "",
        "nav": _as_float(payload.get("latestNav")),
        "navDate": payload.get("latestNavDate") or "",
        "equityAllocPct": _as_float(alloc.get("equityAllocation")),
        "holdings": holdings,
    }


def _fetch_one(code):
    with urllib.request.urlopen(_API.format(code=code), timeout=_FETCH_TIMEOUT_SEC) as r:
        return _trim(json.load(r).get("data") or {})


def portfolios_for(codes, refresh=False):
    """{scheme_code: trimmed portfolio}. Fetches only what is missing or stale."""
    cache = _load_cache()
    now = time.time()
    wanted = []
    for code in codes:
        entry = cache.get(code)
        fresh = (
            isinstance(entry, dict)
            and entry.get("holdings")
            and (now - _as_float(entry.get("fetchedAt"))) < _CACHE_TTL_SEC
        )
        if refresh or not fresh:
            wanted.append(code)

    fetched = 0
    for i, code in enumerate(wanted):
        try:
            cache[code] = _fetch_one(code)
            fetched += 1
        except Exception:
            cache.setdefault(code, {})  # leave a marker; the fund shows as unavailable
        if i < len(wanted) - 1:
            time.sleep(0.4)  # be polite to a free API
    if fetched:
        _save_cache(cache)
    return {code: cache.get(code) or {} for code in codes}


def _equity_weights(portfolio):
    """{canonical_stock: (display_name, weight_pct)} or {} for a non-equity fund."""
    if _as_float(portfolio.get("equityAllocPct")) <= _EQUITY_ALLOC_MIN_PCT:
        return {}
    out = {}
    for h in portfolio.get("holdings") or []:
        raw = h.get("n") or ""
        key = canonical(raw)
        if not key or _NON_EQUITY.search(raw):
            continue
        display, acc = out.get(key, (re.sub(r"[*@#]+", "", raw).strip(), 0.0))
        out[key] = (display, acc + _as_float(h.get("w")))
    return out


def _overlap_pct(a, b):
    return sum(min(a[k][1], b[k][1]) for k in set(a) & set(b))


def _is_equity(f):
    return bool(f["weights"])


def _pair_overlaps(funds, within_account):
    """Overlap for every pair of equity funds, within one account or across accounts.

    Across accounts the pair is keyed by scheme, so a fund held in three accounts
    contributes one row per counterpart rather than three identical ones.
    """
    out = []
    seen = set()
    eq = [f for f in funds if _is_equity(f)]
    for i, a in enumerate(eq):
        for b in eq[i + 1:]:
            same = a["account"] == b["account"]
            if within_account != same or a["schemeCode"] == b["schemeCode"]:
                continue
            if not within_account:
                key = (a["schemeCode"], b["schemeCode"]) if a["schemeCode"] < b["schemeCode"] \
                    else (b["schemeCode"], a["schemeCode"])
                if key in seen:
                    continue
                seen.add(key)
            shared = set(a["weights"]) & set(b["weights"])
            if not shared:
                continue
            top = sorted(
                ((a["weights"][k][0], min(a["weights"][k][1], b["weights"][k][1])) for k in shared),
                key=lambda x: -x[1],
            )[:5]
            out.append({
                "a": a["name"], "b": b["name"],
                "accountA": a["account"], "accountB": b["account"],
                "overlapPct": round(_overlap_pct(a["weights"], b["weights"]), 2),
                "sharedStocks": len(shared),
                "topShared": [n for n, _ in top],
            })
    out.sort(key=lambda x: -x["overlapPct"])
    return out


def _look_through(funds):
    """Rupee exposure per underlying stock, aggregated across the given funds."""
    agg = collections.defaultdict(lambda: {"name": "", "inr": 0.0, "accounts": set(), "n": 0})
    for f in funds:
        for key, (display, weight) in f["weights"].items():
            e = agg[key]
            e["name"] = e["name"] or display
            e["inr"] += f["value"] * weight / 100.0
            e["accounts"].add(f["account"])
            e["n"] += 1
    rows = [{"name": e["name"], "inr": round(e["inr"], 2), "fundCount": e["n"],
             "accounts": sorted(e["accounts"])} for e in agg.values()]
    rows.sort(key=lambda r: -r["inr"])
    return rows


def _sectors(funds):
    agg = collections.defaultdict(float)
    for f in funds:
        if not _is_equity(f):
            continue
        for h in f["portfolio"].get("holdings") or []:
            raw = h.get("n") or ""
            if _NON_EQUITY.search(raw):
                continue
            sector = (h.get("s") or "").strip() or "Uncategorised"
            agg[sector] += f["value"] * _as_float(h.get("w")) / 100.0
    rows = [{"sector": k, "inr": round(v, 2)} for k, v in agg.items()]
    rows.sort(key=lambda r: -r["inr"])
    return rows


def _effective_stock_count(rows):
    """1 / sum(share^2): how many equally weighted stocks this concentration is worth."""
    total = sum(r["inr"] for r in rows)
    if total <= 0:
        return 0.0
    hhi = sum((r["inr"] / total) ** 2 for r in rows)
    return round(1.0 / hhi, 1) if hhi else 0.0


def _pct(part, whole):
    return round(part / whole * 100, 2) if whole else 0.0


def compute(nav_lookup=None, refresh=False, top_n=25):
    """Full overlap analysis for every account in `mf_holdings`.

    `nav_lookup(scheme_code) -> {"nav": float}` supplies the current NAV; pass the
    dashboard's cached AMFI lookup so fund values match the Mutual funds tab. When
    omitted, the NAV recorded alongside the cached portfolio is used instead.
    """
    df = DB().get_table_data("mf_holdings")
    rows = df.to_dict("records") if not df.empty else []
    codes = sorted({str(r.get("scheme_code")) for r in rows if r.get("scheme_code")})
    if not codes:
        return {
            "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "accounts": [], "household": [], "crossAccount": [], "sectors": [],
            "duplicateSchemes": [], "unavailable": [],
            "equityTotalInr": 0, "grandTotalInr": 0, "debtTotalInr": 0,
            "distinctStocks": 0, "effectiveStocks": 0,
        }

    portfolios = portfolios_for(codes, refresh=refresh)

    funds, unavailable = [], []
    for r in rows:
        code = str(r.get("scheme_code"))
        p = portfolios.get(code) or {}
        units = _as_float(r.get("units"))
        nav = 0.0
        if nav_lookup is not None:
            try:
                nav = _as_float((nav_lookup(code) or {}).get("nav"))
            except Exception:
                nav = 0.0
        if nav <= 0:
            nav = _as_float(p.get("nav"))
        name = p.get("name") or (r.get("scheme_name") or "")
        if not p.get("holdings"):
            unavailable.append({"schemeCode": code, "schemeName": name,
                                "account": str(r.get("account") or "1")})
            continue
        funds.append({
            "account": str(r.get("account") or "1"),
            "schemeCode": code,
            "name": name,
            "category": p.get("category") or "",
            "value": round(units * nav, 2),
            "invested": _as_float(r.get("invested")),
            "equityAllocPct": _as_float(p.get("equityAllocPct")),
            "navDate": p.get("navDate") or "",
            "weights": _equity_weights(p),
            "portfolio": p,
        })
    funds.sort(key=lambda f: (f["account"], -f["value"]))

    equity_funds = [f for f in funds if _is_equity(f)]
    equity_total = sum(f["value"] for f in equity_funds)
    grand_total = sum(f["value"] for f in funds)
    household = _look_through(equity_funds)

    accounts = []
    for acct in sorted({f["account"] for f in funds}):
        af = [f for f in funds if f["account"] == acct]
        eq = [f for f in af if _is_equity(f)]
        eq_val = sum(f["value"] for f in eq)
        lt = _look_through(eq)
        accounts.append({
            "account": acct,
            "totalInr": round(sum(f["value"] for f in af), 2),
            "equityInr": round(eq_val, 2),
            "fundCount": len(af),
            "equityFundCount": len(eq),
            "distinctStocks": len(lt),
            "effectiveStocks": _effective_stock_count(lt),
            "funds": [{
                "schemeCode": f["schemeCode"], "name": f["name"], "category": f["category"],
                "valueInr": f["value"], "investedInr": f["invested"],
                "equityAllocPct": f["equityAllocPct"], "isEquity": _is_equity(f),
                "stockCount": len(f["weights"]),
            } for f in af],
            "pairs": _pair_overlaps(af, within_account=True),
            "topStocks": [{
                "name": s["name"], "inr": s["inr"], "pct": _pct(s["inr"], eq_val),
                "fundCount": s["fundCount"],
            } for s in lt[:top_n]],
        })

    by_scheme = collections.defaultdict(list)
    for f in equity_funds:
        by_scheme[f["schemeCode"]].append(f)
    duplicates = [{
        "schemeCode": code, "name": group[0]["name"],
        "accounts": sorted(f["account"] for f in group),
        "totalInr": round(sum(f["value"] for f in group), 2),
    } for code, group in by_scheme.items() if len(group) > 1]
    duplicates.sort(key=lambda d: -d["totalInr"])

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "equityTotalInr": round(equity_total, 2),
        "grandTotalInr": round(grand_total, 2),
        "debtTotalInr": round(grand_total - equity_total, 2),
        "distinctStocks": len(household),
        "effectiveStocks": _effective_stock_count(household),
        "accounts": accounts,
        "household": [{
            "name": s["name"], "inr": s["inr"], "pct": _pct(s["inr"], equity_total),
            "fundCount": s["fundCount"], "accounts": s["accounts"],
        } for s in household[:top_n]],
        "crossAccount": _pair_overlaps(funds, within_account=False),
        "sectors": [{"sector": s["sector"], "inr": s["inr"], "pct": _pct(s["inr"], equity_total)}
                    for s in _sectors(equity_funds)],
        "duplicateSchemes": duplicates,
        "unavailable": unavailable,
    }
