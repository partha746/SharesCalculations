"""Pune gold and silver rates.

Primary source is GoodReturns' per-city pages, which quote Pune rates for 24K/22K/18K gold
and silver. They are HTML, not an API, so the parser is deliberately loose: it looks for the
rupee figure attached to each purity phrase rather than relying on page structure, and every
value is sanity-checked before being returned.

oropocket's keyless API is the fallback. It is India-wide rather than Pune and quotes a
dealer buy price, so it is only used when the scrape yields nothing; responses say which
source they came from.
"""
import html as _html
import re
import time

import requests

_GOLD_URL = "https://www.goodreturns.in/gold-rates/pune.html"
_SILVER_URL = "https://www.goodreturns.in/silver-rates/pune.html"
_FALLBACK_URL = "https://api.oropocket.com/public/prices"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}
_TIMEOUT_SEC = 20

# Guard rails. A parse that lands outside these is a broken page, not a price move, and
# would otherwise be recorded as history and skew every chart after it.
_SANE_RANGE_PER_GRAM = {
    "gold": (1000.0, 100000.0),
    "silver": (10.0, 10000.0),
}

_CACHE = {}
_CACHE_TTL_SEC = 600  # rates move slowly; a 10-minute cache is plenty


def _to_float(text):
    try:
        return float(str(text).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _plain_text(markup):
    """Strip tags/scripts and decode entities so the rupee figures are greppable."""
    body = re.sub(r"<script.*?</script>|<style.*?</style>", " ", markup, flags=re.S | re.I)
    body = re.sub(r"<[^>]+>", " ", body)
    return re.sub(r"\s+", " ", _html.unescape(body))


def _sane(metal, value):
    lo, hi = _SANE_RANGE_PER_GRAM[metal]
    return value is not None and lo <= value <= hi


def _fetch(url):
    resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT_SEC)
    resp.raise_for_status()
    return _plain_text(resp.text)


def _parse_gold(text):
    """{'24k': 15332.0, '22k': 14054.0, '18k': 11499.0} for whatever purities are present."""
    out = {}
    for karat in (24, 22, 18):
        m = re.search(
            r"\u20b9\s*([\d,]+(?:\.\d+)?)\s*per gram for %d\s*karat" % karat, text, re.I
        )
        value = _to_float(m.group(1)) if m else None
        if _sane("gold", value):
            out["%dk" % karat] = value
    return out


def _parse_silver(text):
    """Per-gram silver. Falls back to the per-kilogram figure when the gram one is missing."""
    m = re.search(r"\u20b9\s*([\d,]+(?:\.\d+)?)\s*per gram", text, re.I)
    value = _to_float(m.group(1)) if m else None
    if _sane("silver", value):
        return value
    m = re.search(r"\u20b9\s*([\d,]+(?:\.\d+)?)\s*per kilogram", text, re.I)
    per_kg = _to_float(m.group(1)) if m else None
    if per_kg:
        value = per_kg / 1000.0
        if _sane("silver", value):
            return value
    return None


def _fallback_rates():
    """India-wide dealer quote, used only when the Pune pages cannot be parsed."""
    try:
        resp = requests.get(_FALLBACK_URL, headers={"Accept": "application/json"}, timeout=_TIMEOUT_SEC)
        data = (resp.json() or {}).get("data") or {}
    except Exception as e:
        print("[metals] fallback failed: {}".format(e), flush=True)
        return None
    gold = _to_float((data.get("gold") or {}).get("buy"))
    silver = _to_float((data.get("silver") or {}).get("buy"))
    if not _sane("gold", gold) and not _sane("silver", silver):
        return None
    return {
        "source": "oropocket (India, dealer buy)",
        "city": "India",
        "gold": {"24k": gold} if _sane("gold", gold) else {},
        "silver": silver if _sane("silver", silver) else None,
    }


def get_rates(refresh=False):
    """Current Pune rates: {source, city, gold: {24k,22k,18k}, silver, fetchedAt, stale}.

    `silver` and each gold purity are INR per gram. Returns None only when both the Pune
    pages and the fallback API fail; a partially parsed page still returns what it found.
    """
    now = time.time()
    cached = _CACHE.get("rates")
    if cached and not refresh and (now - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]

    gold, silver = {}, None
    try:
        gold = _parse_gold(_fetch(_GOLD_URL))
    except Exception as e:
        print("[metals] Pune gold page: {}".format(e), flush=True)
    try:
        silver = _parse_silver(_fetch(_SILVER_URL))
    except Exception as e:
        print("[metals] Pune silver page: {}".format(e), flush=True)

    if gold or silver is not None:
        rates = {
            "source": "GoodReturns (Pune)",
            "city": "Pune",
            "gold": gold,
            "silver": silver,
        }
    else:
        rates = _fallback_rates()
        if rates is None:
            # Serve the last good value rather than nothing, flagged as stale.
            if cached:
                stale = dict(cached[1])
                stale["stale"] = True
                return stale
            return None

    rates["fetchedAt"] = int(now * 1000)
    rates["stale"] = False
    _CACHE["rates"] = (now, rates)
    return rates
