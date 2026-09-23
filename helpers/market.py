"""USD->INR FX rates + NVDA stock/live/pre-post prices (Finnhub, Frankfurter, Nasdaq, yfinance)."""
import datetime as dt
import locale
import re
import sqlite3
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from retrying import retry

from helpers import config
from helpers.db import DB

# Cache for pre/post-market price to avoid rate limits (key -> (timestamp, price))
_yf_extended_price_cache = {}
_YF_CACHE_TTL_SEC = 90  # reuse result for 90s

# Nasdaq's own quote feed, which powers nasdaq.com. Undocumented but keyless and real-time
# during extended hours; Yahoo (yfinance) now rate-limits this host on every request, so
# Nasdaq is tried first and yfinance is kept only as a fallback for when it recovers.
_NASDAQ_QUOTE_URL = "https://api.nasdaq.com/api/quote/{symbol}/info"
_NASDAQ_EXTENDED_URL = "https://api.nasdaq.com/api/quote/{symbol}/extended-trading"
_NASDAQ_HEADERS = {
    # The endpoint returns 403 to non-browser agents.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
_NASDAQ_TIMEOUT_SEC = 12


def _parse_money(value):
    """'$217.9105' / '1,234.50' -> float. None when blank or unparseable."""
    if value is None:
        return None
    text = str(value).replace("$", "").replace(",", "").strip()
    if not text or text.upper() in ("N/A", "NA", "--"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # host scripts may still be on 3.8
    ZoneInfo = None


def _et_clock_to_ist(day_text, clock_text):
    """'Sep 17, 2026' + '05:11:31 PM' in ET -> '02:41:31 AM' in IST.

    Nasdaq quotes extended-hours timestamps as a bare ET wall clock, so the session's date
    is needed to shift it. Returns the original string when it cannot be parsed.
    """
    if not clock_text:
        return clock_text
    try:
        naive = datetime.strptime("{} {}".format(day_text, clock_text), "%b %d, %Y %I:%M:%S %p")
    except (ValueError, TypeError):
        return clock_text
    if ZoneInfo is None:
        # ET is UTC-4 (EDT) or UTC-5 (EST); IST is UTC+5:30, so EDT differs by 9h30m.
        return (naive + timedelta(hours=9, minutes=30)).strftime("%I:%M:%S %p")
    et = naive.replace(tzinfo=ZoneInfo("America/New_York"))
    return et.astimezone(ZoneInfo("Asia/Kolkata")).strftime("%I:%M:%S %p")


def _parse_priced_at(value):
    """'$223.9437 (05:11:31 PM)' -> (223.9437, '05:11:31 PM')."""
    text = str(value or "")
    at = ""
    if "(" in text and ")" in text:
        at = text[text.index("(") + 1:text.rindex(")")].strip()
        text = text[:text.index("(")]
    return _parse_money(text), at


def _parse_as_of(text):
    """'Data last updated Sep 21, 2026 09:30 AM ET.' -> (epoch_ms, 'Sep 21, 2026').

    Nasdaq keeps a session's table readable until shortly before it next opens, so during
    Monday's regular hours the post table still holds Friday's numbers while the pre table
    holds this morning's. Callers need this to tell which one is actually the latest.
    """
    m = re.search(r"([A-Z][a-z]{2} \d{1,2}, \d{4})(?:\s+(\d{1,2}:\d{2}\s*[AP]M))?", str(text or ""))
    if not m:
        return None, ""
    day, clock = m.group(1), m.group(2)
    try:
        when = datetime.strptime(
            "{} {}".format(day, clock) if clock else day,
            "%b %d, %Y %I:%M %p" if clock else "%b %d, %Y",
        )
    except ValueError:
        return None, day
    # Timestamps are quoted in ET; treat them as UTC-4/5 only to order sessions, which is
    # all this is used for, so the exact offset does not matter.
    return int((when - datetime(1970, 1, 1)).total_seconds() + 4 * 3600) * 1000, day


def _parse_consolidated(value):
    """'$219.2308 +5.3308 (+2.49%)' -> (219.2308, 5.3308, 2.49)."""
    parts = str(value or "").split()
    price = _parse_money(parts[0]) if parts else None
    change = _parse_money(parts[1]) if len(parts) > 1 else None
    pct = None
    if len(parts) > 2:
        pct = _parse_money(parts[2].strip("()%").replace("+", ""))
    return price, change, pct


class RupeeConv:
    """_summary_

    Returns:
        _type_: _description_
    """

    def __init__(self) -> None:
        """_summary_
        """
        self.db_obj = DB()
        self.todays_date = datetime.now()
        self.rupee_symbol = u'\u20B9'
        # Regular-session high/low, filled in by get_live_price() from the same quote.
        self.day_high = None
        self.day_low = None
        
        self.pre_start_time = datetime.time(datetime(2022, 3, 21, 16, 30))
        self.pre_stop_time = datetime.time(datetime(2022, 3, 21, 19, 0))
        self.post_start_time = datetime.time(datetime(2022, 3, 21, 19, 0))
        self.post_stop_time = datetime.time(datetime(2022, 3, 21, 1, 30))

    @retry(wait_random_min=10, stop_max_attempt_number=3)
    def get_rupee_rate(self, date):
        def fetch_rate_for_day(date_str):
            url = f"{config.FRANKFURTER_BASE}/{date_str}"
            params = {
                "from": "USD",
                "to": "INR"
            }
            response = requests.get(url, params=params)
            data = response.json()
            rate = data.get("rates", {}).get("INR")
            if rate:
                return round(rate, 2)
            else:
                print(f"Error retrieving rate for {date_str}: {data}")
                return None

        # Handle pandas Series
        if isinstance(date, pd.Series):
            return [self.get_rupee_rate(d) for d in date]

        # Handle single date (check datetime before dt.date — datetime subclasses date).
        if isinstance(date, datetime):
            date_str = date.strftime('%Y-%m-%d')
        elif isinstance(date, dt.date):
            date_str = date.strftime('%Y-%m-%d')
        elif isinstance(date, str):
            try:
                if '/' in date:
                    date_obj = datetime.strptime(date, "%m/%d/%Y")
                else:
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                date_str = date_obj.strftime('%Y-%m-%d')
            except Exception as e:
                print(f"Invalid date format: {date} -> {e}")
                return None
        else:
            raise ValueError(f"Unsupported date format: {type(date)}")

        return fetch_rate_for_day(date_str)

    def get_usd_to_inr_open_er_api(self):
        """USD→INR from ExchangeRate-API (open.er-api.com). Updates more often than ECB-only Frankfurter."""
        try:
            response = requests.get(config.ER_API_URL, timeout=15)
            data = response.json()
            if data.get("result") == "success":
                rates = data.get("rates") or {}
                rate = rates.get("INR")
                if rate is not None:
                    return float(rate)
        except Exception as e:
            print(f"open.er-api USD/INR: {e}")
        return None

    @retry(wait_random_min=10, stop_max_attempt_number=3)
    def get_latest_usd_to_inr(self):
        """Fetch the latest USD→INR rate (for live display / refresh). Uses Frankfurter 'latest' endpoint."""
        url = config.FRANKFURTER_LATEST_URL
        params = {"base": "USD", "symbols": "INR"}
        try:
            response = requests.get(url, params=params)
            data = response.json()
            rate = data.get("rates", {}).get("INR")
            if rate is not None:
                return round(float(rate), 2)
        except Exception as e:
            print(f"Error retrieving latest USD/INR rate: {e}")
        return None

    def get_usd_to_inr_for_prior_calendar_day(self, max_days_back=7):
        """USD→INR for a day before today, same Frankfurter source as get_rupee_rate (not the live/latest tick).

        Steps back through calendar days (handles weekends/holidays where a given date has no fix).
        """
        from datetime import date, timedelta

        today = date.today()
        for days_back in range(1, max_days_back + 1):
            d = today - timedelta(days=days_back)
            r = self.get_rupee_rate(d)
            if r is not None:
                return round(float(r), 2)
        return None

    def get_stock_price(self, stock_code='NVDA', api_key=None):
        """
        Fetches the latest stock price for a given symbol using Finnhub.io.

        Parameters:
            stock_code (str): Stock ticker symbol (e.g., 'NVDA', 'AAPL')
            api_key (str): Finnhub.io API key (defaults to config.FINNHUB_TOKEN)

        Returns:
            float: Latest stock price, or None if not available
        """
        quote = self.get_quote(stock_code, api_key)
        if quote is None:
            return None
        price = quote.get("c")
        if price is not None:
            return round(float(price), 2)
        return None

    def get_quote(self, stock_code='NVDA', api_key=None):
        """Fetches the Finnhub quote (c, o, h, l, pc, t) for the symbol. Returns dict or None."""
        url = "https://finnhub.io/api/v1/quote"
        params = {"symbol": stock_code.upper(), "token": api_key or config.FINNHUB_TOKEN}
        try:
            response = requests.get(url, params=params)
            data = response.json()
            if isinstance(data, dict) and data.get("c") is not None:
                return data
            return None
        except Exception as e:
            print(f"Error retrieving quote for {stock_code}:", e)
            return None

    def _yf_session(self):
        """Requests session with SSL verify disabled for environments where cert verification fails (e.g. corporate)."""
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
        s = requests.Session()
        s.verify = False
        return s

    def nasdaq_quote(self, symbol="NVDA"):
        """Nasdaq's live quote: {status, price, prevClose, timestamp, realTime} or None.

        During extended hours `primaryData` carries the pre/post-market print and
        `secondaryData` the last regular close; `marketStatus` says which session we are in
        ('Pre-Market', 'After Hours', 'Market Open', 'Closed'), so callers do not have to
        infer the window from the clock.
        """
        try:
            session = self._yf_session()  # shares the relaxed-TLS session used elsewhere
            resp = session.get(
                _NASDAQ_QUOTE_URL.format(symbol=symbol.upper()),
                params={"assetclass": "stocks"},
                headers=_NASDAQ_HEADERS,
                timeout=_NASDAQ_TIMEOUT_SEC,
            )
            if resp.status_code != 200:
                print(f"[nasdaq] {symbol}: HTTP {resp.status_code}")
                return None
            data = (resp.json() or {}).get("data") or {}
        except Exception as e:
            print(f"[nasdaq] {symbol}: {e}")
            return None

        primary = data.get("primaryData") or {}
        secondary = data.get("secondaryData") or {}
        price = _parse_money(primary.get("lastSalePrice"))
        if price is None or price <= 0:
            return None
        return {
            "status": (data.get("marketStatus") or "").strip(),
            "price": round(price, 2),
            "prevClose": _parse_money(secondary.get("lastSalePrice")),
            "timestamp": (primary.get("lastTradeTimestamp") or "").strip(),
            "realTime": bool(primary.get("isRealTime")),
        }

    def extended_session_stats(self, session, symbol="NVDA"):
        """Pre/post session last, change, high, low and volume from Nasdaq. None if no data.

        `session` is 'pre' or 'post'. Nasdaq resets a session's table shortly before it next
        opens, so pre-market figures are readable from 4:00 AM ET until the following
        pre-open, and post-market from 4:00 PM ET until the following post-open.

        The session *open* is not available here: the trade table is capped at the last 100
        trades per half-hour bucket, which for a liquid symbol covers only the final seconds
        of the bucket. `extended_price_history` (recorded locally) supplies that instead.
        """
        if session not in ("pre", "post"):
            return None
        try:
            resp = self._yf_session().get(
                _NASDAQ_EXTENDED_URL.format(symbol=symbol.upper()),
                params={"assetclass": "stocks", "markettype": session},
                headers=_NASDAQ_HEADERS,
                timeout=_NASDAQ_TIMEOUT_SEC,
            )
            if resp.status_code != 200:
                print(f"[nasdaq] {symbol} {session}-market: HTTP {resp.status_code}")
                return None
            data = (resp.json() or {}).get("data") or {}
        except Exception as e:
            print(f"[nasdaq] {symbol} {session}-market: {e}")
            return None

        rows = ((data.get("infoTable") or {}).get("rows")) or []
        if not rows:
            return None  # session has not started, or its table has been reset
        row = rows[0]
        last, change, change_pct = _parse_consolidated(row.get("consolidated"))
        if last is None:
            return None
        high, high_at = _parse_priced_at(row.get("highPrice"))
        low, low_at = _parse_priced_at(row.get("lowPrice"))
        volume = _parse_money(row.get("volume"))
        # "Market Close: $213.9". For a pre-market session that is the right reference (the
        # prior day's close), but for post-market Nasdaq still reports the *previous* day's
        # close, which folds the whole regular session into the change. Post-market has to be
        # measured against the close that session opened from.
        ref_close = _parse_money(str(data.get("previousInfo") or "").split(":")[-1])
        if session == "post":
            quote = self.nasdaq_quote(symbol)
            if quote:
                # secondaryData holds the regular close while an extended session is running;
                # once everything is shut, primaryData is that same close.
                regular_close = quote.get("prevClose") or quote.get("price")
                if regular_close:
                    ref_close = regular_close
        if ref_close:
            change = round(last - ref_close, 4)
            change_pct = round(change / ref_close * 100, 2)
        updates = data.get("lastUpdateInfo") or []
        as_of_ms, as_of_date = _parse_as_of(updates[0] if updates else "")
        # Surface the high/low clock in IST; the raw feed quotes them in ET.
        high_at = _et_clock_to_ist(as_of_date, high_at)
        low_at = _et_clock_to_ist(as_of_date, low_at)
        return {
            "asOfMs": as_of_ms,
            "asOfDate": as_of_date,
            "session": session,
            "last": last,
            "change": change,
            "changePct": change_pct,
            "high": high,
            "highAt": high_at,
            "low": low,
            "lowAt": low_at,
            "volume": int(volume) if volume else None,
            "prevClose": ref_close,
            "asOf": (updates[0] if updates else "").strip(),
        }

    def _nasdaq_session_price(self, symbol, wanted):
        """(answered, price) for the requested session. `wanted` is 'pre' or 'post'.

        `answered` is False only when Nasdaq itself was unreachable, so the caller knows to
        try yfinance. When Nasdaq answers but reports a different session we return
        (True, None): it is authoritative about the session, and falling through to a
        rate-limited Yahoo would just stall the caller for several seconds every poll.
        """
        quote = self.nasdaq_quote(symbol)
        if not quote:
            return False, None
        status = quote["status"].lower()
        in_session = ("pre" in status) if wanted == "pre" else ("after" in status or "post" in status)
        return True, (quote["price"] if in_session else None)

    def get_premarket_price(self, symbol="NVDA"):
        """Current pre-market price (USD), or None. Use during pre-market (4–9:30 AM ET).

        Nasdaq first, yfinance as a fallback.
        """
        sym = symbol.upper()
        cache_key = f"pre_{sym}"
        now_utc = time.time()
        cached = _yf_extended_price_cache.get(cache_key)
        if cached is not None:
            ts, price = cached
            if (now_utc - ts) < _YF_CACHE_TTL_SEC and price is not None:
                return price
        answered, price = self._nasdaq_session_price(sym, "pre")
        if price is not None:
            _yf_extended_price_cache[cache_key] = (now_utc, price)
            return price
        if answered:
            return None
        return self._get_premarket_price_yf(sym)

    def _get_premarket_price_yf(self, symbol="NVDA"):
        """Fallback pre-market lookup via yfinance. The caller owns the cache read."""
        try:
            import yfinance as yf
        except ImportError:
            return None
        sym = symbol.upper()
        cache_key = f"pre_{sym}"
        now_utc = time.time()
        session = self._yf_session()
        try:
            # 1) Try ticker.info preMarketPrice first (one request; works in newer yfinance)
            ticker = yf.Ticker(sym, session=session)
            info = ticker.info
            if isinstance(info, dict):
                pm = info.get("preMarketPrice")
                if pm is not None and float(pm) > 0:
                    price = round(float(pm), 2)
                    _yf_extended_price_cache[cache_key] = (now_utc, price)
                    return price
            # 2) Fallback: download with prepost, use last bar if within 24h (relaxed from 12h for delays)
            df = yf.download(
                sym, period="5d", interval="1m", prepost=True, progress=False,
                timeout=15, auto_adjust=True, threads=False, session=session
            )
            if df is None or df.empty:
                _yf_extended_price_cache[cache_key] = (now_utc, None)
                return None
            close_col = df["Close"] if "Close" in df.columns else df.iloc[:, 3]
            last_close = close_col.iloc[-1]
            if last_close is None or float(last_close) <= 0:
                _yf_extended_price_cache[cache_key] = (now_utc, None)
                return None
            last_ts = df.index[-1]
            if hasattr(last_ts, "timestamp"):
                last_sec = last_ts.timestamp()
            else:
                try:
                    last_sec = pd.Timestamp(last_ts).timestamp()
                except Exception:
                    last_sec = now_utc - 3600
            if (now_utc - last_sec) >= 24 * 3600:
                _yf_extended_price_cache[cache_key] = (now_utc, None)
                return None
            price = round(float(last_close), 2)
            _yf_extended_price_cache[cache_key] = (now_utc, price)
            return price
        except Exception as e:
            print(f"yfinance pre-market for {sym}: {e}")
            _yf_extended_price_cache[cache_key] = (now_utc, None)
            return None

    def get_postmarket_price(self, symbol="NVDA"):
        """Current post-market price (USD), or None. Use during post-market (4–8 PM ET).

        Nasdaq first, yfinance as a fallback.
        """
        sym = symbol.upper()
        cache_key = f"post_{sym}"
        now_utc = time.time()
        cached = _yf_extended_price_cache.get(cache_key)
        if cached is not None:
            ts, price = cached
            if (now_utc - ts) < _YF_CACHE_TTL_SEC and price is not None:
                return price
        answered, price = self._nasdaq_session_price(sym, "post")
        if price is not None:
            _yf_extended_price_cache[cache_key] = (now_utc, price)
            return price
        if answered:
            return None
        return self._get_postmarket_price_yf(sym)

    def _get_postmarket_price_yf(self, symbol="NVDA"):
        """Fallback post-market lookup via yfinance. The caller owns the cache read."""
        try:
            import yfinance as yf
        except ImportError:
            return None
        sym = symbol.upper()
        cache_key = f"post_{sym}"
        now_utc = time.time()
        session = self._yf_session()
        try:
            df = yf.download(
                sym, period="5d", interval="1m", prepost=True, progress=False,
                timeout=15, auto_adjust=True, threads=False, session=session
            )
            if df is None or df.empty:
                _yf_extended_price_cache[cache_key] = (now_utc, None)
                return None
            close_col = df["Close"] if "Close" in df.columns else df.iloc[:, 3]
            last_close = close_col.iloc[-1]
            if last_close is None or float(last_close) <= 0:
                _yf_extended_price_cache[cache_key] = (now_utc, None)
                return None
            last_ts = df.index[-1]
            if hasattr(last_ts, "timestamp"):
                last_sec = last_ts.timestamp()
            else:
                try:
                    last_sec = pd.Timestamp(last_ts).timestamp()
                except Exception:
                    last_sec = now_utc - 3600
            if (now_utc - last_sec) >= 12 * 3600:
                _yf_extended_price_cache[cache_key] = (now_utc, None)
                return None
            price = round(float(last_close), 2)
            _yf_extended_price_cache[cache_key] = (now_utc, price)
            return price
        except Exception as e:
            print(f"yfinance post-market for {sym}: {e}")
            _yf_extended_price_cache[cache_key] = (now_utc, None)
            return None

    @retry(wait_random_min=10, stop_max_attempt_number=3)
    def get_live_price(self):
        """Returns (live_price_usd, todays_usd_inr_rate, open_price_usd or None, prev_close_usd or None).

        The session high/low from the same quote are left on `day_high` / `day_low` rather than
        widening this tuple, which several callers unpack positionally. get_quote() is not
        cached, so reading them here avoids a second request for the same data.
        """
        quote = self.get_quote(stock_code='nvda')
        live_price = None
        open_price = None
        prev_close = None
        self.day_high = None
        self.day_low = None
        if quote:
            c = quote.get("c")
            o = quote.get("o")
            pc = quote.get("pc")
            h = quote.get("h")
            l = quote.get("l")
            if c is not None:
                live_price = round(float(c), 2)
            if o is not None and float(o) > 0:
                open_price = round(float(o), 2)
            if pc is not None and float(pc) > 0:
                prev_close = round(float(pc), 2)
            if h is not None and float(h) > 0:
                self.day_high = round(float(h), 2)
            if l is not None and float(l) > 0:
                self.day_low = round(float(l), 2)
        if live_price is None:
            live_price = self.get_stock_price(stock_code='nvda')
        todays_rp = self.get_usd_to_inr_open_er_api()
        if todays_rp is None:
            todays_rp = self.get_latest_usd_to_inr()
            if todays_rp is not None:
                todays_rp = float(todays_rp)
        if todays_rp is None:
            todays_rp = self.get_rupee_rate(self.todays_date)
        return live_price, todays_rp, open_price, prev_close

    def print_rupees(self, amt, cur='INR'):
        if cur == 'INR':
            locale.setlocale(locale.LC_MONETARY, 'en_IN.UTF-8')
        elif cur == 'USD':
            locale.setlocale(locale.LC_MONETARY, 'en_US.UTF-8')

        if isinstance(amt, pd.Series):
            rupees = []
            for amt in amt:
                rupees.append(locale.currency(amt, grouping=True).replace('?','₹'))
            return rupees
        elif isinstance(amt, list):
            rupees = []
            for amt in amt:
                rupees.append(locale.currency(amt, grouping=True).replace('?','₹'))
            return rupees

        return locale.currency(amt, grouping=True).replace('?','₹')

    def update_null_rupees_rate(self, table_name, date_column_name, rs_column_name):
        """_summary_

        Args:
            table_name (_type_): _description_
            date_column_name (_type_): _description_
            rs_column_name (_type_): _description_
        """
        conn = sqlite3.connect(self.db_obj.db_path)
        db_cursor = conn.cursor()
        null_df = 'SELECT ' + date_column_name + ' FROM ' + \
            table_name + ' where ' + rs_column_name + ' IS NULL;'
        query = db_cursor.execute(null_df)
        cols = [column[0] for column in query.description]
        rs_rate_null_df = pd.DataFrame.from_records(
            data=query.fetchall(), columns=cols)

        if len(rs_rate_null_df) > 0:
            rs_rate_null_df[rs_column_name] = self.get_rupee_rate(
                rs_rate_null_df[date_column_name][0])

            for _, row in rs_rate_null_df.iterrows():
                cmd = 'UPDATE ' + table_name + ' SET ' + rs_column_name + ' = \'' + \
                    str(row[rs_column_name]) + '\' where ' + date_column_name + \
                    ' = \'' + str(row[date_column_name]) + '\''
                db_cursor.execute(cmd)

            conn.commit()
        conn.close()

