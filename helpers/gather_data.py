import datetime as dt
import json
import locale
import os
import sqlite3
import sys
import uuid
import warnings
from datetime import date, datetime, timedelta
import requests

import pandas as pd
import pytz
import time
from django.utils.encoding import smart_str
from elasticsearch import Elasticsearch, helpers
from retrying import retry

# Cache for yfinance pre/post-market price to avoid rate limits (key -> (timestamp, price))
_yf_extended_price_cache = {}
_YF_CACHE_TTL_SEC = 90  # reuse result for 90s


class EksHelper:
    """_summary_
    """

    def __init__(self) -> None:
        """_summary_
        """
        self.creds = {
            "id": "O3jMaoMBDsq7mYZZEuYO",
            "name": "Access",
            "api_key": "uFhFnhGTQYaImOOVrhiU_Q",
            "encoded": "TzNqTWFvTUJEc3E3bVlaWkV1WU86dUZoRm5oR1RRWWFJbU9PVnJoaVVfUQ=="
        }
        self.datacleaner_obj = DataCleaner()
        self.tax_obj = Tax()
        rupee_conv_obj = RupeeConv()

        self.livePrice, self.todaysRP, *_ = rupee_conv_obj.get_live_price()

    def login_eks(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        elastic_obj = Elasticsearch(
            'http://' + '192.168.1.235' + ':9200', api_key=(self.creds['id'], self.creds['api_key']))

        return elastic_obj

    def convert_to_kibana_document(self, instance_data, index_name):
        """_summary_

        Args:
            instance_data (_type_): _description_
            index_name (_type_): _description_

        Returns:
            _type_: _description_
        """
        data = [
            {
                "_index": index_name,
                "_id": uuid.uuid1(),
                "_source": each
            }
            for each in instance_data
        ]
        return (data)

    def generate_kibana_data_available_stock(self, type, data, index_name):
        dictionary = data.to_dict('records')
        for each_data in dictionary:
            taxSlab = self.tax_obj.get_tax_slab(each_data['Buy_Date'])
            each_data["current_date"] = datetime.now(pytz.timezone('Asia/Kolkata'))
            each_data["PerShare_INR"] = float(each_data["PerShare_INR_raw"])
            if type == 'ESPP':
                each_data["OwnInvestedMoneyESPP"] = float(each_data["OwnInvestedMoney_raw"])
                each_data["TDS_Price"] = self.datacleaner_obj.convert_from_symbol(each_data["TDS_Price"])
                each_data["OwnInvestedMoney"] = self.datacleaner_obj.convert_from_symbol(each_data["OwnInvestedMoney"])
                each_data["CompanyInvestedMoney"] = self.datacleaner_obj.convert_from_symbol(each_data["CompanyInvestedMoney"])
            
            each_data["TodaysValue"] = float(each_data["TodaysValue_raw"])
            each_data["Max_Value_FY"] = float(each_data["Max_Value_FY_raw"])
            each_data["FY_Closing_Value"] = float(each_data["FY_Closing_Value_raw"])
            each_data["TaxNeedtoPay"] = round((each_data['CapitalGain']) * taxSlab, 2)
            each_data["Buy_Year"] = float(each_data["Buy_Year"])
            each_data["Buy_Month"] = float(each_data["Buy_Month"])
            each_data["InvestmentType"] = type
            each_data["livePrice"] = float(self.livePrice)
            each_data["liveRupeePrice"] = float(self.todaysRP)

            each_data["Price_Bought"] = float(self.datacleaner_obj.convert_from_symbol(each_data["Price_Bought"]))
            each_data["InitialValue"] = float(self.datacleaner_obj.convert_from_symbol(each_data["InitialValue"]))
            
            try:
                del each_data['Buy_Date_formatted']
                del each_data['OwnInvestedMoneyNC']
                del each_data['PerShare_INR_raw']
                del each_data['TaxNeedtoPay_raw']
                del each_data['FY_Closing_Value_raw']
                del each_data['Max_Value_FY_raw']
                del each_data['TodaysValue_raw']
                del each_data['InitialValue_raw']
                del each_data['Price_Bought_raw']
                del each_data['TDS_Price_raw']
            except:
                pass
            
        return self.convert_to_kibana_document(dictionary, index_name)

    def generate_kibana_data_sold_stock(self, data, index_name):
        dictionary = data.to_dict('records')
        for each_data in dictionary:
            each_data["current_date"] = datetime.now(pytz.timezone('Asia/Kolkata'))
            each_data["Max_Value_FY"] = float(self.datacleaner_obj.convert_from_symbol(each_data["Max_Value_FY"]))
            each_data["FY_Closing_Value"] = float(self.datacleaner_obj.convert_from_symbol(each_data["FY_Closing_Value"]))
            each_data["ProfitNSU"] = float(self.datacleaner_obj.convert_from_symbol(each_data["ProfitNSU"]))
            if "-" in each_data["TaxNeedToBePaid"]:
                each_data["TaxNeedToBePaid"] = each_data["TaxNeedToBePaid"].replace("-", "")
                each_data["TaxNeedToBePaid"] = -1 * float(self.datacleaner_obj.convert_from_symbol(each_data["TaxNeedToBePaid"]))
            else:
                each_data["TaxNeedToBePaid"] = float(self.datacleaner_obj.convert_from_symbol(each_data["TaxNeedToBePaid"]))
                
            each_data['Price_Bought'] = float(self.datacleaner_obj.convert_from_symbol(each_data["Price_Bought"]))
            each_data['InitialValue'] = float(self.datacleaner_obj.convert_from_symbol(each_data["InitialValue"]))
            each_data['SellRupeeRate'] = float(self.datacleaner_obj.convert_from_symbol(each_data["SellRupeeRate"]))
            each_data['BuyRupeeRate'] = float(self.datacleaner_obj.convert_from_symbol(each_data["BuyRupeeRate"]))
            
            each_data["Buy_Year"] = float(each_data["Buy_Year"])
            each_data["Buy_Month"] = float(each_data["Buy_Month"])
            each_data["InvestmentType"] = "SellOut"
            each_data["livePrice"] = float(self.livePrice)
            each_data["liveRupeePrice"] = float(self.todaysRP)

            try:
                del each_data['Buy_Date_formatted']
                del each_data['Sell_Date_formatted']    
            except:
                pass

        return self.convert_to_kibana_document(dictionary, index_name)


class DB:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        db_name = 'nvShares.db'
        self.db_path = os.path.join(sys.path[0], 'configs', db_name)
    
    def create_tables(self, db_path):
        """Create NSU, ESPP, SellOut tables if they do not exist."""
        conn = sqlite3.connect(db_path)
        db_cursor = conn.cursor()
        nsu_create_table_query = "CREATE TABLE IF NOT EXISTS 'NSU' ( `Buy_Date` TEXT, `Available_Sell` REAL, `Price_Bought` REAL, `RupeeRate` REAL )"
        db_cursor.execute(nsu_create_table_query)
        espp_create_table_query = "CREATE TABLE IF NOT EXISTS 'ESPP' ( `Buy_Date` TEXT, `Available_Sell` REAL, `Price_Bought` REAL, `RupeeRate` REAL, `TDS_Price` REAL )"
        db_cursor.execute(espp_create_table_query)
        sellout_create_table_query = "CREATE TABLE IF NOT EXISTS 'SellOut' ( `Sell_Date` TEXT, `Buy_Date` TEXT, `Qty_Sold` INTEGER, `Price_Bought` REAL, `Price_Sell` REAL, `BuyRupeeRate` REAL, `SellRupeeRate` REAL, `Type` TEXT )"
        db_cursor.execute(sellout_create_table_query)
        conn.commit()
        conn.close()

    def ensure_tables(self):
        """Create tables if the database exists but NSU table is missing."""
        if not os.path.isfile(self.db_path):
            return
        conn = sqlite3.connect(self.db_path)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='NSU'")
        if cur.fetchone() is None:
            conn.close()
            self.create_tables(self.db_path)
        else:
            conn.close()

    def get_table_data(self, table_name):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * from " + table_name, conn)
        conn.close()
        
        return df
    
    def check_for_empty_db(self):
        tables_available = ['NSU', 'ESPP', 'SellOut']
        
        result = {}
        for each_table in tables_available:
            df = self.get_table_data(each_table)
            total_values = df.shape[0]
            
            result[each_table] = True
            if total_values < 1:
                result[each_table] = False
                print(each_table + ' data not found.')
        
        return result

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
        
        self.pre_start_time = datetime.time(datetime(2022, 3, 21, 16, 30))
        self.pre_stop_time = datetime.time(datetime(2022, 3, 21, 19, 0))
        self.post_start_time = datetime.time(datetime(2022, 3, 21, 19, 0))
        self.post_stop_time = datetime.time(datetime(2022, 3, 21, 1, 30))

    @retry(wait_random_min=10, stop_max_attempt_number=3)
    def get_rupee_rate(self, date):
        def fetch_rate_for_day(date_str):
            url = f"https://api.frankfurter.app/{date_str}"
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
            response = requests.get("https://open.er-api.com/v6/latest/USD", timeout=15)
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
        url = "https://api.frankfurter.dev/v1/latest"
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

    def get_stock_price(self, stock_code='NVDA', api_key="cvsfdk9r01qhup0qfks0cvsfdk9r01qhup0qfksg"):
        """
        Fetches the latest stock price for a given symbol using Finnhub.io.

        Parameters:
            stock_code (str): Stock ticker symbol (e.g., 'NVDA', 'AAPL')
            api_key (str): Finnhub.io API key

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

    def get_quote(self, stock_code='NVDA', api_key="cvsfdk9r01qhup0qfks0cvsfdk9r01qhup0qfksg"):
        """Fetches the Finnhub quote (c, o, h, l, pc, t) for the symbol. Returns dict or None."""
        url = "https://finnhub.io/api/v1/quote"
        params = {"symbol": stock_code.upper(), "token": api_key}
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

    def get_premarket_price(self, symbol="NVDA"):
        """Get current pre-market price (USD) via yfinance. Returns float or None. Use when in pre-market (4–9:30 AM ET)."""
        try:
            import yfinance as yf
        except ImportError:
            return None
        sym = symbol.upper()
        cache_key = f"pre_{sym}"
        now_utc = time.time()
        cached = _yf_extended_price_cache.get(cache_key)
        if cached is not None:
            ts, price = cached
            if (now_utc - ts) < _YF_CACHE_TTL_SEC and price is not None:
                return price
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
        """Get current post-market price (USD) via yfinance. Returns float or None. Use when in post-market (4–8 PM ET)."""
        try:
            import yfinance as yf
        except ImportError:
            return None
        sym = symbol.upper()
        cache_key = f"post_{sym}"
        now_utc = time.time()
        cached = _yf_extended_price_cache.get(cache_key)
        if cached is not None:
            ts, price = cached
            if (now_utc - ts) < _YF_CACHE_TTL_SEC and price is not None:
                return price
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
        """Returns (live_price_usd, todays_usd_inr_rate, open_price_usd or None, prev_close_usd or None)."""
        quote = self.get_quote(stock_code='nvda')
        live_price = None
        open_price = None
        prev_close = None
        if quote:
            c = quote.get("c")
            o = quote.get("o")
            pc = quote.get("pc")
            if c is not None:
                live_price = round(float(c), 2)
            if o is not None and float(o) > 0:
                open_price = round(float(o), 2)
            if pc is not None and float(pc) > 0:
                prev_close = round(float(pc), 2)
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


class Tax:
    def __init__(self) -> None:
        self.fix_tax_slab = 0.30  # default when buy_date unknown; < 2 yrs = 30%, >= 2 yrs = 12.5%

    def get_tax_slab(self, buy_date):
        """Tax on capital gain: >= 2 years since buy -> 12.5%; else -> 30%."""
        years_bought = round((date.today() - buy_date).days / 365.2425, 1)
        if years_bought >= 2.0:
            return 0.125
        return 0.30
    
    def generate_tax_doc(self):
        """_summary_
        """
        datacleaner_obj = DataCleaner()
        
        fy_date = datetime(date.today().year, 4, 1)
        shares_dict = []
        dfNSU, _, _, _, _, _, _, _ = OwnStockData().generate_display_data(type='NSU')
        dfESPP, _, _, _, _, _, _, _ = OwnStockData().generate_display_data(type='ESPP')
        dfs_arr = [dfNSU.iterrows(), dfESPP.iterrows()]
        for each_df in dfs_arr:
            for _, row in each_df:
                shares_dict_row = {
                    "CountryName": "2-United States Of America",
                    "CountryCodeExcludingIndia": "2",
                    "NameOfEntity": "NVIDIA",
                    "AddressOfEntity": "2788 San Tomas Expressway Santa Clara,CA",
                    "ZipCode": "95051",
                    "NatureOfEntity": "Shares",
                    "InterestAcquiringDate": datetime.strptime(row['Buy_Date_formatted'], "%d/%m/%Y").strftime("%Y-%m-%d"),
                    "InitialValOfInvstmnt": int(round(float(datacleaner_obj.convert_from_symbol(row['InitialValue'])), 0)),
                    "PeakBalanceDuringPeriod": int(round(float(datacleaner_obj.convert_from_symbol(row['Max_Value_FY'])), 0)),
                    "ClosingBalance": int(round(float(datacleaner_obj.convert_from_symbol(row['FY_Closing_Value'])), 0)),
                    "TotGrossAmtPaidCredited": 0,
                    "TotGrossProceeds": 0
                }
                invest_date = datetime.strptime(row['Buy_Date_formatted'], "%d/%m/%Y")
                if invest_date < fy_date:
                    shares_dict.append(shares_dict_row)

        with open(os.path.join(sys.path[0], 'output', "AY_" + str(date.today().year) + "_Shares.json"), "w") as outfile:
            outfile.write(json.dumps(shares_dict, indent=4))

class OwnStockData:
    def __init__(self) -> None:
        self.db_obj = DB()
        self.rupeeconv_obj = RupeeConv()
        self.tax_obj = Tax()
        
        self.livePrice, self.todaysRP, *_ = self.rupeeconv_obj.get_live_price()
        self.max_closing_json = {}

    @retry(wait_fixed=30000)
    def fetch_max_high_and_closing(self, symbol, start_date, end_date, api_key="db623c532e3e4568aad28629c00b574e"):
        json_file = "configs/historic_data.json"
        key = f"{str(start_date)}{str(end_date)}"
        current_year = datetime.now().year

        # Load JSON cache if exists
        if os.path.exists(json_file):
            with open(json_file, "r") as f:
                try:
                    self.max_closing_json = json.load(f)
                except json.JSONDecodeError:
                    self.max_closing_json = {}
        else:
            self.max_closing_json = {}

        # Check if data is from current year
        start_is_current = datetime.strptime(start_date, "%Y-%m-%d").year == current_year
        end_is_current = datetime.strptime(end_date, "%Y-%m-%d").year == current_year

        cached_entry = self.max_closing_json.get(key)

        should_update = False
        if start_is_current or end_is_current:
            if cached_entry:
                last_updated = datetime.strptime(cached_entry.get("last_updated", "1900-01-01"), "%Y-%m-%d")
                if datetime.now() - last_updated > timedelta(days=1):
                    should_update = True
            else:
                should_update = True
        else:
            # Not current year: don't update if already cached
            if cached_entry:
                return round(cached_entry["max_price"], 0), round(cached_entry["closing_price"], 0)
            else:
                should_update = True

        if not should_update and cached_entry:
            return round(cached_entry["max_price"], 0), round(cached_entry["closing_price"], 0)

        # Fetch from API
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": symbol,
            "interval": "1month",
            "start_date": start_date,
            "end_date": end_date,
            "apikey": api_key
        }

        response = requests.get(url, params=params)
        data = response.json()

        try:
            df = pd.DataFrame(data["values"])
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["high"] = pd.to_numeric(df["high"])
            df["close"] = pd.to_numeric(df["close"])

            max_high = df["high"].max()
            latest_close = df.loc[df["datetime"] == df["datetime"].max(), "close"].values[0]

            self.max_closing_json[key] = {
                "max_price": max_high,
                "closing_price": latest_close,
                "last_updated": datetime.now().strftime("%Y-%m-%d")
            }

            # Save to JSON
            with open(json_file, "w") as f:
                json.dump(self.max_closing_json, f, indent=4)

            return round(max_high, 0), round(latest_close, 0)

        except Exception as e:
            print("Data error for", start_date, "to", end_date, ":", data)
            return 0, 0

    def generate_display_data(self, type):
        """_summary_
        """
        df = self.db_obj.get_table_data(type)
        df.rename(columns = {'TDS_Price':'TDS_Price_raw', 'Price_Bought':'Price_Bought_raw'}, inplace = True)
        
        totalSellable = df['Available_Sell'].sum()
        
        if type == 'ESPP':
            Price_Bought = 'TDS_Price_raw'
        elif type == 'NSU':
            Price_Bought = 'Price_Bought_raw'
            
        currentValue = totalSellable * self.livePrice * self.todaysRP
        tds_paid_on = (df['Available_Sell'].mul(df[Price_Bought]) * df['RupeeRate'])
        df['Available_Sell'] = pd.to_numeric(df['Available_Sell'], errors='coerce').fillna(0).astype(int)
        df['PerShare_INR_raw'] = round(df[Price_Bought].mul(self.todaysRP), 0)
        df['InitialValue_raw'] = (df['Available_Sell'].mul(df[Price_Bought])).mul(df['RupeeRate'])
        df['TodaysValue_raw'] = (df['Available_Sell'].mul(self.livePrice) * self.todaysRP)
        df['CapitalGain'] = df['TodaysValue_raw'] - df['InitialValue_raw']
        # Tax per row: >= 2 years since buy -> 12.5%; else 30%
        buy_dates_parsed = pd.to_datetime(df['Buy_Date'], errors='coerce')
        buy_dates = buy_dates_parsed.dt.date
        df['TaxSlab'] = buy_dates.map(lambda d: self.tax_obj.get_tax_slab(d) if pd.notna(d) else self.tax_obj.fix_tax_slab)
        df['TaxNeedtoPay_raw'] = df['CapitalGain'] * df['TaxSlab']

        if type == 'ESPP':
            df['OwnInvestedMoney_raw'] = (df['Available_Sell'].mul(df['Price_Bought_raw']) * df['RupeeRate'])
            df['CompanyInvestedMoney_raw'] = df['InitialValue_raw'] - df['OwnInvestedMoney_raw']
            # Profit % = gain / cost base (same as NSU), so it matches "Total purchase" and tax; not (value - own paid)/own paid
            df['ProfitPercent'] = ((df['CapitalGain'] / df['InitialValue_raw'])) * 100

            df['OwnInvestedMoney'] = self.rupeeconv_obj.print_rupees(df['OwnInvestedMoney_raw'])
            df['CompanyInvestedMoney'] = self.rupeeconv_obj.print_rupees(df['CompanyInvestedMoney_raw'])
            df['TDS_Price'] = self.rupeeconv_obj.print_rupees(df['TDS_Price_raw'], cur='USD')
        elif type == 'NSU':
            df['ProfitPercent'] = ((df['CapitalGain'] / df['InitialValue_raw']))*100

        df['Buy_Date_formatted'] = buy_dates_parsed.dt.strftime("%d/%m/%Y")
        df['Buy_Year'] = buy_dates_parsed.dt.strftime("%Y")
        df['Buy_Month'] = buy_dates_parsed.dt.strftime("%m")

        Max_Price = []
        FY_Closing_Price = []
        
        for cnt in range(len(df['Buy_Year'])):
            try:
                yv, mv = df['Buy_Year'].iloc[cnt], df['Buy_Month'].iloc[cnt]
                year = int(float(yv)) if pd.notna(yv) and str(yv).replace('.', '').isdigit() else 2000
                month = int(float(mv)) if pd.notna(mv) and str(mv).replace('.', '').isdigit() else 1
            except (ValueError, TypeError):
                year, month = 2000, 1

            if month > 3:
                start = f"{year}-04-01"
                end = f"{year + 1}-03-31"
                closing_month = f"{year + 1}-03-01"
                closing_end = f"{year + 1}-03-31"
            else:
                start = f"{year - 1}-04-01"
                end = f"{year}-03-31"
                closing_month = f"{year}-03-01"
                closing_end = f"{year}-03-31"

            if f"{str(start)}{str(end)}" in self.max_closing_json.keys():
                max_price = self.max_closing_json[f"{str(start)}{str(end)}"]["max_price"]
            else:
                max_price, _ = self.fetch_max_high_and_closing("NVDA", start, end)

            if f"{str(closing_month)}{str(closing_end)}" in self.max_closing_json.keys():
                closing_price = self.max_closing_json[f"{str(closing_month)}{str(closing_end)}"]["closing_price"]
            else:
                _, closing_price = self.fetch_max_high_and_closing("NVDA", closing_month, closing_end)

            Max_Price.append(max_price)
            FY_Closing_Price.append(closing_price)

        df['Max_Price'] = Max_Price
        df['FY_Closing_Price'] = FY_Closing_Price
        df['Max_Value_FY_raw'] = (df['Available_Sell'].mul(df['RupeeRate'])).mul(df['Max_Price'])
        df['FY_Closing_Value_raw'] = (df['Available_Sell'].mul(df['RupeeRate'])).mul(df['FY_Closing_Price'])
        df['Buy_Date'] = buy_dates_parsed.dt.date

        df['PerShare_INR'] = self.rupeeconv_obj.print_rupees(df['PerShare_INR_raw'])
        df['TaxNeedtoPay'] = self.rupeeconv_obj.print_rupees(df['TaxNeedtoPay_raw'])
        df['FY_Closing_Value'] = self.rupeeconv_obj.print_rupees(df['FY_Closing_Value_raw'])
        df['Max_Value_FY'] = self.rupeeconv_obj.print_rupees(df['Max_Value_FY_raw'])
        df['TodaysValue'] = self.rupeeconv_obj.print_rupees(df['TodaysValue_raw'])
        df['InitialValue'] = self.rupeeconv_obj.print_rupees(df['InitialValue_raw'])
        df['Price_Bought'] = self.rupeeconv_obj.print_rupees(df['Price_Bought_raw'], cur='USD')

        df = df.sort_values(by=['Buy_Date'], ascending=True)
        # Keep full precision for dashboard (price columns and _raw columns); round(1) for display elsewhere
        cols_to_preserve = ['Price_Bought_raw', 'InitialValue_raw', 'TodaysValue_raw', 'TaxNeedtoPay_raw', 'ProfitPercent']
        if type == 'ESPP':
            cols_to_preserve = cols_to_preserve + ['TDS_Price_raw']
        raw_backup = df[cols_to_preserve].copy()
        df = df.round(1)
        df[cols_to_preserve] = raw_backup
        # Restore TaxSlab after round(1): 0.125 was rounded to 0.1; keep 12.5% / 30% correct
        df['TaxSlab'] = df['Buy_Date'].map(lambda d: self.tax_obj.get_tax_slab(d) if pd.notna(d) else self.tax_obj.fix_tax_slab)

        total_qty = int(float(df['Available_Sell'].sum()) or 0)
        total_todays_value = float(df['TodaysValue_raw'].sum())
        # Unrealised profit: gain = TodaysValue - InitialValue (in INR); after tax = gain - tax
        total_gain_before_tax = float(df['CapitalGain'].sum())
        total_tds = float(df['TaxNeedtoPay_raw'].sum())
        # Gain cannot exceed total current value (cost base is non-negative)
        if total_gain_before_tax > total_todays_value and total_gain_before_tax > 0:
            scale = total_todays_value / total_gain_before_tax
            total_tds = total_tds * scale
            total_gain_before_tax = total_todays_value
        if total_tds > total_gain_before_tax:
            total_tds = total_gain_before_tax
        total_capital_gain = total_gain_before_tax - total_tds  # profit after tax
        avg_buy_price = float(round(df['Price_Bought_raw'].mean(), 0))
        avg_profit_percent = float(round(df['ProfitPercent'].mean(), 0))

        return df, currentValue, tds_paid_on, total_qty, total_capital_gain, total_tds, avg_buy_price, avg_profit_percent

    def generate_sellout_display_data(self):
        dfSellOut = self.db_obj.get_table_data('SellOut')

        buy_dates_parsed_so = pd.to_datetime(dfSellOut['Buy_Date'], errors='coerce')
        dfSellOut['Buy_Date_formatted'] = buy_dates_parsed_so.dt.strftime("%d/%m/%Y")
        dfSellOut['Buy_Year'] = buy_dates_parsed_so.dt.strftime("%Y")
        dfSellOut['Buy_Month'] = buy_dates_parsed_so.dt.strftime("%m")

        Max_Price = []
        FY_Closing_Price = []

        for cnt in range(len(dfSellOut['Buy_Year'])):
            try:
                yv, mv = dfSellOut['Buy_Year'].iloc[cnt], dfSellOut['Buy_Month'].iloc[cnt]
                buy_year = int(float(yv)) if pd.notna(yv) and str(yv).replace('.', '').isdigit() else 2000
                buy_month = int(float(mv)) if pd.notna(mv) and str(mv).replace('.', '').isdigit() else 1
            except (ValueError, TypeError):
                buy_year, buy_month = 2000, 1
            
            if buy_month > 3:
                fy_start = f"{buy_year}-04-01"
                fy_end = f"{buy_year+1}-03-31"
                closing_start = f"{buy_year+1}-03-01"
                closing_end = f"{buy_year+1}-03-31"
            else:
                fy_start = f"{buy_year-1}-04-01"
                fy_end = f"{buy_year}-03-31"
                closing_start = f"{buy_year}-03-01"
                closing_end = f"{buy_year}-03-31"

            if f"{str(fy_start)}{str(fy_end)}" in self.max_closing_json.keys():
                max_price = self.max_closing_json[f"{str(fy_start)}{str(fy_end)}"]["max_price"]
            else:
                max_price, _ = self.fetch_max_high_and_closing("NVDA", fy_start, fy_end)

            if f"{str(closing_start)}{str(closing_end)}" in self.max_closing_json.keys():
                closing_price = self.max_closing_json[f"{str(closing_start)}{str(closing_end)}"]["closing_price"]
            else:
                _, closing_price = self.fetch_max_high_and_closing("NVDA", closing_start, closing_end)

            Max_Price.append(round(max_price, 0))
            FY_Closing_Price.append(round(closing_price, 0))

        dfSellOut['Max_Price'] = Max_Price
        dfSellOut['FY_Closing_Price'] = FY_Closing_Price
        dfSellOut['Max_Value_FY'] = ((dfSellOut['Qty_Sold'].mul(dfSellOut['BuyRupeeRate'])).mul(dfSellOut['Max_Price']))
        dfSellOut['FY_Closing_Value'] = ((dfSellOut['Qty_Sold'].mul(dfSellOut['BuyRupeeRate'])).mul(dfSellOut['FY_Closing_Price']))
        dfSellOut['InitialValue'] = (dfSellOut['Qty_Sold'].mul(dfSellOut['Price_Bought'])).mul(dfSellOut['BuyRupeeRate'])

        dfSellOut['Buy_Date'] = buy_dates_parsed_so.dt.date

        sell_dates_parsed = pd.to_datetime(dfSellOut['Sell_Date'], errors='coerce')
        dfSellOut['Sell_Date_formatted'] = sell_dates_parsed.dt.strftime("%d/%m/%Y")
        dfSellOut['Sell_Date'] = sell_dates_parsed.dt.date

        dfSellOut = dfSellOut.sort_values(by=['Sell_Date'], ascending=True)

        dfSellOut['Buy@R'] = ((dfSellOut['Qty_Sold'].mul(dfSellOut['Price_Bought'])) * dfSellOut['BuyRupeeRate'])
        dfSellOut['Sold@R'] = ((dfSellOut['Qty_Sold'].mul(dfSellOut['Price_Sell'])) * dfSellOut['SellRupeeRate'])
        # Tax per row: >= 2 years since buy -> 12.5%; else 30%
        dfSellOut['TaxSlab'] = dfSellOut['Buy_Date'].map(lambda d: self.tax_obj.get_tax_slab(d) if pd.notna(d) else self.tax_obj.fix_tax_slab)
        gain_before_tax = dfSellOut['Sold@R'] - dfSellOut['Buy@R']
        dfSellOut['TaxNeedToBePaid'] = (round((gain_before_tax * dfSellOut['TaxSlab']), 2))
        dfSellOut['ProfitPercent'] = round(((gain_before_tax - dfSellOut['TaxNeedToBePaid']) / dfSellOut['Buy@R']) * 100, 1)
        dfSellOut['ProfitNSU'] = round((dfSellOut[dfSellOut['Type'] == 'NSU']['Sold@R']), 2)
        dfSellOut['ProfitESPP'] = round((dfSellOut[dfSellOut['Type'] == 'ESPP']['Sold@R'] - dfSellOut[dfSellOut['Type'] == 'ESPP']['Buy@R']), 2)
        dfSellOut = dfSellOut.fillna(0)
        dfSellOut = dfSellOut.round(1)

        # Realised profit = (total sell value − total buy value) − total tax (same as your manual: sell − buy − tax)
        total_sell_inr = dfSellOut['Sold@R'].sum()
        total_buy_inr = dfSellOut['Buy@R'].sum()
        total_tax = dfSellOut['TaxNeedToBePaid'].sum()
        sell_profit = (total_sell_inr - total_buy_inr) - total_tax
        total_qty_sold = int(float(dfSellOut['Qty_Sold'].sum()) or 0)
        # Normalize Type (strip, upper) so "NSU", "RSU", "nsu" all count as RSU
        type_norm = dfSellOut['Type'].astype(str).str.strip().str.upper()
        total_sell_rsu_inr = dfSellOut.loc[type_norm.isin(['NSU', 'RSU']), 'Sold@R'].sum()
        total_sell_espp_inr = dfSellOut.loc[type_norm == 'ESPP', 'Sold@R'].sum()

        dfSellOut['Price_Bought'] = self.rupeeconv_obj.print_rupees(dfSellOut['Price_Bought'], cur='USD')
        dfSellOut['BuyRupeeRate'] = self.rupeeconv_obj.print_rupees(dfSellOut['BuyRupeeRate'])
        # dfSellOut['Price_Sell'] = self.rupeeconv_obj.print_rupees(dfSellOut['BuyRupeeRate'], cur='USD')
        dfSellOut['SellRupeeRate'] = self.rupeeconv_obj.print_rupees(dfSellOut['SellRupeeRate'])
        dfSellOut['ProfitESPP'] = self.rupeeconv_obj.print_rupees(dfSellOut['ProfitESPP'])
        dfSellOut['ProfitNSU'] = self.rupeeconv_obj.print_rupees(dfSellOut['ProfitNSU'])
        dfSellOut['TaxNeedToBePaid'] = self.rupeeconv_obj.print_rupees(dfSellOut['TaxNeedToBePaid'])
        dfSellOut['InitialValue'] = self.rupeeconv_obj.print_rupees(dfSellOut['InitialValue'])
        dfSellOut['Max_Value_FY'] = self.rupeeconv_obj.print_rupees(dfSellOut['Max_Value_FY'])
        dfSellOut['FY_Closing_Value'] = self.rupeeconv_obj.print_rupees(dfSellOut['FY_Closing_Value'])
        
        return dfSellOut, sell_profit, total_qty_sold, total_sell_inr, total_sell_rsu_inr, total_sell_espp_inr
        
class DataCleaner:
    def convert_from_symbol(self, text):
        symbols = ["$", "₹", "?", ",", " "]
        
        for each_symbol in symbols:
            text = text.replace(each_symbol, '')      
        
        return text