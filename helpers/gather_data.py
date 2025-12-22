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

        self.livePrice, self.todaysRP = rupee_conv_obj.get_live_price()

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
        """_summary_
        """
        conn = sqlite3.connect(db_path)
        db_cursor = conn.cursor()

        nsu_create_table_query = "CREATE TABLE 'NSU' ( `Buy_Date` TEXT, `Available_Sell` REAL, `Price_Bought` REAL, `RupeeRate` REAL )"
        db_cursor.execute(nsu_create_table_query)

        espp_create_table_query = "CREATE TABLE 'ESPP' ( `Buy_Date` TEXT, `Available_Sell` REAL, `Price_Bought` REAL, `RupeeRate` REAL )"
        db_cursor.execute(espp_create_table_query)

        sellout_create_table_query = "CREATE TABLE 'SellOut' ( `Sell_Date` TEXT, `Buy_Date` TEXT, `Qty_Sold` INTEGER, `Price_Bought` REAL, `Price_Sell` REAL, `BuyRupeeRate` REAL, `SellRupeeRate` REAL, `Type` TEXT )"
        db_cursor.execute(sellout_create_table_query)
        
        conn.commit()

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

        # Handle single date
        if isinstance(date, datetime):
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

    def get_stock_price(self, stock_code='NVDA', api_key="cvsfdk9r01qhup0qfks0cvsfdk9r01qhup0qfksg"):
        """
        Fetches the latest stock price for a given symbol using Finnhub.io.

        Parameters:
            stock_code (str): Stock ticker symbol (e.g., 'NVDA', 'AAPL')
            api_key (str): Finnhub.io API key

        Returns:
            float: Latest stock price, or None if not available
        """
        url = f"https://finnhub.io/api/v1/quote"
        params = {
            "symbol": stock_code.upper(),
            "token": api_key
        }

        try:
            response = requests.get(url, params=params)
            data = response.json()
            price = data.get("c")  # 'c' is current price
            if price is not None:
                return round(float(price), 2)
            else:
                print("Price not found in response:", data)
                return None
        except Exception as e:
            print(f"Error retrieving stock price for {stock_code}:", e)
            return None

    @retry(wait_random_min=10, stop_max_attempt_number=3)
    def get_live_price(self):
        live_price = self.get_stock_price(stock_code='nvda')
        todays_rp = self.get_rupee_rate(self.todays_date)

        return live_price, todays_rp

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
    def  __init__(self) -> None:
        self.fix_tax_slab = 0.3

    def get_tax_slab(self, buy_date):
        """_summary_

        Args:
            buy_date (_type_): _description_

        Returns:
            _type_: _description_
        """
        years_bought = round((date.today() - buy_date).days / 365.2425, 1)

        if years_bought >= 2.0:
            tax_rate = 0.125
        else:
            tax_rate = 0.34

        return tax_rate
    
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
        
        self.livePrice, self.todaysRP = self.rupeeconv_obj.get_live_price()
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
        df['Available_Sell'] = df['Available_Sell'].astype(int)
        df['PerShare_INR_raw'] = round(df[Price_Bought].mul(self.todaysRP), 0)
        df['InitialValue_raw'] = (df['Available_Sell'].mul(df[Price_Bought])).mul(df['RupeeRate'])
        df['TodaysValue_raw'] = (df['Available_Sell'].mul(self.livePrice) * self.todaysRP)
        df['CapitalGain'] = df['TodaysValue_raw'] - df['InitialValue_raw']
        df['TaxNeedtoPay_raw'] = df['CapitalGain'] * self.tax_obj.fix_tax_slab

        if type == 'ESPP':
            df['OwnInvestedMoney_raw'] = (df['Available_Sell'].mul(df['Price_Bought_raw']) * df['RupeeRate'])
            df['CompanyInvestedMoney_raw'] = df['InitialValue_raw'] - df['OwnInvestedMoney_raw']
            df['ProfitPercent'] = (((df['TodaysValue_raw']-df['OwnInvestedMoney_raw']) / df['OwnInvestedMoney_raw']))*100                
            
            df['OwnInvestedMoney'] = self.rupeeconv_obj.print_rupees(df['OwnInvestedMoney_raw'])
            df['CompanyInvestedMoney'] = self.rupeeconv_obj.print_rupees(df['CompanyInvestedMoney_raw'])
            df['TDS_Price'] = self.rupeeconv_obj.print_rupees(df['TDS_Price_raw'], cur='USD')
        elif type == 'NSU':
            df['ProfitPercent'] = ((df['CapitalGain'] / df['InitialValue_raw']))*100

        df['Buy_Date_formatted'] = pd.to_datetime(df['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%d/%m/%Y")
        df['Buy_Year'] = pd.to_datetime(df['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%Y")
        df['Buy_Month'] = pd.to_datetime(df['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%m")

        Max_Price = []
        FY_Closing_Price = []
        
        for cnt in range(len(df['Buy_Year'])):
            year = int(df['Buy_Year'][cnt])
            month = int(df['Buy_Month'][cnt])

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
        df['Buy_Date'] = pd.to_datetime(df['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.date

        df['PerShare_INR'] = self.rupeeconv_obj.print_rupees(df['PerShare_INR_raw'])
        df['TaxNeedtoPay'] = self.rupeeconv_obj.print_rupees(df['TaxNeedtoPay_raw'])
        df['FY_Closing_Value'] = self.rupeeconv_obj.print_rupees(df['FY_Closing_Value_raw'])
        df['Max_Value_FY'] = self.rupeeconv_obj.print_rupees(df['Max_Value_FY_raw'])
        df['TodaysValue'] = self.rupeeconv_obj.print_rupees(df['TodaysValue_raw'])
        df['InitialValue'] = self.rupeeconv_obj.print_rupees(df['InitialValue_raw'])
        df['Price_Bought'] = self.rupeeconv_obj.print_rupees(df['Price_Bought_raw'], cur='USD')

        df = df.sort_values(by=['Buy_Date'], ascending=True)
        df = df.round(1)
        
        total_qty = int(df['Available_Sell'].sum())
        total_capital_gain = float(df['TodaysValue_raw'].sum())
        total_tds = float(df['TaxNeedtoPay_raw'].sum())
        avg_buy_price = float(round(df['Price_Bought_raw'].mean(), 0))
        avg_profit_percent = float(round(df['ProfitPercent'].mean(), 0))

        return df, currentValue, tds_paid_on, total_qty, total_capital_gain, total_tds, avg_buy_price, avg_profit_percent

    def generate_sellout_display_data(self):
        dfSellOut = self.db_obj.get_table_data('SellOut')

        dfSellOut['Buy_Date_formatted'] = pd.to_datetime(dfSellOut['Buy_Date']).dt.strftime("%d/%m/%Y")
        dfSellOut['Buy_Year'] = pd.to_datetime(dfSellOut['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%Y")
        dfSellOut['Buy_Month'] = pd.to_datetime(dfSellOut['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%m")

        Max_Price = []
        FY_Closing_Price = []

        for cnt in range(len(dfSellOut['Buy_Year'])):
            buy_year = int(dfSellOut['Buy_Year'][cnt])
            buy_month = int(dfSellOut['Buy_Month'][cnt])
            
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

        dfSellOut['Buy_Date'] = pd.to_datetime(dfSellOut['Buy_Date']).dt.date
        
        dfSellOut['Sell_Date_formatted'] = pd.to_datetime(dfSellOut['Sell_Date']).dt.strftime("%d/%m/%Y")
        dfSellOut['Sell_Date'] = pd.to_datetime(dfSellOut['Sell_Date']).dt.date
        
        dfSellOut = dfSellOut.sort_values(by=['Sell_Date'], ascending=True)

        dfSellOut['Buy@R'] = ((dfSellOut['Qty_Sold'].mul(dfSellOut['Price_Bought'])) * dfSellOut['BuyRupeeRate'])
        dfSellOut['Sold@R'] = ((dfSellOut['Qty_Sold'].mul(dfSellOut['Price_Sell'])) * dfSellOut['SellRupeeRate'])
        dfSellOut['ProfitPercent'] = round(((dfSellOut['Sold@R'] - dfSellOut['Buy@R'] - ((dfSellOut['Sold@R'] - dfSellOut['Buy@R']) * self.tax_obj.fix_tax_slab)) / dfSellOut['Buy@R']) * 100, 2)
        dfSellOut['ProfitNSU'] = round((dfSellOut[dfSellOut['Type'] == 'NSU']['Sold@R']), 2)
        dfSellOut['ProfitESPP'] = round((dfSellOut[dfSellOut['Type'] == 'ESPP']['Sold@R'] - dfSellOut[dfSellOut['Type'] == 'ESPP']['Buy@R']), 2)
        dfSellOut['TaxNeedToBePaid'] = (round(((dfSellOut['Sold@R'] - dfSellOut['Buy@R']) * self.tax_obj.fix_tax_slab), 2))

        dfSellOut = dfSellOut.fillna(0)
        dfSellOut = dfSellOut.round(1)
        
        sell_profit = ((dfSellOut[dfSellOut['Type'] == 'NSU']['Sold@R']).sum() + (dfSellOut[dfSellOut['Type'] == 'ESPP']['Sold@R'] - dfSellOut[dfSellOut['Type'] == 'ESPP']['Buy@R']).sum() - ((dfSellOut['Sold@R'] - dfSellOut['Buy@R']) * self.tax_obj.fix_tax_slab).sum())
        
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
        
        return dfSellOut, sell_profit
        
class DataCleaner:
    def convert_from_symbol(self, text):
        symbols = ["$", "₹", "?", ",", " "]
        
        for each_symbol in symbols:
            text = text.replace(each_symbol, '')      
        
        return text