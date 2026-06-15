"""Build display/holdings/sold dataframes from the DB plus market data."""
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from retrying import retry

from helpers import config
from helpers.db import DB
from helpers.market import RupeeConv
from helpers.tax import Tax


class OwnStockData:
    def __init__(self) -> None:
        self.db_obj = DB()
        self.rupeeconv_obj = RupeeConv()
        self.tax_obj = Tax()
        
        self.livePrice, self.todaysRP, *_ = self.rupeeconv_obj.get_live_price()
        self.max_closing_json = {}

    @retry(wait_fixed=30000)
    def fetch_max_high_and_closing(self, symbol, start_date, end_date, api_key=None):
        api_key = api_key or config.FMV_API_KEY
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

