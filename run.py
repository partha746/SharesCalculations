# python -m pip install pyopenssl yfinance lxml yahoo_fin requests_html setuptools pandas django tabulate
# sudo apt install sqlitebrowser

import argparse
import datetime as dt
import json
import locale
import os
import sqlite3
import sys
import uuid
import warnings
from datetime import date, datetime, timedelta

import pandas as pd
import pytz
import requests
from django.utils.encoding import smart_str
from elasticsearch import Elasticsearch, helpers
from retrying import retry
from tabulate import tabulate
from yahoo_fin import stock_info as si
from yahoo_fin.stock_info import get_data as gd
from yahoofinancials import YahooFinancials

parser = argparse.ArgumentParser(description='Script to track NV investment')
parser.add_argument("-elk", help="Send Data to ELK", action="store_true")
parser.add_argument("-itr", help="Generate ITR json", action="store_true")
args = parser.parse_args()

warnings.filterwarnings("ignore", category=UserWarning)


taxSlab = 0.3
todaysDate = datetime.now()
dbName = os.path.join(sys.path[0], 'nvShares.db')
# dbName = os.path.join(sys.path[0], 'nvShares_split_20-07-2021.db')
conn = sqlite3.connect(dbName)


def login_eks():

    creds = {"id": "O3jMaoMBDsq7mYZZEuYO", "name": "Access", "api_key": "uFhFnhGTQYaImOOVrhiU_Q",
             "encoded": "TzNqTWFvTUJEc3E3bVlaWkV1WU86dUZoRm5oR1RRWWFJbU9PVnJoaVVfUQ=="}

    elastic_obj = Elasticsearch(
        'http://' + '192.168.1.21' + ':9200', api_key=(creds['id'], creds['api_key']))

    return elastic_obj


def genarate_data(instance_data, index_name):
    # print(json.dumps(instance_data, indent=3))
    data = [
        {
            "_index": index_name,
            "_id": uuid.uuid1(),
            "_source": each
        }
        for each in instance_data
    ]
    return (data)


@retry(wait_random_min=10, stop_max_attempt_number=3)
def convRate(date):
    yahoo_financials = YahooFinancials('USDINR=X')
    if isinstance(date, pd.Series):
        rateList = []
        for dat in date:
            date = datetime.strptime(dat, '%m/%d/%Y').strftime("%Y-%m-%d")
            prev_day = datetime.today() - timedelta(days=1)
            rateList.append(yahoo_financials.get_historical_price_data(
                prev_day, date, "daily")["USDINR=X"]["prices"][0]["close"])
        return rateList
    if not isinstance(date, dt.date):
        if '/' in date:
            date = datetime.strptime(date, '%m/%d/%Y').strftime("%Y-%m-%d")
            prev_day = (datetime.strptime(date, "%Y-%m-%d") -
                        timedelta(days=1)).strftime("%Y-%m-%d")
            rate = yahoo_financials.get_historical_price_data(prev_day, date, "daily")[
                "USDINR=X"]["prices"][0]["close"]
            return round(rate, 2)
    else:
        date = datetime.strptime(
            str(date), '%Y-%m-%d %H:%M:%S.%f').strftime("%Y-%m-%d")
        prev_day = (datetime.strptime(date, '%Y-%m-%d') -
                    timedelta(days=1)).strftime("%Y-%m-%d")
        rate = yahoo_financials.get_historical_price_data(prev_day, date, "daily")[
            "USDINR=X"]["prices"][0]["close"]
        return round(rate, 2)


def rupeesPrint(amt):
    # locale.setlocale(locale.LC_MONETARY, 'en_IN')
    locale.setlocale(locale.LC_MONETARY, 'en_IN.UTF-8')
    if isinstance(amt, pd.Series):
        rupees = []
        for amt in amt:
            rupees.append(locale.currency(amt, grouping=True))
        return rupees
    elif isinstance(amt, list):
        rupees = []
        for amt in amt:
            rupees.append(locale.currency(amt, grouping=True))
        return rupees

    return locale.currency(amt, grouping=True)


def rupeesRateNull(tableName, dateColumnName, RPcolumnName):
    c = conn.cursor()
    nullDF = 'SELECT ' + dateColumnName + ' FROM ' + \
        tableName + ' where ' + RPcolumnName + ' IS NULL;'
    # RSRatenullDF = pd.read_sql_query(nullDF, conn)
    query = c.execute(nullDF)
    cols = [column[0] for column in query.description]
    RSRatenullDF = pd.DataFrame.from_records(
        data=query.fetchall(), columns=cols)

    if len(RSRatenullDF) > 0:
        RSRatenullDF[RPcolumnName] = convRate(RSRatenullDF[dateColumnName][0])

        for index, row in RSRatenullDF.iterrows():
            cmd = 'UPDATE ' + tableName + ' SET ' + RPcolumnName + ' = \'' + \
                str(row[RPcolumnName]) + '\' where ' + dateColumnName + \
                ' = \'' + str(row[dateColumnName]) + '\''
            c.execute(cmd)

        conn.commit()


def get_tax_slab(buy_date):

    years_bought = round((date.today() - buy_date).days / 365.2425, 1)

    if years_bought >= 2.0:
        tax_rate = 0.2
    else:
        tax_rate = 0.34

    return tax_rate


###################Update database with rupee Rate###################
# CREATE TABLE "NSU" ( `Buy_Date` TEXT, `Available_Sell` REAL, `Price_Bought` REAL, `RupeeRate` REAL )
rupeesRateNull('NSU', 'Buy_Date', 'RupeeRate')
# CREATE TABLE "ESPP" ( `Buy_Date` TEXT, `Available_Sell` REAL, `Price_Bought` REAL, `RupeeRate` REAL )
rupeesRateNull('ESPP', 'Buy_Date', 'RupeeRate')

# CREATE TABLE "SellOut" ( `Sell_Date` TEXT, `Buy_Date` TEXT, `Qty_Sold` INTEGER, `Price_Bought` REAL, `Price_Sell` REAL, `BuyRupeeRate` REAL, `SellRupeeRate` REAL, `Type` TEXT )
rupeesRateNull('SellOut', 'Buy_Date', 'BuyRupeeRate')
rupeesRateNull('SellOut', 'Sell_Date', 'SellRupeeRate')


@retry(wait_random_min=10, stop_max_attempt_number=3)
def get_live_price():

    def get_price():
        pre_start_time = datetime.time(datetime(2022, 3, 21, 16, 30))
        pre_stop_time = datetime.time(datetime(2022, 3, 21, 19, 0))
        post_start_time = datetime.time(datetime(2022, 3, 21, 19, 0))
        post_stop_time = datetime.time(datetime(2022, 3, 21, 1, 30))

        if post_start_time <= datetime.now().time() <= post_stop_time:
            return round(si.get_postmarket_price("nvda"), 2)
        elif pre_start_time <= datetime.now().time() <= pre_stop_time:
            return round(si.get_premarket_price("nvda"), 2)
        else:
            return round(si.get_live_price("nvda"), 2)

    livePrice = get_price()
    todaysRP = convRate(todaysDate)
    # livePrice = 312.5
    # todaysRP = 75.59

    return livePrice, todaysRP


livePrice, todaysRP = get_live_price()

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#
arrowRT = u'\u2192'
print('--------------------------')
print('NVDA\t\t: $ ' + str(livePrice))
print('USD ' + smart_str(arrowRT) + ' INR\t: ' + rupeesPrint(todaysRP))
print('--------------------------')

###################ESPP Calculations###################
dfESPP = pd.read_sql_query("SELECT * from ESPP", conn)

totalSellableESPP = dfESPP['Available_Sell'].sum()
OwnInvestedMoneyESPP = (dfESPP['Available_Sell'].mul(
    dfESPP['Price_Bought']) * dfESPP['RupeeRate'])
taxableInvestedMoneyESPP = (dfESPP['Available_Sell'].mul(
    dfESPP['TDS_Price']) * dfESPP['RupeeRate'])
currentESPPValue = totalSellableESPP * livePrice * todaysRP
taxableAmountESPP = (currentESPPValue - OwnInvestedMoneyESPP)

profitPercentESPP = round(
    ((taxableAmountESPP[0] / OwnInvestedMoneyESPP[0]) * 100), 1)

amtPaidTaxForESPP_SP_DF = (dfESPP['Available_Sell'].mul(livePrice) * todaysRP)
dfESPP['PerShare_INR'] = rupeesPrint(
    round(dfESPP['TDS_Price'].mul(todaysRP), 0))
dfESPP['ProfitPercent'] = (round(((amtPaidTaxForESPP_SP_DF - OwnInvestedMoneyESPP - (
    (amtPaidTaxForESPP_SP_DF - OwnInvestedMoneyESPP) * taxSlab)) / OwnInvestedMoneyESPP) * 100, 0))
dfESPP['OwnInvestedMoneyESPP'] = rupeesPrint(OwnInvestedMoneyESPP)
dfESPP['OwnInvestedMoneyESPPNC'] = OwnInvestedMoneyESPP
dfESPP['TodaysValue'] = rupeesPrint(
    dfESPP['Available_Sell'].mul(livePrice) * todaysRP)
dfESPP['TaxNeedtoPay'] = rupeesPrint(((dfESPP['Available_Sell'].mul(
    livePrice) * todaysRP) - taxableInvestedMoneyESPP) * taxSlab)
dfESPP['InitialValue'] = (dfESPP['Available_Sell'].mul(
    dfESPP['TDS_Price'])).mul(dfESPP['RupeeRate'])

dfESPP['CompanyInvestedMoney'] = dfESPP['InitialValue'] - \
    dfESPP['OwnInvestedMoneyESPPNC']

if profitPercentESPP > 0:
    arrowSign = u'\u2191'
else:
    arrowSign = u'\u2193'

print('\t\t\t\t\tESPP Distribution')
# print('\t\t\t\t\t-----------------')
dfESPP['Buy_Date_formatted'] = pd.to_datetime(
    dfESPP['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%d/%m/%Y")
dfESPP['Buy_Year'] = pd.to_datetime(
    dfESPP['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%Y")
dfESPP['Buy_Month'] = pd.to_datetime(
    dfESPP['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%m")

Max_Price = []
FY_Closing_Price = []
for cnt in range(0, len(dfESPP['Buy_Year'])):
    if int(dfESPP['Buy_Month'][cnt]) > 3:
        Max_Price.append(round(gd("nvda", start_date="01/04/" + dfESPP['Buy_Year'][cnt], end_date="31/03/" + str(
            int(dfESPP['Buy_Year'][cnt]) + 1), index_as_date=True, interval="1mo")['high'].max(), 0))
        try:
            FY_Closing_Price.append(round(gd("nvda", start_date="01/03/" + str(int(dfESPP['Buy_Year'][cnt]) + 1), end_date="31/03/" + str(
                int(dfESPP['Buy_Year'][cnt]) + 1), index_as_date=True, interval="1mo")['close'].max(), 0))
        except:
            FY_Closing_Price.append(0)
    else:
        Max_Price.append(round(gd("nvda", start_date="01/04/" + str(int(
            dfESPP['Buy_Year'][cnt]) - 1), end_date="31/03/" + dfESPP['Buy_Year'][cnt], index_as_date=True, interval="1mo")['high'].max(), 0))
        FY_Closing_Price.append(round(gd("nvda", start_date="01/03/" + str(int(dfESPP['Buy_Year'][cnt]) - 1), end_date="31/03/" + str(
            int(dfESPP['Buy_Year'][cnt]) - 1), index_as_date=True, interval="1mo")['close'].max(), 0))

dfESPP['Max_Price'] = Max_Price
dfESPP['FY_Closing_Price'] = FY_Closing_Price
dfESPP['Max_Value_FY'] = rupeesPrint(
    (dfESPP['Available_Sell'].mul(dfESPP['RupeeRate'])).mul(dfESPP['Max_Price']))
dfESPP['FY_Closing_Value'] = rupeesPrint((dfESPP['Available_Sell'].mul(
    dfESPP['RupeeRate'])).mul(dfESPP['FY_Closing_Price']))

dfESPP['Buy_Date'] = pd.to_datetime(
    dfESPP['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.date
dfESPP = dfESPP.sort_values(by=['Buy_Date'], ascending=True)

dfESPP = dfESPP.round(1)
print(tabulate(dfESPP[['Buy_Date_formatted', 'Available_Sell', 'Price_Bought', 'RupeeRate', 'TDS_Price', 'PerShare_INR', 'ProfitPercent', 'OwnInvestedMoneyESPP',
      'TodaysValue', 'TaxNeedtoPay', 'InitialValue', 'Max_Value_FY', 'FY_Closing_Value', 'CompanyInvestedMoney']], headers='keys', tablefmt='pretty', colalign=("centre",)))
# print('-----------------------------------------')
print('ESPP Available to sell \t: ' + str(int(dfESPP['Available_Sell'].sum())))
print('ESPP Profit after TAX \t: ' + rupeesPrint(currentESPPValue -
      ((((dfESPP['Available_Sell'].mul(livePrice) * todaysRP) - taxableInvestedMoneyESPP) * taxSlab).sum())))
print('Total Tax to be Paid \t: ' + rupeesPrint((((dfESPP['Available_Sell'].mul(
    livePrice) * todaysRP) - taxableInvestedMoneyESPP) * taxSlab).sum()))
print('ESPP Average Buy \t: $ ' + str(round(dfESPP['Price_Bought'].mean(), 0)))
print('ESPP Average Profit \t: ' +
      str(round(dfESPP['ProfitPercent'].mean(), 0)) + '%')
print('________________________________________')
print('\n')

if args.elk:
    login_eks().indices.delete(index='psardar-shares', ignore_unavailable=True)

ESPP_dict = dfESPP.to_dict('records')
for each_data in ESPP_dict:
    taxSlab = get_tax_slab(each_data['Buy_Date'])
    taxableInvestedMoneyESPP = (
        each_data['Available_Sell'] * each_data['TDS_Price']) * each_data['RupeeRate']
    each_data["TaxNeedtoPay"] = round(
        (((each_data['Available_Sell'] * livePrice) * todaysRP) - taxableInvestedMoneyESPP) * taxSlab, 2)

    each_data["current_date"] = datetime.now(pytz.timezone('Asia/Kolkata'))
    each_data["PerShare_INR"] = float(each_data["PerShare_INR"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    each_data["OwnInvestedMoneyESPP"] = float(each_data["OwnInvestedMoneyESPP"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    each_data["TodaysValue"] = float(each_data["TodaysValue"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    # each_data["TaxNeedtoPay"] = float(each_data["TaxNeedtoPay"].replace("₹", "").replace("?", "").replace(",","").replace(" ",""))
    each_data["TaxNeedtoPay"] = (
        ((each_data['Available_Sell'] * livePrice) * todaysRP) - taxableInvestedMoneyESPP) * taxSlab
    each_data["Max_Value_FY"] = float(each_data["Max_Value_FY"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    each_data["FY_Closing_Value"] = float(each_data["FY_Closing_Value"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    each_data["Buy_Year"] = float(each_data["Buy_Year"])
    each_data["Buy_Month"] = float(each_data["Buy_Month"])
    each_data["InvestmentType"] = "ESPP"
    each_data["livePrice"] = float(livePrice)
    each_data["liveRupeePrice"] = float(todaysRP)

    del each_data['Buy_Date_formatted']
    del each_data['OwnInvestedMoneyESPPNC']

if args.elk:
    helpers.bulk(login_eks(), genarate_data(ESPP_dict, 'psardar-shares'))
    helpers.bulk(login_eks(), genarate_data(ESPP_dict, 'psardar-shares-daily'))

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#

###################NSU Calculations###################
dfNSU = pd.read_sql_query("SELECT * from NSU", conn)

totalSellableNSU = dfNSU['Available_Sell'].sum()
amtPaidTaxForNSUDF = ((dfNSU['Available_Sell'].mul(
    dfNSU['Price_Bought'])) * dfNSU['RupeeRate'])
amtPaidTaxForNSU = amtPaidTaxForNSUDF.sum()
currentNSUValue = totalSellableNSU * livePrice * todaysRP
taxableAmountNSU = abs(currentNSUValue - amtPaidTaxForNSU)

amtPaidTaxForNSU_SP_DF = ((dfNSU['Available_Sell'].mul(livePrice)) * todaysRP)
dfNSU['PerShare_INR'] = rupeesPrint(
    round(dfNSU['Price_Bought'].mul(todaysRP), 2))
dfNSU['ProfitPercent'] = (round(((amtPaidTaxForNSU_SP_DF - amtPaidTaxForNSUDF - (
    (amtPaidTaxForNSU_SP_DF - amtPaidTaxForNSUDF) * taxSlab)) / amtPaidTaxForNSUDF) * 100, 2))
dfNSU['TodaysValue'] = rupeesPrint(
    dfNSU['Available_Sell'].mul(livePrice) * todaysRP)
dfNSU['TaxNeedtoPay'] = rupeesPrint(((dfNSU['Available_Sell'].mul(
    livePrice) * todaysRP) - amtPaidTaxForNSUDF) * taxSlab)
dfNSU['InitialValue'] = (dfNSU['Available_Sell'].mul(
    dfNSU['Price_Bought'])).mul(dfNSU['RupeeRate'])

print('\t\t\t\t\tNSU Distribution')
# print('\t\t\t\t\t----------------')
dfNSU['Buy_Date_formatted'] = pd.to_datetime(
    dfNSU['Buy_Date']).dt.strftime("%d/%m/%Y")
dfNSU['Buy_Year'] = pd.to_datetime(
    dfNSU['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%Y")
dfNSU['Buy_Month'] = pd.to_datetime(
    dfNSU['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%m")

Max_Price = []
FY_Closing_Price = []
for cnt in range(0, len(dfNSU['Buy_Year'])):
    if int(dfNSU['Buy_Month'][cnt]) > 3:
        Max_Price.append(round(gd("nvda", start_date="01/04/" + dfNSU['Buy_Year'][cnt], end_date="31/03/" + str(
            int(dfNSU['Buy_Year'][cnt]) + 1), index_as_date=True, interval="1mo")['high'].max(), 0))
        try:
            FY_Closing_Price.append(round(gd("nvda", start_date="01/03/" + str(int(dfNSU['Buy_Year'][cnt]) + 1), end_date="31/03/" + str(
                int(dfNSU['Buy_Year'][cnt]) + 1), index_as_date=True, interval="1mo")['close'].max(), 0))
        except:
            FY_Closing_Price.append(0)
    else:
        Max_Price.append(round(gd("nvda", start_date="01/04/" + str(int(
            dfNSU['Buy_Year'][cnt]) - 1), end_date="31/03/" + dfNSU['Buy_Year'][cnt], index_as_date=True, interval="1mo")['high'].max(), 0))
        FY_Closing_Price.append(round(gd("nvda", start_date="01/03/" + str(int(dfNSU['Buy_Year'][cnt]) - 1), end_date="31/03/" + str(
            int(dfNSU['Buy_Year'][cnt]) - 1), index_as_date=True, interval="1mo")['close'].max(), 0))

dfNSU['Max_Price'] = Max_Price
dfNSU['FY_Closing_Price'] = FY_Closing_Price
dfNSU['Max_Value_FY'] = rupeesPrint(
    (dfNSU['Available_Sell'].mul(dfNSU['RupeeRate'])).mul(dfNSU['Max_Price']))
dfNSU['FY_Closing_Value'] = rupeesPrint(
    (dfNSU['Available_Sell'].mul(dfNSU['RupeeRate'])).mul(dfNSU['FY_Closing_Price']))

dfNSU['Buy_Date'] = pd.to_datetime(dfNSU['Buy_Date']).dt.date
dfNSU = dfNSU.sort_values(by=['Buy_Date'], ascending=True)

dfNSU = dfNSU.round(1)

print(tabulate(dfNSU[['Buy_Date_formatted', 'Available_Sell', 'Price_Bought', 'RupeeRate', 'PerShare_INR', 'ProfitPercent', 'TodaysValue',
      'TaxNeedtoPay', 'InitialValue', 'Max_Value_FY', 'FY_Closing_Value']], headers='keys', tablefmt='pretty', colalign=("centre",)))
# print('-----------------------------------------')
print('NSU Available to sell \t: ' + str(int(dfNSU['Available_Sell'].sum())))
print('NSU Profit after TAX \t: ' +
      rupeesPrint(currentNSUValue - (taxableAmountNSU * taxSlab)))
print('Total Tax to be Paid \t: ' + rupeesPrint((((dfNSU['Available_Sell'].mul(
    livePrice) * todaysRP) - amtPaidTaxForNSUDF) * taxSlab).sum()))
print('NSU Average Buy \t: $ ' + str(round(dfNSU['Price_Bought'].mean(), 2)))
print('NSU Average Profit \t: ' +
      str(round(dfNSU['ProfitPercent'].mean(), 2)) + '%')
print('________________________________________')

NSU_dict = dfNSU.to_dict('records')
for each_data in NSU_dict:
    each_data["current_date"] = datetime.now(pytz.timezone('Asia/Kolkata'))
    each_data["PerShare_INR"] = float(each_data["PerShare_INR"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    each_data["TodaysValue"] = float(each_data["TodaysValue"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))

    taxSlab = get_tax_slab(each_data['Buy_Date'])
    amtPaidTaxForNSUDF = (
        (each_data['Available_Sell'] * each_data['Price_Bought'])) * each_data['RupeeRate']
    each_data["TaxNeedtoPay"] = round(
        (((each_data['Available_Sell'] * livePrice) * todaysRP) - amtPaidTaxForNSUDF) * taxSlab, 2)

    # taxNeedtoPay_tmp = each_data["TaxNeedtoPay"].replace("₹", "").replace("?", "").replace(",","").replace(" ","")
    # taxNeedtoPay = 0
    # if '-' in taxNeedtoPay_tmp:
    #     taxNeedtoPay = '-' + taxNeedtoPay_tmp[:-1]

    # each_data["TaxNeedtoPay"] = taxNeedtoPay
    each_data["Max_Value_FY"] = float(each_data["Max_Value_FY"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    each_data["FY_Closing_Value"] = float(each_data["FY_Closing_Value"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    each_data["Buy_Year"] = float(each_data["Buy_Year"])
    each_data["Buy_Month"] = float(each_data["Buy_Month"])
    each_data["InvestmentType"] = "NSU"
    each_data["livePrice"] = float(livePrice)
    each_data["liveRupeePrice"] = float(todaysRP)

    del each_data['Buy_Date_formatted']

if args.elk:
    helpers.bulk(login_eks(), genarate_data(NSU_dict, 'psardar-shares'))
    helpers.bulk(login_eks(), genarate_data(NSU_dict, 'psardar-shares-daily'))

print('\n')

###################Sell Calculations###################
dfSellOut = pd.read_sql_query("SELECT * from SellOut", conn)

dfSellOut['Buy_Date_formatted'] = pd.to_datetime(
    dfSellOut['Buy_Date']).dt.strftime("%d/%m/%Y")

dfSellOut['Buy_Year'] = pd.to_datetime(
    dfSellOut['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%Y")
dfSellOut['Buy_Month'] = pd.to_datetime(
    dfSellOut['Buy_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime("%m")

Max_Price = []
FY_Closing_Price = []
for cnt in range(0, len(dfSellOut['Buy_Year'])):
    if int(dfSellOut['Buy_Month'][cnt]) > 3:
        Max_Price.append(round(gd("nvda", start_date="01/04/" + dfSellOut['Buy_Year'][cnt], end_date="31/03/" + str(
            int(dfSellOut['Buy_Year'][cnt]) + 1), index_as_date=True, interval="1mo")['high'].max(), 0))
        try:
            FY_Closing_Price.append(round(gd("nvda", start_date="01/03/" + str(int(dfSellOut['Buy_Year'][cnt]) + 1), end_date="31/03/" + str(
                int(dfSellOut['Buy_Year'][cnt]) + 1), index_as_date=True, interval="1mo")['close'].max(), 0))
        except:
            FY_Closing_Price.append(0)
    else:
        Max_Price.append(round(gd("nvda", start_date="01/04/" + str(int(
            dfSellOut['Buy_Year'][cnt]) - 1), end_date="31/03/" + dfSellOut['Buy_Year'][cnt], index_as_date=True, interval="1mo")['high'].max(), 0))
        FY_Closing_Price.append(round(gd("nvda", start_date="01/03/" + str(int(dfSellOut['Buy_Year'][cnt]) - 1), end_date="31/03/" + str(
            int(dfSellOut['Buy_Year'][cnt]) - 1), index_as_date=True, interval="1mo")['close'].max(), 0))

dfSellOut['Max_Price'] = Max_Price
dfSellOut['FY_Closing_Price'] = FY_Closing_Price
dfSellOut['Max_Value_FY'] = rupeesPrint((dfSellOut['Qty_Sold'].mul(
    dfSellOut['BuyRupeeRate'])).mul(dfSellOut['Max_Price']))
dfSellOut['FY_Closing_Value'] = rupeesPrint((dfSellOut['Qty_Sold'].mul(
    dfSellOut['BuyRupeeRate'])).mul(dfSellOut['FY_Closing_Price']))
dfSellOut['InitialValue'] = (dfSellOut['Qty_Sold'].mul(
    dfSellOut['Price_Bought'])).mul(dfSellOut['BuyRupeeRate'])

dfSellOut['Buy_Date'] = pd.to_datetime(dfSellOut['Buy_Date']).dt.date
dfSellOut = dfSellOut.sort_values(by=['Buy_Date'], ascending=True)

dfSellOut['Sell_Date_formatted'] = pd.to_datetime(
    dfSellOut['Sell_Date']).dt.strftime("%d/%m/%Y")
dfSellOut['Sell_Date'] = pd.to_datetime(dfSellOut['Sell_Date']).dt.date

dfSellOut['Buy@R'] = ((dfSellOut['Qty_Sold'].mul(dfSellOut['Price_Bought']))
                      * dfSellOut['BuyRupeeRate'])
dfSellOut['Sold@R'] = ((dfSellOut['Qty_Sold'].mul(dfSellOut['Price_Sell']))
                       * dfSellOut['SellRupeeRate'])
dfSellOut['ProfitPercent'] = round(((dfSellOut['Sold@R'] - dfSellOut['Buy@R'] - (
    (dfSellOut['Sold@R'] - dfSellOut['Buy@R']) * taxSlab)) / dfSellOut['Buy@R']) * 100, 2)
dfSellOut['ProfitNSU'] = rupeesPrint(
    round((dfSellOut[dfSellOut['Type'] == 'NSU']['Sold@R']), 2))
dfSellOut['ProfitESPP'] = round((dfSellOut[dfSellOut['Type'] == 'ESPP']
                                ['Sold@R'] - dfSellOut[dfSellOut['Type'] == 'ESPP']['Buy@R']), 2)
dfSellOut['TaxNeedToBePaid'] = rupeesPrint(
    round(((dfSellOut['Sold@R'] - dfSellOut['Buy@R']) * taxSlab), 2))

print('\t\t\t\t\tSell Distribution')
# print('\t\t\t\t\t----------------')
dfSellOut = dfSellOut.fillna(0)
dfSellOut = dfSellOut.round(1)
# print(dfSellOut[['Type', 'Buy_Date', 'Sell_Date', 'Qty_Sold', 'Price_Bought', 'BuyRupeeRate', 'Price_Sell', 'SellRupeeRate', 'ProfitESPP', 'ProfitNSU', 'TaxNeedToBePaid', 'ProfitPercent']].to_string(index=False))
print(tabulate(dfSellOut[['Type', 'Buy_Date_formatted', 'Sell_Date_formatted', 'Qty_Sold', 'Price_Bought', 'BuyRupeeRate', 'Price_Sell', 'SellRupeeRate', 'ProfitESPP',
      'ProfitNSU', 'TaxNeedToBePaid', 'ProfitPercent', 'InitialValue', 'Max_Value_FY', 'FY_Closing_Value']], headers='keys', tablefmt='pretty', colalign=("centre",)))
print('\n\n-----------------------------------------------------------')
print('Realised Profit\t\t\t: ' + rupeesPrint(round(((dfSellOut[dfSellOut['Type'] == 'NSU']['Sold@R']).sum() + (
    dfSellOut[dfSellOut['Type'] == 'ESPP']['Sold@R'] - dfSellOut[dfSellOut['Type'] == 'ESPP']['Buy@R']).sum() - ((dfSellOut['Sold@R'] - dfSellOut['Buy@R']) * taxSlab).sum()), 2)))
print('-----------------------------------------------------------')
print('Unrealised Profit(After TAX)\t: '
      + rupeesPrint((currentESPPValue - (((((dfESPP['Available_Sell'].mul(livePrice) * todaysRP) - taxableInvestedMoneyESPP) * taxSlab).sum()))) + (currentNSUValue - (taxableAmountNSU * taxSlab))))
print('-----------------------------------------------------------')
print('Unrealised Profit(Before TAX)\t: ' + rupeesPrint(((dfESPP['Available_Sell'].mul(
    livePrice) * todaysRP).sum()) + ((dfNSU['Available_Sell'].mul(livePrice) * todaysRP).sum())))
print('-----------------------------------------------------------')
print('Total Shares Available\t\t: ' +
      str(int(dfNSU['Available_Sell'].sum()) + int(dfESPP['Available_Sell'].sum())))
print('-----------------------------------------------------------')
print('\n')

SellOut_dict = dfSellOut.to_dict('records')
for each_data in SellOut_dict:
    each_data["current_date"] = datetime.now(pytz.timezone('Asia/Kolkata'))
    each_data["Max_Value_FY"] = float(each_data["Max_Value_FY"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    each_data["FY_Closing_Value"] = float(each_data["FY_Closing_Value"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    each_data["ProfitNSU"] = float(each_data["ProfitNSU"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))
    each_data["TaxNeedToBePaid"] = float(each_data["TaxNeedToBePaid"].replace(
        "₹", "").replace("?", "").replace(",", "").replace(" ", ""))

    each_data["Buy_Year"] = float(each_data["Buy_Year"])
    each_data["Buy_Month"] = float(each_data["Buy_Month"])
    each_data["InvestmentType"] = "SellOut"
    each_data["livePrice"] = float(livePrice)
    each_data["liveRupeePrice"] = float(todaysRP)

    del each_data['Buy_Date_formatted']
    del each_data['Sell_Date_formatted']

if args.elk:
    helpers.bulk(login_eks(), genarate_data(SellOut_dict, 'psardar-shares'))

conn.commit()
conn.close()
print('\n\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n')

fy_date = datetime(date.today().year, 4, 1)
shares_dict = []
dfs_arr = [dfNSU.iterrows(), dfESPP.iterrows()]
for each_df in dfs_arr:
    for index, row in each_df:
        shares_dict_row = {
            "CountryName": "2-United States Of America",
            "CountryCodeExcludingIndia": "2",
            "NameOfEntity": "NVIDIA",
            "AddressOfEntity": "2788 San Tomas Expressway Santa Clara,CA",
            "ZipCode": "95051",
            "NatureOfEntity": "Shares",
            "InterestAcquiringDate": datetime.strptime(row['Buy_Date_formatted'], "%d/%m/%Y").strftime("%Y-%m-%d"),
            "InitialValOfInvstmnt": int(round(float(str(row['InitialValue']).replace("₹", "").replace("?", "").replace(",", "")), 0)),
            "PeakBalanceDuringPeriod": int(round(float(str(row['Max_Value_FY']).replace("₹", "").replace("?", "").replace(",", "")), 0)),
            "ClosingBalance": int(round(float(str(row['FY_Closing_Value']).replace("₹", "").replace("?", "").replace(",", "")), 0)),
            "TotGrossAmtPaidCredited": 0,
            "TotGrossProceeds": 0
        }
        invest_date = datetime.strptime(row['Buy_Date_formatted'], "%d/%m/%Y")
        if invest_date < fy_date:
            shares_dict.append(shares_dict_row)

if args.itr:
    with open(os.path.join(sys.path[0], "AY_" + str(date.today().year) + "_Shares.json"), "w") as outfile:
        outfile.write(json.dumps(shares_dict, indent=4))
