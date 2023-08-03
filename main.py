# python -m pip install pyopenssl yfinance lxml yahoo_fin requests_html setuptools pandas django tabulate
# sudo apt install sqlitebrowser

import argparse
import os
import warnings
from datetime import datetime

from django.utils.encoding import smart_str
from elasticsearch import helpers
from tabulate import tabulate

from helpers import gather_data

parser = argparse.ArgumentParser(description='Script to track NV investment')
parser.add_argument("-elk", help="Send Data to ELK", action="store_true")
parser.add_argument("-itr", help="Generate ITR json", action="store_true")
args = parser.parse_args()

warnings.filterwarnings("ignore", category=UserWarning)


taxSlab = 0.3
todaysDate = datetime.now()

###################Update database with rupee Rate###################
rupee_conv_obj = gather_data.RupeeConv()
db_obj = gather_data.DB()
datacleaner_obj = gather_data.DataCleaner()
ekshelper_obj = gather_data.EksHelper()
tax_obj = gather_data.Tax()

rs_sym = rupee_conv_obj.rupee_symbol

if not os.path.isfile(db_obj.db_path):
    db_obj.create_tables(db_obj.db_path)

db_status = db_obj.check_for_empty_db()
if db_status['NSU']:
    rupee_conv_obj.update_null_rupees_rate('NSU', 'Buy_Date', 'RupeeRate')

if db_status['ESPP']:
    rupee_conv_obj.update_null_rupees_rate('ESPP', 'Buy_Date', 'RupeeRate')

if db_status['SellOut']:
    rupee_conv_obj.update_null_rupees_rate('SellOut', 'Buy_Date', 'BuyRupeeRate')
    rupee_conv_obj.update_null_rupees_rate('SellOut', 'Sell_Date', 'SellRupeeRate')

livePrice, todaysRP = rupee_conv_obj.get_live_price()

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#
arrowRT = u'\u2192'
print('-'*26)
print('NVDA\t\t: $ ' + str(livePrice))
print('USD ' + smart_str(arrowRT) + ' INR\t: ' + rupee_conv_obj.print_rupees(todaysRP))
print('-'*26)

###################Stock Calculations###################
# babel.numbers.format_currency(10000, 'INR', locale='en_IN')
if args.elk:
    ekshelper_obj.login_eks().indices.delete(index='psardar-shares', ignore_unavailable=True)

types =['NSU', 'ESPP']
all_qty, all_profit_after_tax, all_tax = 0, 0, 0
for type in types:
    if db_status[type]:
        df, currentValue, taxableInvestedMoney, total_qty, total_capital_gain, total_tds, avg_buy_price, avg_profit_percent = gather_data.OwnStockData().generate_display_data(type=type)
        
        print('\t\t\t\t\t' + type + ' Distribution')
        if type == 'ESPP':
            print(tabulate(df[['Buy_Date_formatted', 'Available_Sell', 'Price_Bought', 'RupeeRate', 'TDS_Price', 'PerShare_INR', 'ProfitPercent', 'OwnInvestedMoney',
            'TodaysValue', 'TaxNeedtoPay', 'InitialValue', 'Max_Value_FY', 'FY_Closing_Value', 'CompanyInvestedMoney']], headers='keys', tablefmt='pretty', colalign=("centre",)))
            Price_Bought = 'TDS_Price_raw'
        if type == 'NSU':
            print(tabulate(df[['Buy_Date_formatted', 'Available_Sell', 'Price_Bought', 'RupeeRate', 'PerShare_INR', 'ProfitPercent', 'TodaysValue',
            'TaxNeedtoPay', 'InitialValue', 'Max_Value_FY', 'FY_Closing_Value']], headers='keys', tablefmt='pretty', colalign=("centre",)))
            Price_Bought = 'Price_Bought_raw'

        print('-----------------------------------------')
        print(type + ' Available to sell \t: ' + str(total_qty))
        print(type + ' Profit after TAX \t: ' + rupee_conv_obj.print_rupees(total_capital_gain))
        print('Total Tax to be Paid \t: ' + rupee_conv_obj.print_rupees(total_tds))
        print(type + ' Average Buy \t: $ ' + str(avg_buy_price))
        print(type + ' Average Profit \t: ' + str(avg_profit_percent) + '%')
        print('________________________________________')
        print('\n')
        
        all_qty += total_qty
        all_profit_after_tax += total_capital_gain
        all_tax += total_tds

        if args.elk:
            helpers.bulk(ekshelper_obj.login_eks(), ekshelper_obj.generate_kibana_data_available_stock(type=type, data=df, index_name='psardar-shares'))
            helpers.bulk(ekshelper_obj.login_eks(), ekshelper_obj.generate_kibana_data_available_stock(type=type, data=df, index_name='psardar-shares-daily'))

###################Sell Calculations###################
if db_status['SellOut']:
    dfSellOut, sell_profit = gather_data.OwnStockData().generate_sellout_display_data()
    print(tabulate(dfSellOut[['Type', 'Buy_Date_formatted', 'Sell_Date_formatted', 'Qty_Sold', 'Price_Bought', 'BuyRupeeRate', 'Price_Sell', 'SellRupeeRate', 'ProfitESPP',
        'ProfitNSU', 'TaxNeedToBePaid', 'ProfitPercent', 'InitialValue', 'Max_Value_FY', 'FY_Closing_Value']], headers='keys', tablefmt='pretty', colalign=("centre",)))

    if args.elk:
        helpers.bulk(ekshelper_obj.login_eks(), ekshelper_obj.generate_kibana_data_sold_stock(data=dfSellOut, index_name='psardar-shares'))

if db_status['NSU'] and db_status['ESPP']:
    print('\n\n-----------------------------------------------------------')
    print('Total Shares Available\t\t: ' + str(all_qty))
    print('-----------------------------------------------------------')
    print('Unrealised Profit(After TAX)\t: ' + rupee_conv_obj.print_rupees(all_profit_after_tax))
    print('-----------------------------------------------------------')
    print('Unrealised Profit(Before TAX)\t: ' + rupee_conv_obj.print_rupees(all_profit_after_tax + all_tax))
    print('-----------------------------------------------------------')

if db_status['SellOut']:
    print('Realised Profit\t\t\t: ' + rupee_conv_obj.print_rupees(round(sell_profit), 2))
    print('-----------------------------------------------------------')

print('\n' + 'X'*130 + '\n')

if args.itr:
    tax_obj.generate_tax_doc()
