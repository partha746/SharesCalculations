import sqlite3
import os
import sys

db_name = 'nvShares.db'
db_path = os.path.join(sys.path[0], 'configs', db_name)
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

split_ratio = 10

def truncate(value, decimals):
    factor = split_ratio ** decimals
    return int(value * factor) / factor

def update_table(table_name, cursor):
    if table_name == 'NSU':
        cursor.execute(f'SELECT Buy_Date, Available_Sell, Price_Bought, RupeeRate FROM {table_name}')
    elif table_name == 'ESPP':
        cursor.execute(f'SELECT Buy_Date, Available_Sell, Price_Bought, RupeeRate, TDS_Price FROM {table_name}')
    
    rows = cursor.fetchall()

    for row in rows:
        if table_name == 'NSU':
            buy_date, available_sell, price_bought, rupee_rate = row
            new_available_sell = available_sell * split_ratio
            new_price_bought = truncate(price_bought / split_ratio, 2)
            cursor.execute(f'''
                UPDATE {table_name}
                SET Available_Sell = ?, Price_Bought = ?
                WHERE Buy_Date = ? AND Available_Sell = ? AND Price_Bought = ? AND RupeeRate = ?
            ''', (new_available_sell, new_price_bought, buy_date, available_sell, price_bought, rupee_rate))
        elif table_name == 'ESPP':
            buy_date, available_sell, price_bought, rupee_rate, tds_price = row
            new_available_sell = available_sell * split_ratio
            new_price_bought = truncate(price_bought / split_ratio, 2)
            new_tds_price = truncate(tds_price / split_ratio, 2)
            cursor.execute(f'''
                UPDATE {table_name}
                SET Available_Sell = ?, Price_Bought = ?, TDS_Price = ?
                WHERE Buy_Date = ? AND Available_Sell = ? AND Price_Bought = ? AND RupeeRate = ?
            ''', (new_available_sell, new_price_bought, new_tds_price, buy_date, available_sell, price_bought, rupee_rate))

update_table('NSU', cursor)
update_table('ESPP', cursor)

conn.commit()
conn.close()

print("Database updated successfully.")