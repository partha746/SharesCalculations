"""SQLite access for the nvShares database (NSU/ESPP/SellOut tables)."""
import os
import sqlite3
import sys

import pandas as pd


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

