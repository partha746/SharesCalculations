"""Database access + shared table metadata."""
from contextlib import contextmanager

from helpers import config

DB_PATH = config.DB_PATH

TABLE_COLUMNS = {
    "NSU": ["Buy_Date", "Available_Sell", "Price_Bought", "RupeeRate"],
    "ESPP": ["Buy_Date", "Available_Sell", "Price_Bought", "RupeeRate", "TDS_Price"],
    "SellOut": [
        "Sell_Date", "Buy_Date", "Qty_Sold", "Price_Bought", "Price_Sell",
        "BuyRupeeRate", "SellRupeeRate", "Type",
    ],
    "Split": ["date", "split_ratio"],
}


@contextmanager
def get_db():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

