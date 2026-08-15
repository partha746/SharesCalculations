"""Backwards-compatible facade. The implementations now live in focused modules
(helpers/cleaning, db, market, tax, stock_data, eks). Import from here as before:
    from helpers import gather_data; gather_data.RupeeConv()
"""
from helpers.cleaning import DataCleaner
from helpers.db import DB
from helpers.market import RupeeConv
from helpers.tax import Tax
from helpers.stock_data import OwnStockData

__all__ = ["DataCleaner", "DB", "RupeeConv", "Tax", "OwnStockData", "EksHelper"]


def __getattr__(name):
    # EksHelper drags in elasticsearch/pytz, which the dashboard never needs.
    # Resolve it on first access so those stay optional installs.
    if name == "EksHelper":
        from helpers.eks import EksHelper

        return EksHelper
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
