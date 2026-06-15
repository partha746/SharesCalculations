"""Elasticsearch/Kibana document generation (legacy ELK push)."""
import uuid
from datetime import datetime

import pytz
from elasticsearch import Elasticsearch, helpers

from helpers import config
from helpers.cleaning import DataCleaner
from helpers.tax import Tax
from helpers.market import RupeeConv


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
            f"http://{config.ELK_HOST}:{config.ELK_PORT}", api_key=(self.creds['id'], self.creds['api_key']))

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

