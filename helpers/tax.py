"""Capital-gains tax slabs + ITR foreign-asset JSON generation."""
import json
import os
import sys
from datetime import date, datetime

from helpers.cleaning import DataCleaner


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
        from helpers.stock_data import OwnStockData  # lazy: avoids tax <-> stock_data import cycle
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

