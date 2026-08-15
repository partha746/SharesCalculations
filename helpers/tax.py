"""Capital-gains tax slabs + ITR foreign-asset JSON generation."""
import json
import os
import sys
from datetime import date, datetime

from helpers.cleaning import DataCleaner

_TAX_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs", "tax_config.json"
)
_DEFAULT_SLABS = ((2, 0.125), (0, 0.30))


def _as_date(value):
    """datetime / pandas Timestamp / date string -> date. None if missing or unparseable."""
    if value is None or value != value:  # NaN / NaT
        return None
    if isinstance(value, datetime):
        try:
            return value.date()
        except ValueError:
            return None
    if isinstance(value, date):
        return value
    text = str(value).strip()[:10]
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _shift_years(d, years):
    """`d` advanced by whole calendar years; 29 Feb lands on 28 Feb in a non-leap year."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d.replace(year=d.year + years, month=2, day=28)


class Tax:
    def __init__(self) -> None:
        self.slabs = self._load_slabs()
        # Default when buy_date is unknown: the shortest-holding (so highest) slab.
        self.fix_tax_slab = max(rate for _, rate in self.slabs)

    @staticmethod
    def _load_slabs():
        """[(min_years, rate)] from configs/tax_config.json, file order preserved (first match wins)."""
        try:
            with open(_TAX_CONFIG_PATH) as f:
                raw = json.load(f).get("slabs")
        except (OSError, ValueError, AttributeError):
            raw = None
        if not isinstance(raw, list):
            return list(_DEFAULT_SLABS)
        slabs = []
        for slab in raw:
            try:
                slabs.append((int(slab["holdingPeriodMinYears"]), float(slab["taxPercent"]) / 100.0))
            except (KeyError, TypeError, ValueError):
                continue
        return slabs or list(_DEFAULT_SLABS)

    def get_tax_slab(self, buy_date, as_of=None):
        """Capital-gains rate for a lot bought on `buy_date` and sold on `as_of` (default: today).

        The holding period is compared on the calendar (2 years = the same day two years later)
        rather than as days/365, so a lot is never treated as long-term before its anniversary.
        For an already-sold lot pass the sell date as `as_of`: the holding period ends at the
        sale, not today.
        """
        buy_date = _as_date(buy_date)
        if buy_date is None:
            return self.fix_tax_slab
        as_of = _as_date(as_of) or date.today()
        for min_years, rate in self.slabs:
            if as_of >= _shift_years(buy_date, min_years):
                return rate
        return self.fix_tax_slab

    
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

