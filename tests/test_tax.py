"""Capital-gains slab tests for helpers/tax.py.

Run from the repo root:  python3 -m unittest discover -s tests -t . -v
"""
import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import helpers.tax as tax_module
from helpers.tax import Tax, _as_date, _shift_years

LTCG = 0.125
STCG = 0.30


def config_with_slabs(pairs):
    """Temp tax_config.json built from [(min_years, percent)]; returns its path."""
    cfg = {"slabs": [{"holdingPeriodMinYears": y, "taxPercent": p} for y, p in pairs]}
    tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump(cfg, tmp)
    tmp.close()
    return tmp.name


class TestHoldingPeriodBoundary(unittest.TestCase):
    """A lot must not be long-term until its 2-year anniversary."""

    def setUp(self):
        self.tax = Tax()

    def test_lot_just_under_two_years_is_short_term(self):
        """Regression: a lot bought 30/08/2024 was reported at 12.5% on 16/08/2026.

        716 days / 365.2425 = 1.96 years, which round(_, 1) turned into 2.0.
        """
        self.assertEqual(self.tax.get_tax_slab(date(2024, 8, 30), as_of=date(2026, 8, 16)), STCG)

    def test_day_before_anniversary_is_short_term(self):
        self.assertEqual(self.tax.get_tax_slab(date(2024, 8, 30), as_of=date(2026, 8, 29)), STCG)

    def test_on_anniversary_is_long_term(self):
        self.assertEqual(self.tax.get_tax_slab(date(2024, 8, 30), as_of=date(2026, 8, 30)), LTCG)

    def test_day_after_anniversary_is_long_term(self):
        self.assertEqual(self.tax.get_tax_slab(date(2024, 8, 30), as_of=date(2026, 8, 31)), LTCG)

    def test_no_short_term_lot_is_ever_long_term(self):
        """Every day in the 2 years after purchase must be short-term.

        The old days/365.2425 + round(_, 1) formula flipped to 12.5% at 713 days,
        misclassifying the final ~17 days of the short-term window.
        """
        buy = date(2024, 8, 30)
        for offset in range((date(2026, 8, 30) - buy).days):
            as_of = date.fromordinal(buy.toordinal() + offset)
            self.assertEqual(
                self.tax.get_tax_slab(buy, as_of=as_of), STCG,
                "{} ({} days after {}) should be short-term".format(as_of, offset, buy),
            )

    def test_leap_day_purchase_uses_feb_28(self):
        """29 Feb 2024 + 2 years has no 29 Feb, so the anniversary lands on 28 Feb 2026."""
        buy = date(2024, 2, 29)
        self.assertEqual(_shift_years(buy, 2), date(2026, 2, 28))
        self.assertEqual(self.tax.get_tax_slab(buy, as_of=date(2026, 2, 27)), STCG)
        self.assertEqual(self.tax.get_tax_slab(buy, as_of=date(2026, 2, 28)), LTCG)

    def test_leap_day_anniversary_in_a_leap_year(self):
        self.assertEqual(_shift_years(date(2020, 2, 29), 4), date(2024, 2, 29))

    def test_long_held_lot_is_long_term(self):
        self.assertEqual(self.tax.get_tax_slab(date(2018, 1, 15), as_of=date(2026, 8, 16)), LTCG)

    def test_same_day_buy_and_sell_is_short_term(self):
        self.assertEqual(self.tax.get_tax_slab(date(2026, 8, 16), as_of=date(2026, 8, 16)), STCG)

    def test_as_of_defaults_to_today(self):
        self.assertEqual(self.tax.get_tax_slab(date.today()), STCG)
        self.assertEqual(self.tax.get_tax_slab(_shift_years(date.today(), -3)), LTCG)


class TestSoldLotUsesSellDate(unittest.TestCase):
    """For a sold lot the holding period ends at the sale, not today."""

    def setUp(self):
        self.tax = Tax()

    def test_short_term_sale_stays_short_term_years_later(self):
        """Bought 2018, sold 17 months later: short-term forever, however long ago that was."""
        self.assertEqual(self.tax.get_tax_slab(date(2018, 1, 15), as_of=date(2019, 6, 15)), STCG)

    def test_ten_day_flip_is_short_term(self):
        self.assertEqual(self.tax.get_tax_slab(date(2024, 8, 30), as_of=date(2024, 9, 9)), STCG)

    def test_long_held_sale_is_long_term(self):
        self.assertEqual(self.tax.get_tax_slab(date(2021, 1, 15), as_of=date(2023, 5, 1)), LTCG)

    def test_sell_date_beats_today(self):
        """Same lot: short-term at its sale date, long-term if wrongly measured to today."""
        buy = date(2022, 6, 15)
        self.assertEqual(self.tax.get_tax_slab(buy, as_of=date(2023, 1, 27)), STCG)
        self.assertEqual(self.tax.get_tax_slab(buy, as_of=date(2026, 8, 16)), LTCG)


class TestMissingAndOddDates(unittest.TestCase):
    def setUp(self):
        self.tax = Tax()

    def test_db_dates_are_month_first(self):
        """The NSU / ESPP / SellOut tables store Buy_Date as MM/DD/YYYY."""
        self.assertEqual(_as_date("08/30/2024"), date(2024, 8, 30))
        self.assertEqual(_as_date("12/09/2020"), date(2020, 12, 9))

    def test_unambiguous_day_first_still_parses(self):
        self.assertEqual(_as_date("30/08/2024"), date(2024, 8, 30))

    def test_iso_datetime_string_is_truncated_to_date(self):
        self.assertEqual(_as_date("2024-08-30 00:00:00"), date(2024, 8, 30))

    def test_accepts_date_strings_and_datetimes(self):
        expected = self.tax.get_tax_slab(date(2024, 8, 30), as_of=date(2026, 8, 16))
        for buy in ("08/30/2024", "2024-08-30", "30/08/2024", datetime(2024, 8, 30, 13, 45)):
            self.assertEqual(self.tax.get_tax_slab(buy, as_of="2026-08-16"), expected)

    def test_unknown_buy_date_falls_back_to_highest_rate(self):
        """An unknown holding period must not be given the cheaper long-term rate."""
        self.assertEqual(self.tax.fix_tax_slab, STCG)
        for missing in (None, float("nan"), "", "   ", "not-a-date"):
            self.assertEqual(self.tax.get_tax_slab(missing), STCG)
            self.assertIsNone(_as_date(missing))

    def test_unparseable_as_of_falls_back_to_today(self):
        self.assertEqual(self.tax.get_tax_slab(date(2018, 1, 15), as_of="junk"), LTCG)

    def test_pandas_timestamp_and_nat(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")
        self.assertEqual(_as_date(pd.Timestamp("2024-08-30")), date(2024, 8, 30))
        self.assertIsNone(_as_date(pd.NaT))
        # A row with no sell date must not crash; it falls back to today.
        self.assertEqual(self.tax.get_tax_slab(date(2024, 8, 30), as_of=pd.NaT), STCG)


class TestSlabConfig(unittest.TestCase):
    """Rates come from configs/tax_config.json, not from hardcoded constants."""

    def test_shipped_config_matches_expected_rates(self):
        self.assertEqual(sorted(Tax().slabs), [(0, STCG), (2, LTCG)])

    def test_reads_rates_from_config(self):
        path = config_with_slabs([(3, 10.0), (0, 40.0)])
        self.addCleanup(os.unlink, path)
        with mock.patch.object(tax_module, "_TAX_CONFIG_PATH", path):
            tax = Tax()
        self.assertEqual(tax.get_tax_slab(date(2020, 1, 1), as_of=date(2022, 12, 31)), 0.40)
        self.assertEqual(tax.get_tax_slab(date(2020, 1, 1), as_of=date(2023, 1, 1)), 0.10)
        self.assertEqual(tax.fix_tax_slab, 0.40)

    def test_first_matching_slab_wins(self):
        path = config_with_slabs([(2, 12.5), (1, 20.0), (0, 30.0)])
        self.addCleanup(os.unlink, path)
        with mock.patch.object(tax_module, "_TAX_CONFIG_PATH", path):
            tax = Tax()
        buy = date(2024, 1, 1)
        self.assertEqual(tax.get_tax_slab(buy, as_of=date(2024, 6, 1)), 0.30)
        self.assertEqual(tax.get_tax_slab(buy, as_of=date(2025, 1, 1)), 0.20)
        self.assertEqual(tax.get_tax_slab(buy, as_of=date(2026, 1, 1)), 0.125)

    def test_falls_back_to_defaults_when_config_missing(self):
        with mock.patch.object(tax_module, "_TAX_CONFIG_PATH", "/nonexistent/tax_config.json"):
            tax = Tax()
        self.assertEqual(tax.slabs, [(2, LTCG), (0, STCG)])

    def test_falls_back_to_defaults_when_config_malformed(self):
        for payload in ({}, {"slabs": []}, {"slabs": "nope"}, {"slabs": [{"taxPercent": 1}]}):
            tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
            json.dump(payload, tmp)
            tmp.close()
            self.addCleanup(os.unlink, tmp.name)
            with mock.patch.object(tax_module, "_TAX_CONFIG_PATH", tmp.name):
                tax = Tax()
            self.assertEqual(tax.slabs, [(2, LTCG), (0, STCG)], "payload {!r}".format(payload))


class TestRowArithmetic(unittest.TestCase):
    """Tax, net proceeds and profit % must stay consistent with the slab."""

    # Illustrative round figures, not real holdings.
    COST = 1000000.0
    GROSS = 2200000.0

    def setUp(self):
        self.tax = Tax()
        self.gain = self.GROSS - self.COST

    def test_slab_drives_tax_and_net(self):
        """A lot inside the 2-year window pays the short-term rate, which more than doubles the tax."""
        slab = self.tax.get_tax_slab(date(2024, 8, 30), as_of=date(2026, 8, 16))
        self.assertEqual(slab, STCG)
        self.assertEqual(round(self.gain * LTCG), 150000)
        self.assertEqual(round(self.GROSS - self.gain * LTCG), 2050000)
        self.assertEqual(round(self.gain * slab), 360000)
        self.assertEqual(round(self.GROSS - self.gain * slab), 1840000)

    def test_profit_percent_is_after_tax_over_cost(self):
        """The Holdings tab shows profit net of tax: (gain − tax) / cost."""
        tax_due = round(self.gain * LTCG, 2)
        self.assertAlmostEqual(round((self.gain - tax_due) / self.COST * 100, 1), 105.0, places=1)

    def test_loss_making_lot_owes_no_tax(self):
        slab = self.tax.get_tax_slab(date(2025, 1, 1), as_of=date(2026, 8, 16))
        self.assertEqual(max(80000.0 - 100000.0, 0) * slab, 0)


class TestSlabSurvivesDataframeRounding(unittest.TestCase):
    """df.round(1) turns 0.125 into 0.1; the display builders must restore the real slab."""

    def test_round_one_would_corrupt_the_rate(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")
        df = pd.DataFrame({"TaxSlab": [LTCG, STCG]})
        self.assertEqual(list(df.round(1)["TaxSlab"]), [0.1, 0.3])

        backup = df["TaxSlab"].copy()
        rounded = df.round(1)
        rounded["TaxSlab"] = backup
        self.assertEqual(list(rounded["TaxSlab"]), [LTCG, STCG])


class TestRealDatabaseSlabs(unittest.TestCase):
    """Every lot in the live database must agree with its calendar anniversary."""

    @classmethod
    def setUpClass(cls):
        from helpers import config

        if not os.path.isfile(config.DB_PATH):
            raise unittest.SkipTest("no database at {}".format(config.DB_PATH))
        cls.db_path = config.DB_PATH
        cls.tax = Tax()

    def rows(self, table):
        import sqlite3

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            return conn.execute("select * from {}".format(table)).fetchall()
        finally:
            conn.close()

    def test_holdings_match_anniversary(self):
        today = date.today()
        for table in ("NSU", "ESPP"):
            for row in self.rows(table):
                buy = _as_date(row["Buy_Date"])
                if buy is None:
                    continue
                expected = LTCG if today >= _shift_years(buy, 2) else STCG
                self.assertEqual(
                    self.tax.get_tax_slab(buy), expected,
                    "{} lot bought {} ({} days ago)".format(table, buy, (today - buy).days),
                )

    def test_sold_lots_are_measured_to_the_sell_date(self):
        for row in self.rows("SellOut"):
            buy, sell = _as_date(row["Buy_Date"]), _as_date(row["Sell_Date"])
            if buy is None or sell is None:
                continue
            self.assertGreaterEqual(sell, buy, "sell {} precedes buy {}".format(sell, buy))
            expected = LTCG if sell >= _shift_years(buy, 2) else STCG
            self.assertEqual(
                self.tax.get_tax_slab(buy, as_of=sell), expected,
                "lot bought {} sold {} ({} days held)".format(buy, sell, (sell - buy).days),
            )

    def test_lots_sold_within_two_years_are_short_term(self):
        for row in self.rows("SellOut"):
            buy, sell = _as_date(row["Buy_Date"]), _as_date(row["Sell_Date"])
            if buy is None or sell is None or sell >= _shift_years(buy, 2):
                continue
            self.assertEqual(
                self.tax.get_tax_slab(buy, as_of=sell), STCG,
                "lot bought {} sold {} was held under two years".format(buy, sell),
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
