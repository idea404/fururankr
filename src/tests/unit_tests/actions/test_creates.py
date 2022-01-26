import collections
import datetime as dt
import unittest

import numpy as np
import pandas as pd

from rankr.actions.creates import fill_position_prices_from_df_multi_threaded
from rankr.db.models import Ticker, FuruTicker, Furu, TickerHistoryMissingError, TickerHistoryDataError
from tests.mocks.base import MockSession


class TestCreatesFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.session = MockSession()

    def setUp(self) -> None:
        self.furu_1 = Furu(handle="MaxTradezz")
        self.furu_2 = Furu(handle="JibbyTrading")
        self.furu_3 = Furu(handle="MehYouKnow")

    def test_fill_position_prices_from_df_multi_threaded(self):
        self.session.set_return_queries({Ticker: []})

        positions = [
            FuruTicker(
                furu=self.furu_1,
                ticker_symbol="XVDR",
                date_entered=dt.date(2021, 1, 20),
            ),
            FuruTicker(
                furu=self.furu_1,
                ticker_symbol="GGGM",
                date_entered=dt.date(2021, 5, 20),
            ),
            FuruTicker(
                furu=self.furu_2,
                ticker_symbol="GGGM",
                date_entered=dt.date(2021, 3, 9),
            ),
            FuruTicker(
                furu=self.furu_2,
                ticker_symbol="LAPK",
                date_entered=dt.date(2021, 4, 9),
            ),
            FuruTicker(
                furu=self.furu_1,
                ticker_symbol="BNMM",
                date_entered=dt.date(2021, 5, 20),
            ),
            FuruTicker(
                furu=self.furu_2,
                ticker_symbol="BNMM",
                date_entered=dt.date(2021, 3, 9),
            ),
            FuruTicker(
                furu=self.furu_2,
                ticker_symbol="XDDR",
                date_entered=dt.date(2021, 3, 9),
            ),
            FuruTicker(
                furu=self.furu_2,
                ticker_symbol="GCGM",
                date_entered=dt.date(2021, 3, 9),
            ),
            FuruTicker(
                furu=self.furu_1,
                ticker_symbol="LQPK",
                date_entered=dt.date(2021, 5, 20),
            ),
            FuruTicker(
                furu=self.furu_2,
                ticker_symbol="LQPK",
                date_entered=dt.date(2021, 3, 9),
            ),
            FuruTicker(
                furu=self.furu_2,
                ticker_symbol="BCMM",
                date_entered=dt.date(2021, 3, 9),
            ),
        ]
        pppd = collections.defaultdict(list)
        for pos in positions:
            pppd[pos.ticker_symbol].append(pos)

        d = dict()
        d["XVDR"] = pd.DataFrame(
            columns=["Date", "Open", "High", "Low", "Close", "Volume"],
            data=[
                [dt.datetime(2021, 1, 20), 1.1, 1.2, 1, 1.1, 12],
            ],
        ).set_index("Date")
        d["GGGM"] = pd.DataFrame(
            columns=["Date", "Open", "High", "Low", "Close", "Volume"],
            data=[
                [dt.datetime(2021, 3, 9), 99, 101, 80, 99, 122],
                [dt.datetime(2021, 5, 20), 99.8, 101.10, 98.1, 101, 222],
            ],
        ).set_index("Date")
        d["LAPK"] = pd.DataFrame(
            columns=["Date", "Open", "High", "Low", "Close", "Volume"],
            data=[
                [np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],
            ],
        ).set_index("Date")
        d["BNMM"] = pd.DataFrame(
            columns=["Date", "Open", "High", "Low", "Close", "Volume"],
            data=[
                [np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],
            ],
        ).set_index("Date")
        d["XDDR"] = pd.DataFrame(
            columns=["Date", "Open", "High", "Low", "Close", "Volume"],
            data=[
                [np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],
            ],
        ).set_index("Date")
        d["GCGM"] = pd.DataFrame(
            columns=["Date", "Open", "High", "Low", "Close", "Volume"],
            data=[
                [np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],
            ],
        ).set_index("Date")
        d["LQPK"] = pd.DataFrame(
            columns=["Date", "Open", "High", "Low", "Close", "Volume"],
            data=[
                [np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],
            ],
        ).set_index("Date")
        d["BCMM"] = pd.DataFrame(
            columns=["Date", "Open", "High", "Low", "Close", "Volume"],
            data=[
                [np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN],
            ],
        ).set_index("Date")
        d = pd.concat(d, axis=1)

        fill_position_prices_from_df_multi_threaded(self.session, pppd, d)

        expected_created_ticker_symbols = d.columns.levels[0].to_list()
        self.assertEqual(expected_created_ticker_symbols.sort(), [t.symbol for t in self.session.objects_in_db].sort())

        self.assertEqual(dt.date(2021, 1, 20), positions[0].date_entered)
        self.assertEqual(1.1, positions[0].price_entered)

        self.assertEqual(dt.date(2021, 5, 20), positions[1].date_entered)
        self.assertEqual(100.4, positions[1].price_entered)

        self.assertEqual(dt.date(2021, 3, 9), positions[2].date_entered)
        self.assertEqual(99, positions[2].price_entered)
