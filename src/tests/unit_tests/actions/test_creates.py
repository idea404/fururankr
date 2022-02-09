import collections
import datetime as dt
import unittest

from rankr.actions.creates import fill_position_prices_from_tickers
from rankr.db.models import (
    Ticker,
    FuruTicker,
    Furu,
    TickerHistory,
)
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

        tol = [
            Ticker("XVDR"),
            Ticker("GGGM"),
            Ticker("LAPK"),
            Ticker("BNMM"),
            Ticker("XDDR"),
            Ticker("GCGM"),
            Ticker("LQPK"),
            Ticker("BCMM"),
        ]

        tol[0].ticker_history.append(
            TickerHistory(
                date=dt.date(2021, 1, 20),
                open=1.1,
                close=1.1,
                high=1.1,
                low=1.1,
                volume=122,
            )
        )

        tol[1].ticker_history.extend(
            [
                TickerHistory(
                    date=dt.date(2021, 5, 20),
                    open=100.4,
                    close=100.4,
                    high=1.1,
                    low=1.1,
                    volume=122,
                ),
                TickerHistory(
                    date=dt.date(2021, 3, 9),
                    open=99,
                    close=99,
                    high=1.1,
                    low=1.1,
                    volume=122,
                ),
            ]
        )

        fill_position_prices_from_tickers(self.session, pppd, tol)

        self.assertEqual(dt.date(2021, 1, 20), positions[0].date_entered)
        self.assertEqual(1.1, positions[0].price_entered)

        self.assertEqual(dt.date(2021, 5, 20), positions[1].date_entered)
        self.assertEqual(100.4, positions[1].price_entered)

        self.assertEqual(dt.date(2021, 3, 9), positions[2].date_entered)
        self.assertEqual(99, positions[2].price_entered)
