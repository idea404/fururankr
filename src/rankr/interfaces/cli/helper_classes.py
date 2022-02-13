from typing import Callable, Optional

from rankr.actions import calculates, finds, creates
from rankr.db import scoped_session_context_manager
from rankr.interfaces.cli.connections import SessionConnections


class CLIActions:
    @staticmethod
    def add_furus_by_handles(conns: SessionConnections):
        handles_str = input(
            "Please type Twitter handles comma-separated (e.g. ZackMorris,DBTrades,FuruForLife)\n"
        )
        handles = [h.strip() for h in handles_str.split(",")]
        for handle in handles:
            calculates.add_and_score_furu_from_handle(conns.session, conns.tweepy, handle)

    @staticmethod
    def add_furus_by_ticker_symbols(conns: SessionConnections):
        symbols_str = input(
            "Please type tickers separated by comma (e.g. AAPL,NFLX,TWTR)\n"
        )
        symbols = symbols_str.split(",")
        finds.find_validate_create_score_furus_for_tickers(
            conns.session, conns.tweepy, symbols
        )

    @staticmethod
    def update_furu_tweets_and_raw_positions(conns: SessionConnections):
        session, tweepy = conns.session, conns.tweepy

        creates.evaluate_error_furus_reactivation(session)
        furus = finds.get_active_furus(session)

        v = input(
            f"Will update all {len(furus)} furus with new tweets and raw positions. Are you sure? (Y/N)\n"
        )
        if v.upper() == "Y":
            calculates.update_tweets_and_raw_positions_multi_threaded_io(
                session, tweepy, furus
            )
        else:
            print("Skipped.")

    @staticmethod
    def update_prices_for_raw_positions(conns: SessionConnections):
        v = input(
            "Will attempt to update raw pricing for all raw positions in DB. Are you sure?\n"
        )
        if v.upper() == "Y":
            with scoped_session_context_manager(conns.scoped_session_class) as session:
                creates.fill_prices_for_raw_furu_positions(session)
                calculates.update_furu_scores_multi_threaded(session)
        else:
            print("Skipped.")

    @staticmethod
    def print_golden_portfolio(conns: SessionConnections):
        from rankr.scripts.analytics.print_golden_portfolio import get_golden_portfolio

        golden_folio = get_golden_portfolio(conns.scoped_session_class())
        print(
            golden_folio.head(30).to_string(
                columns=["position", "symbol", "golden_rank"], index=False
            )
        )

    @staticmethod
    def print_leaderboard(conns: SessionConnections):
        from rankr.scripts.analytics.print_leaderboard import get_leaderboard

        df = get_leaderboard(conns.scoped_session_class())
        print(
            df[
                [
                    "handle",
                    "accuracy",
                    "performance_score",
                    "total_trades_measured",
                    "average_profit",
                    "average_loss",
                    "average_holding_period_days",
                ]
            ].to_string(index=False)
        )

    @staticmethod
    def print_best_trades(conns: SessionConnections):
        from rankr.scripts.analytics.print_best_trades import (
            get_best_trades_print_string,
        )

        string = get_best_trades_print_string(conns.scoped_session_class())
        print(string)

    @staticmethod
    def print_scores_for_tickers(conns: SessionConnections):
        raise NotImplementedError("TODO")  # TODO


class FunctionFactory:
    mapper = {
        11: CLIActions.add_furus_by_handles,
        12: CLIActions.add_furus_by_ticker_symbols,
        22: CLIActions.update_furu_tweets_and_raw_positions,
        23: CLIActions.update_prices_for_raw_positions,
        31: CLIActions.print_golden_portfolio,
        32: CLIActions.print_leaderboard,
        33: CLIActions.print_best_trades,
        34: CLIActions.print_scores_for_tickers,
    }

    @classmethod
    def get_function(cls, input_int: int) -> Optional[Callable]:
        if input_int not in cls.mapper:
            print("Input number does not match a function. Provide valid input.\n")
            return
        return cls.mapper.get(input_int)
