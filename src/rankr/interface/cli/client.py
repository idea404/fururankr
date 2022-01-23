import dataclasses
from typing import Callable

from sqlalchemy.orm import Session
from tweepy import API

from rankr.actions import instantiate_api_session_from_cfg
from rankr.db import create_db_scoped_session


@dataclasses.dataclass
class SessionConnections:
    session: Session
    tweepy: API


class CLIClient:
    welcome_string = (
        "\n"
        "Fururankr ready. Command overview:\n"
        "  Actions\n"
        "    11. Add furus by handles\n"
        "    12. Add furus by tickers\n"
        "  Update Actions\n"
        "    22. Update furu tweets from Twitter and raw positions db tweets\n"
        "    23. Update prices for raw positions\n"
        "  Analytics\n"
        "    31. Print Golden Portfolio\n"
        "    32. Print Leaderboard\n"
        "    33. Print Best Trades\n"
        "    34. Print Scores for tickers\n"
        "  Exit\n"
        "    0. Exit\n"
        "\n"
    )

    def __init__(self):
        self.factory = FunctionFactory
        self.conns = SessionConnections(
            session=create_db_scoped_session()(),
            tweepy=instantiate_api_session_from_cfg(),
        )

    def get_user_input(self) -> int:
        v = input(self.welcome_string)
        print("\n")
        try:
            v = int(v)
            return v
        except Exception as ex:
            print(
                f"Value parsing failed. Please provide a valid value value. Try again. (error={ex})"
            )
            self.get_user_input()

    def start(self):
        v = self.get_user_input()
        func = self.factory.get_function(v)
        func(self.conns)
        print("\n")
        self.conns.session.close()


class CLIActions:
    @staticmethod
    def add_furus_by_handles(conns: SessionConnections):
        from rankr.actions import calculates

        handles_str = input(
            "Please type Twitter handles comma-separated (e.g. ZackMorris,DBTrades,FuruForLife)\n"
        )
        handles = [h.strip() for h in handles_str.split(",")]
        for handle in handles:
            calculates.add_and_score_furu_from_handle(
                conns.session, conns.tweepy, handle
            )

    @staticmethod
    def add_furus_by_ticker_symbols(conns: SessionConnections):
        symbols_str = input(
            "Please type tickers separated by comma (e.g. AAPL,NFLX,TWTR)\n"
        )
        symbols = symbols_str.split(",")
        from rankr.actions import finds

        finds.find_validate_create_score_furus_for_tickers(
            conns.session, conns.tweepy, symbols
        )

    @staticmethod
    def update_furu_tweets_and_raw_positions(conns: SessionConnections):
        session, tweepy = conns.session, conns.tweepy
        from rankr.actions import finds

        furus = finds.get_active_furus(session)
        v = input(
            f"Will update all {len(furus)} furus with new tweets and raw positions. Are you sure? (Y/N)\n"
        )
        if v.upper() == "Y":
            from rankr.actions import calculates

            calculates.update_tweets_and_raw_positions_multi_threaded(
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
            from rankr.actions import creates

            creates.fill_prices_for_raw_furu_positions(conns.session)
            from rankr.actions import calculates

            calculates.update_furu_scores_multi_threaded(conns.session)
        else:
            print("Skipped.")

    @staticmethod
    def print_golden_portfolio(conns: SessionConnections):
        from rankr.scripts.analytics.print_golden_portfolio import \
            get_golden_portfolio

        golden_folio = get_golden_portfolio(conns.session)
        print(
            golden_folio.head(30).to_string(
                columns=["position", "symbol", "golden_rank"], index=False
            )
        )

    @staticmethod
    def print_leaderboard(conns: SessionConnections):
        from rankr.scripts.analytics.print_leaderboard import get_leaderboard

        df = get_leaderboard(conns.session)
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
        from rankr.scripts.analytics.print_best_trades import \
            get_best_trades_print_string

        string = get_best_trades_print_string(conns.session)
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
    def get_function(cls, input_int: int) -> Callable:
        if input_int not in cls.mapper:
            print("Input number does not match a function. Provide valid input.\n")
            return
        return cls.mapper.get(input_int)


if __name__ == "__main__":
    c = CLIClient()
    c.start()
