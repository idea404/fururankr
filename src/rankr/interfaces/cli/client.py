from rankr.actions import instantiate_api_session_from_cfg
from rankr.db import create_db_session_from_cfg
from rankr.interfaces.cli.connections import SessionConnections
from rankr.interfaces.cli.helper_classes import FunctionFactory


class CLIClient:
    welcome_string = (
        "\n"
        "Fururankr ready. Command overview:\n"
        "  Actions\n"
        "    11. Add furus by handles\n"
        "    12. Add furus by tickers\n"
        "  Update Actions\n"
        "    22. Update furu tweets from Twitter and create raw positions\n"
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
            session=create_db_session_from_cfg(),
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
        if func:
            func(self.conns)
        print("\n")
        self.conns.session.close()


if __name__ == "__main__":
    c = CLIClient()
    c.start()
