import datetime as dt
import enum
from typing import List, Optional

import pandas as pd
from sqlalchemy import (
    Column,
    Date,
    Enum,
    Float,
    ForeignKey,
    Integer,
    PickleType,
    Text,
    create_engine,
    text,
)
from sqlalchemy.orm import declarative_base, relationship
from structlog import get_logger

from rankr.db.mixins import MixIn

Base = declarative_base()
metadata = Base.metadata

logger = get_logger()


class Furu(Base, MixIn):
    __tablename__ = "furu"

    class Status(str, enum.Enum):
        ACTIVE = "ACTV"
        CANCELLED = "CANC"
        ERROR = "ERRR"

    MIN_MONTHS_OLD = 9
    NEW_FURU_SEARCH_IN_PAST_DATE = dt.date.today() - dt.timedelta(days=14)
    FETCH_TWEET_HISTORY_CUTOFF_DATE = dt.date.today() - dt.timedelta(days=1095)
    FETCH_TWEET_VALIDATION_CUTOFF_DATE = dt.date.today() - dt.timedelta(days=365)
    MIN_TWEETS_PER_MONTH = 10
    MAX_TWEETS_PER_MONTH = 500
    MIN_TICKERS_PER_MONTH = 1
    MAX_TICKERS_PER_MONTH = 15
    MAX_TOTAL_TWEETS = MAX_TWEETS_PER_MONTH * 24
    MAX_TRIAL_WINDOW_WEEKS = 3
    MAX_FETCH_ATTEMPTS = 3
    MIN_WEEKS_UNTIL_REACTIVATION = 2

    DAYS_TAKEN_TO_EXIT_POSITION = 3
    DAYS_SILENCE_FOR_POSITION_EXIT = 45

    id = Column(Integer, primary_key=True)
    handle = Column(Text, nullable=False)
    accuracy = Column(Float, server_default=text("null"))
    average_profit = Column(Float, server_default=text("null"))
    average_holding_period_days = Column(Integer, server_default=text("null"))
    total_trades_measured = Column(Integer, server_default=text("null"))
    date_last_updated = Column(Date, server_default=text("null"))
    average_loss = Column(Float, server_default=text("null"))
    performance_score = Column(Float, server_default=text("null"))
    expected_return = Column(Float, server_default=text("null"))
    status = Column(Enum(*Status), nullable=False, server_default=text("'ACTV'"))

    positions: List["FuruTicker"] = relationship("FuruTicker", backref="furu")
    furu_tweets: List["FuruTweet"] = relationship("FuruTweet", backref="furu")
    last_fetch_failure_dates: List["FuruFetchFailure"] = relationship(
        "FuruFetchFailure", backref="furu"
    )

    def __init__(
        self,
        handle: str,
        accuracy: float = None,
        average_profit: float = None,
        avg_holding_period: int = None,
        total_trades_measured: int = None,
        average_loss: float = None,
        performance_score: float = None,
        expected_return: float = None,
        status: str = "ACTV",
    ):
        self.handle = handle
        self.accuracy = accuracy
        self.average_profit = average_profit
        self.average_holding_period_days = avg_holding_period
        self.total_trades_measured = total_trades_measured
        self.average_loss = average_loss
        self.performance_score = performance_score
        self.expected_return = expected_return
        self.status = status

    def __str__(self):
        return (
            f"Furu {self.id} @{self.handle} [🎯: {int(round(self.accuracy, 2) * 100) if self.accuracy is not None else None}%, "
            f"🔺: {int(round(self.average_profit, 2) * 100) if self.average_profit is not None else None}%, "
            f"🔻: {int(round(self.average_loss, 2) * 100) if self.average_loss is not None else None}%, "
            f"🎲: {self.total_trades_measured}]"
        )

    def __repr__(self):
        return str(self)

    def get_furu_position_by_ticker_and_entry_date(
        self, ticker_obj, entry_date: dt.date
    ) -> Optional["FuruTicker"]:
        assert isinstance(
            ticker_obj, Ticker
        ), f"Only accepts Ticker objects as parameter"
        positions_at_earlier_start = []
        for pos in [
            pos for pos in self.positions if pos.alpha_ticker == ticker_obj.symbol
        ]:
            if pos.date_closed is not None:
                if pos.date_entered <= entry_date <= pos.date_closed:
                    positions_at_earlier_start.append(pos)
            else:
                if pos.date_entered <= entry_date:
                    positions_at_earlier_start.append(pos)

        if positions_at_earlier_start:
            if len(positions_at_earlier_start) > 1:
                logger.warning(
                    f"{len(positions_at_earlier_start)} positions at "
                    f"earlier dates found for {ticker_obj} "
                    f"and furu={self}. Deleting intersecting position."
                )
                all_dated = {p.date_entered: p for p in positions_at_earlier_start}
                min_date = min(all_dated.keys())
                for k in all_dated.keys():
                    if k != min_date:
                        self.positions.remove(all_dated[k])
                return all_dated[min_date]

            return positions_at_earlier_start[0]

        return None

    def get_furu_position_by_symbol_and_entry_date(
        self, ticker_acronym: str, entry_date: dt.date
    ) -> Optional["FuruTicker"]:
        positions_at_earlier_start = []
        for pos in [
            pos for pos in self.positions if pos.alpha_ticker == ticker_acronym
        ]:
            if pos.date_closed is not None:
                if pos.date_entered <= entry_date <= pos.date_closed:
                    positions_at_earlier_start.append(pos)
            else:
                if pos.date_entered <= entry_date:
                    positions_at_earlier_start.append(pos)

        if positions_at_earlier_start:
            if len(positions_at_earlier_start) > 1:
                logger.warning(
                    f"{len(positions_at_earlier_start)} positions at "
                    f"earlier dates found for symbol={ticker_acronym} "
                    f"and furu={self}. Deleting intersecting position."
                )
                all_dated = {p.date_entered: p for p in positions_at_earlier_start}
                min_date = min(all_dated.keys())
                for k in all_dated.keys():
                    if k != min_date:
                        self.positions.remove(all_dated[k])
                return all_dated[min_date]

            return positions_at_earlier_start[0]

        return None

    def get_new_furu_tweets(self) -> List:
        logger.info(f"Getting new tweets from FuruTweets for {self}")
        relevant_furu_tweets = [
            ft
            for ft in self.furu_tweets
            if self.date_last_updated is None
            or (
                ft.tweets_max_date is not None
                and ft.tweets_max_date >= self.date_last_updated
            )
        ]
        if relevant_furu_tweets:
            return [
                t
                for ft in relevant_furu_tweets
                for t in ft.tweets
                if self.date_last_updated is None
                or (t.created_at.date() >= self.date_last_updated)
            ]
        return []

    def get_all_furu_tweets(self) -> List:
        logger.info(f"Fetching all furu tweets for {self}")
        return [t for ft in self.furu_tweets for t in ft.tweets]

    @property
    def has_new_furu_tweets(self) -> bool:
        if self.furu_tweets:
            ft = self.furu_tweets[-1]
            if self.date_last_updated is None or (
                ft.tweets_max_date is not None
                and ft.tweets_max_date >= self.date_last_updated
            ):
                return True
        return False

    @property
    def latest_tweet_date(self) -> dt.date:
        for ft in self.furu_tweets[::-1]:
            if ft.tweets_max_date is not None:
                return ft.tweets_max_date
        return None

    def get_earliest_cutoff_date_on_open_positions(self) -> dt.date:
        open_positions = [
            position for position in self.positions if position.date_closed is None
        ]
        if open_positions:
            return min(position.date_entered for position in open_positions)
        return Furu.FETCH_TWEET_HISTORY_CUTOFF_DATE

    def get_tweets_cutoff_date(self) -> dt.date:
        if self.furu_tweets:
            max_date = self.furu_tweets[-1].tweets_max_date
            if max_date is not None:
                return max_date
        return Furu.FETCH_TWEET_HISTORY_CUTOFF_DATE

    def register_data_fetch_fail(self):
        logger.info(f"Registering fail data fetch date for {self}")
        fetch_failure = FuruFetchFailure(fetch_failure_date=dt.date.today())
        self.last_fetch_failure_dates.append(fetch_failure)
        self.evaluate_status_for_error()

    def evaluate_status_for_error(self):
        from_date = dt.date.today() - dt.timedelta(weeks=self.MAX_TRIAL_WINDOW_WEEKS)
        relevant_fetch_attempts = [
            f
            for f in self.last_fetch_failure_dates
            if from_date <= f.fetch_failure_date
        ]
        if len(relevant_fetch_attempts) >= self.MAX_FETCH_ATTEMPTS:
            logger.info(
                f"Setting status to ERRR for {self} since failed to fetch data {len(relevant_fetch_attempts)} times"
            )
            self.status = Furu.Status.ERROR

    def evaluate_status_activation(self):
        most_recent_failure_date = max(
            f.fetch_failure_date for f in self.last_fetch_failure_dates
        )
        time_difference = dt.date.today() - most_recent_failure_date
        if time_difference.days >= self.MIN_WEEKS_UNTIL_REACTIVATION * 7:
            logger.info(
                f"Reactivating {self} as has been in error or cancelled for {time_difference.days} days"
            )
            self.status = Furu.Status.ACTIVE


class Ticker(Base, MixIn):
    __tablename__ = "ticker"

    MINIMUM_PRICE = 0.00001
    MAX_FETCH_ATTEMPTS = 3
    MAX_TRIAL_WINDOW_WEEKS = 8
    MIN_WEEKS_UNTIL_REACTIVATION = 12

    class Status(str, enum.Enum):
        ACTIVE = "ACTV"
        CANCELLED = "CANC"

    id = Column(Integer, primary_key=True)
    symbol = Column(Text, nullable=False, unique=True)
    company_name = Column(Text, server_default=text("null"))
    date_last_updated = Column(Date, server_default=text("null"))
    status = Column(Enum(*Status), nullable=False, server_default=text("'ACTV'"))

    last_fetch_failure_dates: List["TickerFetchFailure"] = relationship(
        "TickerFetchFailure", backref="ticker"
    )
    ticker_history: List["TickerHistory"] = relationship(
        "TickerHistory", back_populates="ticker"
    )
    positions: List["FuruTicker"] = relationship("FuruTicker", back_populates="ticker")

    def __init__(self, symbol: str, company_name: str = None):
        self.symbol = symbol
        self.company_name = company_name

    def __str__(self):
        return (
            f"Ticker ${self.symbol} "
            f"[last updated: "
            f"{self.date_last_updated.strftime('%Y-%m-%d') if self.date_last_updated is not None else None}]"
        )

    def __repr__(self):
        return str(self)

    def get_history_at_date(self, date: dt.date) -> Optional["TickerHistory"]:
        matching_histories = [h for h in self.ticker_history if h.date == date]
        if matching_histories:
            return matching_histories[0]
        return None

    def get_nearest_history_to_date(self, date: dt.date) -> Optional["TickerHistory"]:
        histories = {abs((date - hist.date)).days: hist for hist in self.ticker_history}
        if histories:
            min_diff = min(histories.keys())
            return histories[min_diff]
        return None

    def get_history_at_after_date(
        self, date: dt.date, max_day_distance=5
    ) -> Optional["TickerHistory"]:
        histories = {
            abs((date - hist.date)).days: hist
            for hist in self.ticker_history
            if hist.date >= date and abs((date - hist.date)).days <= max_day_distance
        }
        if histories:
            min_diff = min(histories.keys())
            return histories[min_diff]
        raise MissingHistoryError(
            f"Missing Ticker History for {self} from date={date} to {max_day_distance} days on."
        )

    def get_earliest_history_date(self) -> dt.date:
        if self.ticker_history:
            return min(history.date for history in self.ticker_history)
        return None

    def get_latest_history_date(self) -> dt.date:
        if self.ticker_history:
            return max(history.date for history in self.ticker_history)
        return None

    def add_df_to_history(self, df: pd.DataFrame):
        ticker_history_dates = [h.date for h in self.ticker_history]
        yf_history_tuples = [
            yt
            for yt in df.itertuples()
            if yt.Index.date() not in ticker_history_dates
            and yt.Close >= self.MINIMUM_PRICE
            and yt.Open >= self.MINIMUM_PRICE
        ]
        if yf_history_tuples:
            logger.info(f"Adding {len(yf_history_tuples)} rows of history to {self}")
            for tup in yf_history_tuples:
                ticker_history = TickerHistory(
                    date=tup.Index.date(),
                    high=tup.High,
                    low=tup.Low,
                    close=tup.Close,
                    open=tup.Open,
                    volume=tup.Volume,
                )
                self.ticker_history.append(ticker_history)
            self.date_last_updated = dt.date.today()

    def register_data_fetch_fail(self):
        logger.info(f"Registering fail data fetch date for {self}")
        fetch_failure = TickerFetchFailure(fetch_failure_date=dt.date.today())
        self.last_fetch_failure_dates.append(fetch_failure)
        self.evaluate_status_cancellation()

    def evaluate_status_cancellation(self):
        from_date = dt.date.today() - dt.timedelta(weeks=self.MAX_TRIAL_WINDOW_WEEKS)
        relevant_fetch_attempts = [
            f
            for f in self.last_fetch_failure_dates
            if from_date <= f.fetch_failure_date
        ]
        if len(relevant_fetch_attempts) >= self.MAX_FETCH_ATTEMPTS:
            logger.info(
                f"Cancelling {self} since failed to fetch data {len(relevant_fetch_attempts)} times"
            )
            self.status = Ticker.Status.CANCELLED

    def evaluate_status_activation(self):
        most_recent_failure_date = max(
            f.fetch_failure_date for f in self.last_fetch_failure_dates
        )
        time_difference = dt.date.today() - most_recent_failure_date
        if time_difference.days >= self.MIN_WEEKS_UNTIL_REACTIVATION * 7:
            logger.info(
                f"Reactivating {self} as has been cancelled for {time_difference.days} days"
            )
            self.status = Ticker.Status.ACTIVE


class FuruTicker(Base, MixIn):
    __tablename__ = "furu_ticker"

    id = Column(Integer, primary_key=True)
    furu_id = Column(Integer, ForeignKey("furu.id"), nullable=False)
    ticker_id = Column(Integer, ForeignKey("ticker.id"), nullable=True)
    ticker_symbol = Column(Text, nullable=True)
    date_entered = Column(Date, server_default=text("null"), nullable=False)
    date_closed = Column(Date, server_default=text("null"))
    date_last_mentioned = Column(Date, server_default=text("null"))
    price_entered = Column(Float, server_default=text("null"))
    price_closed = Column(Float, server_default=text("null"))

    ticker = relationship("Ticker", back_populates="positions")

    def __init__(
        self,
        furu: Furu = None,
        ticker: Ticker = None,
        furu_id: int = None,
        ticker_id: int = None,
        ticker_symbol: str = None,
        date_entered: dt.date = None,
        date_closed: dt.date = None,
        date_last_mentioned: dt.date = None,
        price_entered: float = None,
        price_closed: float = None,
    ):
        if furu is not None:
            self.furu = furu
        elif furu_id is not None:
            self.furu_id = furu_id
        if ticker is not None:
            self.ticker = ticker
        elif ticker_id is not None:
            self.ticker_id = ticker_id
        self.ticker_symbol = ticker_symbol
        self.date_entered = date_entered
        self.date_closed = date_closed
        self.date_last_mentioned = date_last_mentioned
        self.price_entered = price_entered
        self.price_closed = price_closed

    def __str__(self):
        return (
            f"FuruTicker ${self.alpha_ticker} held by @{self.furu.handle} "
            f"[entered: {self.date_entered} at ${round(self.price_entered, 4) if self.price_entered else None}] "
            f"[closed: {self.date_closed} at ${round(self.price_closed, 4) if self.price_closed else None}]"
        )

    def __repr__(self):
        return str(self)

    @property
    def has_price_data(self):
        return self.price_entered is not None and self.price_closed is not None

    @property
    def is_closed(self):
        return self.date_closed is not None and self.has_price_data

    @property
    def is_raw_closed(self):
        return self.date_closed is not None

    @property
    def is_missing_pricing(self):
        if self.date_entered is not None and self.price_entered is None:
            return True
        if self.date_closed is not None and self.price_closed is None:
            return True
        return False

    @property
    def is_raw(self):
        return self.ticker is None or self.ticker_id is None

    @property
    def is_open(self):
        return self.date_closed is None

    @property
    def alpha_ticker(self):
        return self.ticker_symbol or self.ticker.symbol

    @property
    def is_not_in_future(self) -> bool:
        condition = (
            self.date_closed is not None and self.date_closed <= dt.date.today()
        ) or (self.date_entered <= dt.date.today())
        return condition

    def calculate_position_return(self) -> float:
        assert (
            self.date_closed is not None
            and self.price_closed is not None
            and self.price_closed != 0
        ), "Position close date and closing price required to calculate scores."
        return (self.price_closed / self.price_entered) - 1

    def close_position(self, history: ["TickerHistory"]):
        logger.info(f"Closing position {self}")
        other_furu_positions_in_ticker = [
            pos
            for pos in self.furu.positions
            if pos.ticker_id == self.ticker_id and pos.id != self.id
        ]
        positions_closing_after_entry_date = [
            pos
            for pos in other_furu_positions_in_ticker
            if pos.date_closed is not None
            and pos.date_entered <= history.date <= pos.date_closed
        ]
        positions_closing_after_entry_date += [
            pos
            for pos in other_furu_positions_in_ticker
            if pos.date_closed is None and pos.date_entered <= history.date
        ]

        if positions_closing_after_entry_date:
            logger.warning(
                f"Found other positions with intersecting dates with proposed closing date: {history.date}"
            )
            assert len(positions_closing_after_entry_date) == 1, (
                f"Expected to find at most 1 other"
                f"position with intersecting dates. "
                f"Found: {len(positions_closing_after_entry_date)}"
            )
            intersecting_position = positions_closing_after_entry_date[0]
            logger.warning(
                f"Setting this position's close date to intersecting position's: {intersecting_position}"
            )
            self.date_closed = intersecting_position.date_closed
            self.price_closed = (
                intersecting_position.price_closed or history.get_mid_price_point()
            )
            logger.warning(f"Substituting intersecting position for this position")
            self.furu.positions.remove(intersecting_position)
        else:
            self.date_closed = history.date
            self.price_closed = history.get_mid_price_point()
            logger.info(f"Closed position: {self}")

    def close_raw_position(self, closing_date: dt.date):
        logger.debug(f"Closing raw position {self}")
        other_furu_positions_in_ticker = [
            pos
            for pos in self.furu.positions
            if pos.alpha_ticker == self.alpha_ticker and pos.id != self.id
        ]
        positions_closing_after_entry_date = [
            pos
            for pos in other_furu_positions_in_ticker
            if pos.date_closed is not None
            and pos.date_entered <= closing_date <= pos.date_closed
        ]
        positions_closing_after_entry_date += [
            pos
            for pos in other_furu_positions_in_ticker
            if pos.date_closed is None and pos.date_entered <= closing_date
        ]

        if positions_closing_after_entry_date:
            logger.warning(
                f"Found other positions with intersecting dates with proposed closing date: {closing_date}"
            )
            if len(positions_closing_after_entry_date) > 1:
                logger.warning(
                    f"{len(positions_closing_after_entry_date)} positions at "
                    f"earlier dates found for symbol={self.alpha_ticker} "
                    f"and furu={self}. Deleting intersecting position."
                )
                all_dated = {
                    p.date_entered: p for p in positions_closing_after_entry_date
                }
                min_date = min(all_dated.keys())
                for k in all_dated.keys():
                    if k != min_date:
                        self.furu.positions.remove(all_dated[k])
                intersecting_position = all_dated[min_date]
            else:
                intersecting_position = positions_closing_after_entry_date[0]
            logger.warning(
                f"Setting entry date to lowest between intersecting and self"
            )
            self.date_entered = min(
                intersecting_position.date_entered, self.date_entered
            )
            logger.warning(
                f"Setting exit date to highest between intersecting and self"
            )
            self.date_closed = (
                max(intersecting_position.date_closed, closing_date)
                if intersecting_position.date_closed is not None
                else closing_date
            )
            logger.warning(f"Substituting intersecting position for this position")
            self.furu.positions.remove(intersecting_position)
        else:
            self.date_closed = closing_date
            logger.debug(f"Closed raw position: {self}")


class FuruTweet(Base):
    __tablename__ = "furu_tweet"

    id = Column(Integer, primary_key=True)
    furu_id = Column(Integer, ForeignKey("furu.id"))
    tweets_min_date = Column(Date, server_default=text("null"))
    tweets_max_date = Column(Date, server_default=text("null"))
    tweets_min_id = Column(Integer, server_default=text("null"))
    tweets_max_id = Column(Integer, server_default=text("null"))
    tweets = Column(PickleType)

    def __init__(self, furu_id: int = None, furu: Furu = None, tweets: list = None):
        if furu:
            self.furu = furu
        if furu_id:
            self.furu_id = furu_id
        self.tweets = tweets

    def __str__(self):
        return (
            f"FuruTweet @{self.furu.handle} "
            f"[from: {self.tweets_min_date.isoformat() if self.tweets_min_date else ''}] "
            f"[to: {self.tweets_max_date.isoformat() if self.tweets_max_date else ''}]"
            f"[tweets: {len(self.tweets)}]"
        )

    def __repr__(self):
        return str(self)


class TickerHistory(Base, MixIn):
    __tablename__ = "ticker_history"

    id = Column(Integer, primary_key=True)
    ticker_id = Column(Integer, ForeignKey("ticker.id"))
    date = Column(Date, nullable=False)
    high = Column(Float, server_default=text("null"))
    open = Column(Float, server_default=text("null"))
    close = Column(Float, server_default=text("null"))
    low = Column(Float, server_default=text("null"))
    volume = Column(Integer, server_default=text("null"))

    ticker = relationship("Ticker", back_populates="ticker_history")

    def __init__(
        self,
        date: dt.date,
        high: float,
        open: float,
        close: float,
        low: float,
        volume: int,
        ticker_id: int = None,
        ticker: Ticker = None,
    ):
        self.high = high
        self.open = open
        self.close = close
        self.low = low
        self.volume = volume
        self.date = date
        if ticker_id is not None:
            self.ticker_id = ticker_id
        elif ticker is not None:
            self.ticker = ticker

    def __str__(self):
        return f"TickerHistory ${self.ticker.symbol} [{self.date.strftime('%Y-%m-%d')}]"

    def __repr__(self):
        return str(self)

    def get_mid_price_point(self) -> float:
        if self.open is None or self.close is None:
            raise TickerHistoryDataError(
                f"Open={self.open} and Close={self.close} values required for midpoint calculation (date={self.date})"
            )
        return (self.open + self.close) / 2


class TickerFetchFailure(Base, MixIn):
    __tablename__ = "ticker_fetch_failure"

    id = Column(Integer, primary_key=True)
    ticker_id = Column(Integer, ForeignKey("ticker.id"))
    fetch_failure_date = Column(Date, nullable=False)
    platform = Column(Text, nullable=True, server_default=text("YAHOO"))

    def __init__(self, fetch_failure_date: dt.date, platform: str = None):
        self.fetch_failure_date = fetch_failure_date
        if platform:
            self.platform = platform


class FuruFetchFailure(Base, MixIn):
    __tablename__ = "furu_fetch_failure"

    id = Column(Integer, primary_key=True)
    furu_id = Column(Integer, ForeignKey("furu.id"))
    fetch_failure_date = Column(Date, nullable=False)
    platform = Column(Text, nullable=True, server_default=text("TWITTER"))

    def __init__(self, fetch_failure_date: dt.date, platform: str = None):
        self.fetch_failure_date = fetch_failure_date
        if platform:
            self.platform = platform


class TickerHistoryDataError(Exception):
    pass


class MissingHistoryError(Exception):
    pass


if __name__ == "__main__":
    engine = create_engine("sqlite:///fururankr.db", echo=True)
    Base.metadata.create_all(engine)
