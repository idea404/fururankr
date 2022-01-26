import datetime as dt
from typing import Dict, List, Optional

import pandas as pd
import yfinance
from sqlalchemy.orm import Session
from structlog import get_logger
from tweepy import API

from rankr.actions.finds import get_nearest_business_day_in_future
from rankr.db.models import (
    Furu,
    FuruTicker,
    FuruTweet,
    TickerHistoryMissingError,
    Ticker,
    TickerHistory,
    TickerHistoryDataError,
)


logger = get_logger()

MAX_COUNT = 10


def populate_ticker_history_from_yf(ticker: Ticker, yf_history: pd.DataFrame):
    ticker_history_dates = [h.date for h in ticker.ticker_history]
    yf_history_tuples = [
        yt
        for yt in yf_history.itertuples()
        if yt.Index.date() not in ticker_history_dates
    ]
    for tup in yf_history_tuples:
        ticker_history = TickerHistory(
            date=tup.Index.date(),
            high=tup.High if tup.High > Ticker.MINIMUM_OTC_PRICE else Ticker.MINIMUM_OTC_PRICE,
            low=tup.Low if tup.Low > Ticker.MINIMUM_OTC_PRICE else Ticker.MINIMUM_OTC_PRICE,
            close=tup.Close if tup.Close > Ticker.MINIMUM_OTC_PRICE else Ticker.MINIMUM_OTC_PRICE,
            open=tup.Open if tup.Open > Ticker.MINIMUM_OTC_PRICE else Ticker.MINIMUM_OTC_PRICE,
            volume=tup.Volume,
        )
        ticker.ticker_history.append(ticker_history)
    ticker.date_last_updated = dt.date.today()


def create_ticker_history(
    ticker: Ticker, yfinance_ticker: yfinance.Ticker, date_entered: dt.date
) -> Ticker:
    if date_entered > ticker.date_last_updated:
        logger.info(
            f"Populating YF historical data into ticker history of {ticker} since {date_entered}"
        )
        assert ticker.symbol == yfinance_ticker.ticker, (
            f"Ticker symbol and yfinance ticker not equal. "
            f"Ticker: ${ticker} YF: ${yfinance_ticker.ticker}"
        )
        yf_history: pd.DataFrame = yfinance_ticker.history(
            start=date_entered - dt.timedelta(days=10)
        )
        assert not yf_history.empty, f"No history on YFinance for {ticker}"
        populate_ticker_history_from_yf(ticker, yf_history)

    return ticker


def create_default_ticker_history(
    ticker: Ticker, yfinance_ticker: yfinance.Ticker
) -> Ticker:
    logger.info(f"Creating default history for {ticker}")
    assert ticker.symbol == yfinance_ticker.ticker, (
        f"Ticker symbol and yfinance ticker not equal. "
        f"Ticker: ${ticker} YF: ${yfinance_ticker.ticker}"
    )
    yf_history: pd.DataFrame = yfinance_ticker.history(period="6y")
    populate_ticker_history_from_yf(ticker, yf_history)

    return ticker


def create_ticker_if_new_from_symbol_and_df(
    dbsess: Session,
    symbol: str,
    yf_dataframe: pd.DataFrame,
    existing_db_tickers_dict: Dict[str, Ticker],
) -> Ticker:
    ticker_obj = existing_db_tickers_dict.get(symbol)
    if ticker_obj is None:
        logger.info(f"Creating ticker: ${symbol}")
        ticker_obj = Ticker(symbol=symbol)
        dbsess.add(ticker_obj)

    failed_to_fetch = (
        yf_dataframe.Close.isnull().all() and yf_dataframe.Open.isnull().all()
    )
    if failed_to_fetch:
        logger.warning(f"No Open/Close data for ${ticker_obj.symbol}.")
        ticker_obj.register_data_fetch_fail()
        return ticker_obj
    if not yf_dataframe.empty:
        ticker_obj.add_df_to_history(yf_dataframe)

    return ticker_obj


def create_ticker_if_new(
    dbsess: Session,
    yf_ticker: yfinance.Ticker,
    db_tickers: Dict[str, Ticker],
    db_commit: bool = True,
) -> Ticker:
    ticker_obj: Ticker = db_tickers.get(yf_ticker.ticker, None)
    if ticker_obj is None:
        logger.info(f"Creating ticker: ${yf_ticker.ticker}")
        assert not yf_ticker.history().empty, f"Ticker has no data on YFinance."
        ticker_obj = Ticker(
            symbol=yf_ticker.ticker, company_name=yf_ticker.info.get("longName")
        )
        ticker_obj = create_default_ticker_history(ticker_obj, yf_ticker)
        dbsess.add(ticker_obj)
        if db_commit:
            dbsess.commit()
        db_tickers.update({ticker_obj.symbol: ticker_obj})

    return ticker_obj


def create_furu_from_twitter_user(dbsess: Session, user) -> Furu:
    logger.info(f"Creating (or fetching) Furu from Twitter User: @{user.screen_name}")
    furu: Optional[Furu] = (
        dbsess.query(Furu).filter(Furu.handle == user.screen_name).one_or_none()
    )
    if furu is not None:
        logger.info(f"Fetched {furu}")
        return furu
    furu = Furu(handle=user.screen_name)
    if user.furu_tweets:
        add_new_tweets_to_furu_tweets(furu, user.furu_tweets)
    dbsess.add(furu)
    dbsess.commit()
    logger.info(f"Created {furu}")

    return furu


def create_or_get_furu_position(
    dbsess: Session,
    furu: Furu,
    yf_ticker: yfinance.Ticker,
    first_mention_date: dt.date,
    db_tickers: Dict[str, Ticker],
    db_commit: bool = True,
) -> FuruTicker:
    ticker = create_ticker_if_new(dbsess, yf_ticker, db_tickers, db_commit)
    history = get_ticker_object_history_at_after_date(ticker, first_mention_date)
    furu_position: Optional[
        FuruTicker
    ] = furu.get_furu_position_by_ticker_and_entry_date(ticker, history.date)
    if furu_position is not None:
        logger.info(f"Fetching @{furu.handle} position in {furu_position}")
        return furu_position
    logger.info(f"Creating position in ${yf_ticker.ticker} for {furu}")
    if history is None:
        create_default_ticker_history(ticker, yf_ticker)
        history = get_ticker_object_history_at_after_date(ticker, first_mention_date)
    assert (
        history is not None
    ), f"Can not create a position in a ticker with no historical data."
    furu_position = FuruTicker(
        furu=furu,
        ticker=ticker,
        date_entered=history.date,
        price_entered=history.get_mid_price_point(),
    )
    dbsess.add(furu_position)

    return furu_position


def close_furu_position(
    furu_position: FuruTicker, yf_ticker: yfinance.Ticker, position_close_date: dt.date
) -> FuruTicker:
    history = get_ticker_object_history_at_after_date(
        furu_position.ticker, position_close_date, yf_ticker=yf_ticker
    )
    assert (
        history is not None
    ), f"Can not close a position in a ticker with no historical data."
    if history.date < furu_position.date_entered:
        position_close_date += dt.timedelta(days=7)
        history = get_ticker_object_history_at_after_date(
            furu_position.ticker, position_close_date
        )
    assert (
        history.date >= furu_position.date_entered
    ), f"Can not close a position with a date in the past."
    populate_ticker_history_from_yf(
        furu_position.ticker,
        yf_ticker.history(start=furu_position.date_entered.isoformat()),
    )
    furu_position.close_position(history)

    return furu_position


def add_new_tweets_to_furu_tweets(furu: Furu, twitter_user_tweets: list) -> list:
    logger.info(
        f"Determining new tweets from {len(twitter_user_tweets)} tweets for {furu}"
    )
    if furu.furu_tweets:
        existing_furu_tweets: List[FuruTweet] = furu.furu_tweets
        existing_tweet_ids = [
            tweet.id
            for furu_tweet in existing_furu_tweets
            for tweet in furu_tweet.tweets
        ]
        new_tweet_ids = [
            tweet.id
            for tweet in twitter_user_tweets
            if tweet.id not in existing_tweet_ids
        ]
        new_tweets = [
            tweet for tweet in twitter_user_tweets if tweet.id in new_tweet_ids
        ]
    else:
        new_tweets = twitter_user_tweets

    if new_tweets:
        furu_tweet = FuruTweet(furu_id=furu.id, tweets=new_tweets)
        furu_tweet.tweets_max_date = furu_tweet.tweets[0].created_at.date()
        furu_tweet.tweets_min_date = furu_tweet.tweets[-1].created_at.date()
        furu_tweet.tweets_min_id = furu_tweet.tweets[-1].id
        furu_tweet.tweets_max_id = furu_tweet.tweets[0].id
        furu.furu_tweets.append(furu_tweet)
        logger.info(f"Saved tweets in {furu_tweet}")

    return new_tweets


def create_furu_from_handle(dbsess: Session, handle: str) -> Furu:
    logger.info(f"Creating (or fetching) Furu from handle: @{handle}")
    furu: Optional[Furu] = (
        dbsess.query(Furu).filter(Furu.handle == handle).one_or_none()
    )
    if furu is not None:
        return furu
    furu = Furu(handle=handle)
    dbsess.add(furu)
    dbsess.commit()

    return furu


def get_ticker_object_history_at_after_date(
    ticker: Ticker, date: dt.date, yf_ticker: yfinance.Ticker = None
) -> Optional[TickerHistory]:
    histories = [
        th
        for th in ticker.ticker_history
        if date <= th.date <= date + dt.timedelta(days=MAX_COUNT)
    ]
    for h in histories:
        return h
    yf_ticker = yf_ticker or yfinance.Ticker(ticker.symbol)
    create_ticker_history(ticker, yf_ticker, date)
    histories = [
        th
        for th in ticker.ticker_history
        if date <= th.date <= date + dt.timedelta(days=MAX_COUNT)
    ]
    for h in histories:
        return h
    raise AssertionError(f"No 10-day Ticker History found on {date} for {ticker}")


def create_or_get_raw_furu_position_by_symbol(
    furu, alpha_ticker, first_mention_date
) -> FuruTicker:
    entry_date = get_nearest_business_day_in_future(first_mention_date)
    furu_position: Optional[
        FuruTicker
    ] = furu.get_furu_position_by_symbol_and_entry_date(alpha_ticker, entry_date)
    if furu_position is not None:
        logger.info(f"Fetched existing position {furu_position}")
        return furu_position
    logger.info(f"Creating position in ${alpha_ticker} for {furu}")
    furu_position = FuruTicker(ticker_symbol=alpha_ticker, date_entered=entry_date)
    furu.positions.append(furu_position)
    return furu_position


def close_raw_furu_position_by_symbol(furu_position, closing_date):
    exit_date = get_nearest_business_day_in_future(closing_date)
    assert furu_position.date_entered <= exit_date, (
        f"Can not close a raw position in the past. "
        f"entry={furu_position.date_entered} exit={exit_date}"
    )
    furu_position.close_raw_position(closing_date)
    return furu_position


def create_furu_positions_entries_exits_from_tweets(
    furu: Furu, cash_ticker: str, tweets_for_ticker: list
) -> Furu:
    logger.info(
        f"Creating raw positions in {cash_ticker} using {len(tweets_for_ticker)} tweets for {furu}"
    )
    alpha_ticker = cash_ticker[1:]
    cash_ticker_tweet_dates = sorted(
        [tweet.created_at.date() for tweet in tweets_for_ticker]
    )
    furu_position = create_or_get_raw_furu_position_by_symbol(
        furu, alpha_ticker, cash_ticker_tweet_dates[0]
    )
    furu_position.date_last_mentioned = cash_ticker_tweet_dates[0]
    i = 1
    while cash_ticker_tweet_dates[i:]:
        diff_with_last_tweet = (
            cash_ticker_tweet_dates[i] - furu_position.date_last_mentioned
        ).days
        if diff_with_last_tweet > Furu.DAYS_SILENCE_FOR_POSITION_EXIT:
            closing_date = furu_position.date_last_mentioned + dt.timedelta(
                days=Furu.DAYS_TAKEN_TO_EXIT_POSITION
            )
            close_raw_furu_position_by_symbol(furu_position, closing_date)
            furu_position = create_or_get_raw_furu_position_by_symbol(
                furu, alpha_ticker, cash_ticker_tweet_dates[i]
            )
        furu_position.date_last_mentioned = cash_ticker_tweet_dates[i]
        i += 1
    diff_with_today = (dt.date.today() - furu_position.date_last_mentioned).days
    if diff_with_today > Furu.DAYS_SILENCE_FOR_POSITION_EXIT:
        closing_date = furu_position.date_last_mentioned + dt.timedelta(
            days=Furu.DAYS_TAKEN_TO_EXIT_POSITION
        )
        close_raw_furu_position_by_symbol(furu_position, closing_date)

    return furu


def save_and_return_tweets_for_analysis(furu: Furu, new_furu_tweets: list) -> list:
    new_tweets = add_new_tweets_to_furu_tweets(furu, new_furu_tweets)
    return new_tweets


def set_exit_dates_for_furu_unmentioned_positions(furu: Furu) -> bool:
    silenced_open_positions = [
        p
        for p in furu.positions
        if p.date_closed is None
        and (dt.date.today() - p.date_last_mentioned).days
        > Furu.DAYS_SILENCE_FOR_POSITION_EXIT
    ]
    has_closed_silenced_position = False
    for pos in silenced_open_positions:
        logger.info(
            f"Raw closing {len(silenced_open_positions)} silenced FURU positions for {furu}."
        )
        try:
            higher_date = max(pos.date_last_mentioned, pos.date_entered)
            closing_date = higher_date + dt.timedelta(
                days=Furu.DAYS_TAKEN_TO_EXIT_POSITION
            )
            close_raw_furu_position_by_symbol(pos, closing_date)
            has_closed_silenced_position = True
        except Exception as ex:
            logger.exception(f"Failed to close raw position {pos}. Reason: {ex}")

    return has_closed_silenced_position


def create_raw_furu_positions_with_new_tweets(furu: Furu) -> bool:
    """Adds FuruTickers with no underlying Ticker objects / invoking yfinance API"""
    if not furu.has_new_furu_tweets:
        logger.warning(
            f"Skipping updating positions as no new FT tweets found for {furu}"
        )
        return False

    logger.info(f"Raw updating positions with new FT tweets for {furu}")
    new_tweets = furu.get_new_furu_tweets()
    furu_cash_tickers = {
        word.upper()
        for tweet in new_tweets
        for word in tweet.text.split()
        if word.startswith("$") and word[1:].isalpha()
    }
    has_created_positions = False
    for cash_ticker in furu_cash_tickers:
        cash_ticker_tweets = [
            tweet
            for tweet in new_tweets
            if cash_ticker in tweet.text.upper().split()
            or cash_ticker[1:] in tweet.text.upper().split()
        ]
        if cash_ticker_tweets:
            try:
                create_furu_positions_entries_exits_from_tweets(
                    furu, cash_ticker, cash_ticker_tweets
                )
                has_created_positions = True
            except Exception as ex:
                logger.exception(
                    f"Failed to assess positions in {cash_ticker}. Reason: {ex}"
                )

    has_closed_silenced = set_exit_dates_for_furu_unmentioned_positions(furu)

    return has_created_positions or has_closed_silenced


def add_ticker_and_prices_to_positions(
    ticker_obj: Ticker, relevant_positions: List[FuruTicker]
) -> List[FuruTicker]:
    logger.info(
        f"Assigning prices for {len(relevant_positions)} raw positions in {ticker_obj}"
    )
    for position in relevant_positions:
        position.ticker = ticker_obj
        if position.is_not_in_future:
            try:
                entry_history = ticker_obj.get_history_at_after_date(
                    position.date_entered
                )
                if position.date_entered != entry_history.date:
                    position.date_entered = entry_history.date
                position.price_entered = entry_history.get_mid_price_point()

                if position.date_closed is not None:
                    exit_history = ticker_obj.get_history_at_after_date(
                        position.date_closed
                    )
                    if position.date_closed != exit_history.date:
                        position.date_closed = exit_history.date
                    position.price_closed = exit_history.get_mid_price_point()
            except TickerHistoryDataError as ex:
                logger.error(
                    f"Ticker History Data Error found for {position}. Error: {ex}"
                )
            except TickerHistoryMissingError as ex:
                logger.error(
                    f"Ticker History Missing Error found for {position}. Error: {ex}"
                )
            except Exception as ex:
                logger.error(
                    f"Failed to assign history pricing to {position}. Reason: {ex}"
                )

    return relevant_positions


def fill_prices_for_raw_furu_positions(session: Session) -> bool:
    """Fills position prices for raw tickers using YFinance"""
    evaluate_error_tickers_reactivation(session)

    price_pending_positions = get_price_pending_positions_without_error_tickers(session)
    if not price_pending_positions:
        return False
    logger.info(f"Gathered {len(price_pending_positions)} positions from DB")

    delete_long_symbol_positions(session, price_pending_positions)

    price_pending_positions_dict = get_positions_dict_from_positions_list(
        price_pending_positions
    )
    logger.info(
        f"Fetching YF price data for {len(price_pending_positions)} raw furu positions"
    )
    min_date = min(p.date_entered for p in price_pending_positions)
    prices = yfinance.download(
        list(price_pending_positions_dict.keys()),
        start=min_date.isoformat(),
        group_by="symbol",
    )

    ticker_objects_list = get_or_create_tickers_from_positions_dict_with_prices_df(
        session, price_pending_positions_dict, prices
    )

    fill_position_prices_from_df_serial(session, price_pending_positions_dict, ticker_objects_list)

    return True


def fill_position_prices_from_df_serial(
    session,
    price_pending_positions_dict,
    ticker_objects_list,
    db_commit_batch_size=50,
):
    parallel_data = [
        (
            ticker_obj,
            price_pending_positions_dict.get(ticker_obj.symbol, []),
        )
        for ticker_obj in ticker_objects_list
    ]
    logger.info(
        f"Filling price data for raw positions in {len(price_pending_positions_dict.keys())} tickers"
    )

    i, j = 0, db_commit_batch_size
    while parallel_data[i:]:
        for ticker_obj, relevant_positions in parallel_data[i:j]:
            add_ticker_and_prices_to_positions(ticker_obj, relevant_positions)
        session.commit()
        i, j = j, j + db_commit_batch_size


def get_or_create_tickers_from_positions_dict_with_prices_df(
    session, price_pending_positions_dict, prices_by_symbol_df, commit_batch_size=100
):
    logger.info(
        f"Evaluating whether to create new tickers from {len(price_pending_positions_dict)} symbols"
    )
    existing_db_tickers_dict = {t.symbol: t for t in session.query(Ticker).all()}
    ticker_objects_list = []
    for i, symbol in enumerate(price_pending_positions_dict.keys()):
        ticker_objects_list.append(
            create_ticker_if_new_from_symbol_and_df(
                session, symbol, prices_by_symbol_df[symbol], existing_db_tickers_dict
            )
        )
        if i > 0 and i % commit_batch_size == 0:
            session.commit()
    if price_pending_positions_dict:
        session.commit()

    return ticker_objects_list


def assign_prices_to_positions_from_ticker_obj(
    tuple_data: tuple[Ticker, list[FuruTicker]]
):
    ticker_obj, relevant_positions = tuple_data
    add_ticker_and_prices_to_positions(ticker_obj, relevant_positions)


def get_positions_dict_from_positions_list(price_pending_positions):
    price_pending_positions_dict = {}
    for pos in price_pending_positions:
        if not pos.ticker:
            if pos.ticker_symbol not in price_pending_positions_dict.keys():
                price_pending_positions_dict[pos.alpha_ticker] = []
            price_pending_positions_dict[pos.alpha_ticker].append(pos)
        if pos.ticker:
            if pos.ticker.status == Ticker.Status.ACTIVE:
                if pos.ticker.symbol not in price_pending_positions_dict.keys():
                    price_pending_positions_dict[pos.alpha_ticker] = []
                price_pending_positions_dict[pos.alpha_ticker].append(pos)
    return price_pending_positions_dict


def delete_long_symbol_positions(dbsess, price_pending_positions):
    i = 0
    for bad_pos in [p for p in price_pending_positions if len(p.alpha_ticker) > 6]:
        price_pending_positions.remove(bad_pos)
        dbsess.delete(bad_pos)
        i += 1
    dbsess.commit()
    logger.info(f"Removed {i} positions as ticker names larger than 6 characters")


def get_price_pending_positions_without_error_tickers(dbsess) -> List[FuruTicker]:
    # noinspection PyComparisonWithNone
    price_pending_positions: List[FuruTicker] = (
        dbsess.query(FuruTicker)
        .filter(
            (
                (FuruTicker.date_entered != None)
                & (FuruTicker.price_entered == None)
                & (FuruTicker.date_entered < dt.date.today())
            )
            | (
                (FuruTicker.date_closed != None)
                & (FuruTicker.price_closed == None)
                & (FuruTicker.date_closed < dt.date.today())
            )
        )
        .all()
    )
    error_ticker_symbols: List[str] = [
        t.symbol
        for t in dbsess.query(Ticker).filter(Ticker.status == Ticker.Status.ERROR).all()
    ]
    return [
        p for p in price_pending_positions if p.alpha_ticker not in error_ticker_symbols
    ]


def evaluate_error_furus_reactivation(dbsess):
    error_furus: List[Furu] = (
        dbsess.query(Furu).filter(Furu.status == Furu.Status.ERROR).all()
    )
    if error_furus:
        logger.info(f"Evaluating re-activating {len(error_furus)} furus in error")
        reactivated_count = 0
        for furu in error_furus:
            furu.evaluate_status_activation()
            if furu.status == Furu.Status.ACTIVE:
                reactivated_count += 1
        logger.info(f"Reactivated {reactivated_count} furus")
        if reactivated_count:
            dbsess.commit()


def evaluate_error_tickers_reactivation(dbsess):
    error_tickers: List[Ticker] = (
        dbsess.query(Ticker).filter(Ticker.status == Ticker.Status.ERROR).all()
    )
    if error_tickers:
        logger.info(f"Evaluating re-activating {len(error_tickers)} cancelled tickers")
        reactivated_count = 0
        for ticker in error_tickers:
            ticker.evaluate_status_activation()
            if ticker.status == Ticker.Status.ACTIVE:
                reactivated_count += 1
        logger.info(f"Reactivated {reactivated_count} tickers")
        if reactivated_count:
            dbsess.commit()


def update_furu_tweets_and_create_raw_positions(tuple_data: (API, Furu)) -> Furu:
    logger.info(f"Comprehensively raw updating {tuple_data[1]}")
    tweepy_session, furu = tuple_data
    # update their tweets with most recent tweets
    from rankr.actions.calculates import update_furu_with_latest_tweets

    update_furu_with_latest_tweets((tweepy_session, furu))
    # update their positions based on these tweets without invoking the yfinance API also close silenced ones
    has_new_positions = create_raw_furu_positions_with_new_tweets(furu)

    furu.date_last_updated = (
        dt.date.today() if has_new_positions else furu.date_last_updated
    )

    return furu
