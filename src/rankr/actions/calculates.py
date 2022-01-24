import datetime as dt
import time
from typing import Dict, List, Optional, Tuple

import tweepy
import yfinance
from sqlalchemy.orm import Session, scoped_session
from structlog import get_logger
from tweepy import API

from rankr.actions.creates import (
    close_furu_position,
    create_furu_from_handle,
    create_or_get_furu_position,
    fill_prices_for_raw_furu_positions,
    save_and_return_tweets_for_analysis,
    update_furu_tweets_and_create_raw_positions,
)
from rankr.db.models import Furu, FuruTicker, Ticker

logger = get_logger()


def score_furu_on_closed_position(furu: Furu, furu_position: FuruTicker) -> Furu:
    position_return = furu_position.calculate_position_return()
    accuracy = 1 if position_return > 0 else 0
    position_days = (furu_position.date_closed - furu_position.date_entered).days
    if furu.total_trades_measured is None or furu.total_trades_measured == 0:
        furu.total_trades_measured = 1
        furu.accuracy = accuracy
        furu.average_profit = position_return if accuracy == 1 else None
        furu.average_loss = position_return if accuracy == 0 else None
        furu.average_holding_period_days = position_days
    else:
        furu.total_trades_measured += 1
        if accuracy == 1:
            trades_profitable = round(furu.accuracy * furu.total_trades_measured)
            if trades_profitable == 0:
                furu.average_profit = position_return
            else:
                furu.average_profit += (
                    position_return - furu.average_profit
                ) / trades_profitable
        else:
            trades_unprofitable = round(
                (1 - furu.accuracy) * furu.total_trades_measured
            )
            if trades_unprofitable == 0:
                furu.average_loss = position_return
            else:
                furu.average_loss = -(
                    abs(furu.average_loss)
                    + (
                        (abs(position_return) - abs(furu.average_loss))
                        / trades_unprofitable
                    )
                )

        furu.accuracy += (accuracy - furu.accuracy) / furu.total_trades_measured
        furu.average_holding_period_days += (
            position_days - furu.average_holding_period_days
        ) / furu.total_trades_measured

    return furu


def calculate_furu_performance(furu: Furu) -> Furu:
    furu_closed_positions = [
        position for position in furu.positions if position.is_closed
    ]
    if furu_closed_positions:
        logger.info(f"Scoring {furu} on {len(furu_closed_positions)} closed positions.")
        furu.total_trades_measured = None
        for closed_position in furu_closed_positions:
            try:
                score_furu_on_closed_position(furu, closed_position)
            except Exception as ex:
                logger.exception(
                    f"Could not calculate Furu performance for {closed_position}. Reason: {ex}"
                )
        try:
            furu_profit = furu.average_profit if furu.average_profit is not None else 0
            furu_loss = furu.average_loss if furu.average_loss is not None else 0
            furu.expected_return = (furu.accuracy * furu_profit) + (
                (1 - furu.accuracy) * furu_loss
            )
            furu.performance_score = furu.accuracy * furu_profit + (
                (1 - furu.accuracy) * (furu_loss - furu_profit)
            )
        except Exception as ex:
            logger.exception(
                f"Could not calculate exp. return and performance Furu for {furu}. Reason: {ex}"
            )
        logger.info(f"Scored: {furu} on {len(furu_closed_positions)} closed positions.")
    else:
        logger.info(
            f"Not scoring @{furu.handle} ({furu.id}) as no closed positions were found."
        )

    return furu


def close_furu_unmentioned_positions(furu: Furu) -> Furu:
    silenced_open_positions = [
        p
        for p in furu.positions
        if p.date_closed is None
        and (dt.date.today() - p.date_last_mentioned).days
        > Furu.DAYS_SILENCE_FOR_POSITION_EXIT
    ]
    logger.info(
        f"Closing {len(silenced_open_positions)} silenced FURU positions for @{furu.handle}."
    )
    yf_tickers = yfinance.Tickers([pos.alpha_ticker for pos in silenced_open_positions])
    for pos in silenced_open_positions:
        try:
            closing_date = pos.date_last_mentioned + dt.timedelta(
                days=Furu.DAYS_TAKEN_TO_EXIT_POSITION
            )
            yf_ticker = yf_tickers.tickers.get(pos.alpha_ticker)
            assert (
                yf_ticker is not None
            ), f"Failed to retrieve yfinance data for symbol={pos.alpha_ticker}"
            close_furu_position(pos, yf_ticker, closing_date)
        except AssertionError as ex:
            logger.error(f"Failed to close position {pos}. Reason: {ex}")
            furu.positions.remove(pos)
        except Exception as ex:
            logger.exception(f"Failed to close position {pos}. Reason: {ex}")
            furu.positions.remove(pos)

    return furu


def scoped_score_furu_from_tweets(tuple_data: Tuple[scoped_session, Furu]) -> Furu:
    session, furu = tuple_data
    try:
        furu = score_furu_from_tweets(session(), furu, furu.get_new_furu_tweets())
    except KeyError as ex:
        logger.warning(f"Skipped scoring for {furu}. Reason: {ex}")
    except Exception as ex:
        logger.error(f"Failed to score {furu}. Reason: {ex}")

    return furu


def score_furu_from_tweets(
    dbsess: Session, furu: Furu, twitter_user_tweets: list, db_commit: bool = True
) -> Furu:
    logger.info(f"Scoring @{furu.handle} on {len(twitter_user_tweets)} tweets")
    if not twitter_user_tweets:
        raise KeyError(
            f"No tweets associated with Twitter User @{furu.handle}. Can not perform scoring."
        )

    furu_cash_tickers = {
        word
        for tweet in twitter_user_tweets
        for word in tweet.text.split()
        if word.startswith("$") and word[1:].isalpha()
    }
    yfinance_tickers = yfinance.Tickers([ticker[1:] for ticker in furu_cash_tickers])

    db_tickers: Dict[str, Ticker] = {t.symbol: t for t in dbsess.query(Ticker).all()}

    matched_tickers = {
        "$" + symbol: yf_ticker
        for symbol, yf_ticker in yfinance_tickers.tickers.items()
    }
    for cash_ticker, ticker_data in matched_tickers.items():
        cash_ticker_tweets = [
            tweet
            for tweet in twitter_user_tweets
            if cash_ticker in tweet.text.upper().split()
            or cash_ticker[1:] in tweet.text.upper().split()
        ]
        if not cash_ticker_tweets:
            continue
        try:
            create_furu_positions_from_tweets(
                dbsess, furu, ticker_data, cash_ticker_tweets, db_tickers, db_commit
            )
        except (KeyError, ValueError, IndexError):
            logger.warning(
                f"No data in YFinance for {cash_ticker}. Will skip position."
            )
        except AssertionError as ex:
            logger.error(f"Did not to create positions in {cash_ticker}. Reason: {ex}")
        except Exception as ex:
            logger.exception(
                f"Failed to assess positions in {cash_ticker}. Reason: {ex}"
            )

    close_furu_unmentioned_positions(furu)
    calculate_furu_performance(furu)

    furu.date_last_updated = dt.date.today()
    if db_commit:
        dbsess.commit()

    return furu


def create_furu_positions_from_tweets(
    dbsess: Session,
    furu: Furu,
    ticker_data: yfinance.Ticker,
    cash_ticker_tweets: list,
    db_tickers: Dict[str, Ticker],
    db_commit: bool = True,
) -> Furu:
    logger.info(
        f"Evaluating positions in ${ticker_data.ticker} using {len(cash_ticker_tweets)} tweets for {furu}"
    )
    cash_ticker_tweet_dates = sorted(
        [tweet.created_at.date() for tweet in cash_ticker_tweets]
    )
    furu_position = create_or_get_furu_position(
        dbsess, furu, ticker_data, cash_ticker_tweet_dates[0], db_tickers, db_commit
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
            close_furu_position(furu_position, ticker_data, closing_date)
            furu_position = create_or_get_furu_position(
                dbsess,
                furu,
                ticker_data,
                cash_ticker_tweet_dates[i],
                db_tickers,
                db_commit,
            )
        furu_position.date_last_mentioned = cash_ticker_tweet_dates[i]
        i += 1
    diff_with_today = (dt.date.today() - furu_position.date_last_mentioned).days
    if diff_with_today > Furu.DAYS_SILENCE_FOR_POSITION_EXIT:
        closing_date = furu_position.date_last_mentioned + dt.timedelta(
            days=Furu.DAYS_TAKEN_TO_EXIT_POSITION
        )
        close_furu_position(furu_position, ticker_data, closing_date)

    return furu


def get_twitter_user_cutoff_date(dbsess, handle: str) -> dt.date:
    furu: Optional[Furu] = (
        dbsess.query(Furu).filter(Furu.handle == handle).one_or_none()
    )
    return get_furu_cutoff_date(furu)


def get_furu_cutoff_date(furu: Optional[Furu]) -> dt.date:
    return (
        Furu.FETCH_TWEET_HISTORY_CUTOFF_DATE
        if furu is None
        else furu.get_tweets_cutoff_date()
    )


def get_furu_tweet_validation_cutoff_date(furu: Optional[Furu]) -> dt.date:
    return (
        Furu.FETCH_TWEET_VALIDATION_CUTOFF_DATE
        if furu is None
        else furu.get_tweets_cutoff_date()
    )


def get_new_tweets_for_handle(
    tweepy_session: API, handle: str, cutoff_date: dt.date = None
) -> list:
    logger.info(f"Getting new tweets for @{handle} with cutoff date: {cutoff_date}")
    cutoff_date = cutoff_date or Furu.FETCH_TWEET_HISTORY_CUTOFF_DATE
    furu_tweets = [
        tweet
        for tweet in tweepy.Cursor(
            tweepy_session.user_timeline, **{"screen_name": handle}
        ).items(100)
    ]
    min_date = min(tweet.created_at for tweet in furu_tweets).date()
    min_id = min(tweet.id for tweet in furu_tweets)

    while min_date > cutoff_date:
        logger.debug(f"Loaded {len(furu_tweets)} tweets")
        prev_min_id = min_id
        candidate_tweets = [
            tweet
            for tweet in tweepy.Cursor(
                tweepy_session.user_timeline,
                **{"screen_name": handle, "max_id": min_id},
            ).items(1000)
        ]
        if not candidate_tweets:
            break

        min_date = min(tweet.created_at for tweet in candidate_tweets).date()
        min_id = min(tweet.id for tweet in candidate_tweets)
        if prev_min_id == min_id:
            break
        furu_tweets += candidate_tweets
        if len(furu_tweets) > Furu.MAX_TOTAL_TWEETS:
            logger.debug(
                f"Too many tweets with {len(furu_tweets)} tweets fetched so breaking out of loop"
            )
            break

    return furu_tweets


def update_furu_with_latest_tweets_and_score(
    tuple_data: (API, scoped_session, Furu)
) -> Furu:
    tweepy_session, session, furu = tuple_data
    dbsess = session()

    furu = update_furu_with_latest_tweets((tweepy_session, furu))
    dbsess.commit()

    try:
        furu = score_furu_from_tweets(dbsess, furu, furu.get_new_furu_tweets())
    except KeyError as ex:
        logger.warning(f"Skipped scoring for {furu}. Reason: {ex}")
    except Exception as ex:
        logger.error(f"Failed to score {furu}. Reason: {ex}")

    return furu


def update_furu_with_latest_tweets(tuple_data: (API, Furu)) -> Furu:
    tweepy_session, furu = tuple_data
    if not furu.has_new_furu_tweets:
        logger.info(f"Updating latest tweets for {furu}")
        try:
            cutoff_date = furu.get_tweets_cutoff_date()
            new_furu_tweets = get_new_tweets_for_handle(
                tweepy_session, furu.handle, cutoff_date
            )
            save_and_return_tweets_for_analysis(furu, new_furu_tweets)
        except Exception as ex:
            logger.warning(
                f"Found error while getting data for {furu}. Will try again. Reason: {ex}"
            )
            try:
                time.sleep(2)
                cutoff_date = furu.get_tweets_cutoff_date()
                new_furu_tweets = get_new_tweets_for_handle(
                    tweepy_session, furu.handle, cutoff_date
                )
                save_and_return_tweets_for_analysis(furu, new_furu_tweets)
            except Exception as ex:
                logger.error(
                    f"Failed twice while getting data for {furu}. Reason: {ex}"
                )
                furu.register_data_fetch_fail()

    return furu


def add_and_score_furu_from_handle(
    dbsess: Session, twitter_sess: API, handle: str
) -> Furu:
    logger.info(f"Adding FURU to DB: @{handle}")
    furu = None
    try:
        furu = create_furu_from_handle(dbsess, handle)
        cutoff_date = get_twitter_user_cutoff_date(dbsess, furu.handle)
        furu_tweets = get_new_tweets_for_handle(twitter_sess, furu.handle, cutoff_date)
        tweets_for_positions = save_and_return_tweets_for_analysis(furu, furu_tweets)
        furu = score_furu_from_tweets(dbsess, furu, tweets_for_positions)
    except Exception as ex:
        dbsess.rollback()
        logger.exception(
            f"Could not create and score FURU with handle: @{handle}. Reason: {ex}"
        )

    return furu


def update_furu_tweets_positions_scores_multi_threaded(
    tweepy_session: API,
    dbsess: Session,
    list_of_furus: List[Furu],
    db_commit_batch_size=100,
    workers=4,
) -> List[Furu]:
    """Fast-performing function for bulk update of Furu data"""
    update_tweets_and_raw_positions_multi_threaded(
        dbsess, tweepy_session, list_of_furus, db_commit_batch_size, workers
    )
    # gather all positions that need price data and fetch and save it
    fill_prices_for_raw_furu_positions(dbsess)
    # score furus
    update_furu_scores_multi_threaded(dbsess)

    return list_of_furus


def update_furu_scores_multi_threaded(dbsess: Session):
    import concurrent.futures as cf

    furus = dbsess.query(Furu).filter(Furu.status == Furu.Status.ACTIVE).all()
    logger.info(f"Updating scores for {len(furus)} furus closed positions")
    with cf.ThreadPoolExecutor() as exe:
        exe.map(calculate_furu_performance, furus)


def update_tweets_and_raw_positions_multi_threaded(
    dbsess, tweepy_session, list_of_furus, db_commit_batch_size=100, workers=None
):
    logger.info(
        f"Updating tweets and raw positions for {len(list_of_furus)} furus from Twitter"
    )
    import concurrent.futures as cf

    parallel_data = [(tweepy_session, furu) for furu in list_of_furus]
    i, j = 0, db_commit_batch_size
    while parallel_data[i:]:
        with cf.ThreadPoolExecutor(max_workers=workers) as exe:
            exe.map(update_furu_tweets_and_create_raw_positions, parallel_data[i:j])
        dbsess.commit()
        i, j = j, j + db_commit_batch_size
