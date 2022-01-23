import datetime as dt
from typing import List

import yfinance as yf
from structlog import get_logger

from rankr.actions.calculates import calculate_furu_performance
from rankr.actions.creates import (
    get_ticker_object_history_at_after_date,
    populate_ticker_history_from_yf,
)
from rankr.db import create_db_session_from_cfg
from rankr.db.models import Furu, FuruTicker, Ticker

dbsess = create_db_session_from_cfg(False)
furus: List[Furu] = dbsess.query(Furu).all()
TICKERS = [t for t in dbsess.query(Ticker).all()]

logger = get_logger()

for f in furus:
    if f.id > 1282:
        try:
            logger.info(f"Resetting {f}")
            all_furu_tweets = [t for ft in f.furu_tweets for t in ft.tweets]
            assert (
                all_furu_tweets
            ), f"Can not reset positions for FURU as no tweets found: {f}"
            for pos in f.positions:
                dbsess.delete(pos)

            # Get all cash tickers and filter for those in DB
            furu_cash_tickers = {
                word.upper()
                for tweet in all_furu_tweets
                for word in tweet.text.split()
                if word.startswith("$") and word[1:].isalpha()
            }
            valid_cash_ticker_symbols = [
                t for t in TICKERS if t.symbol in [s[1:] for s in furu_cash_tickers]
            ]
            for ticker in valid_cash_ticker_symbols:
                cash_ticker_tweets = [
                    tweet
                    for tweet in all_furu_tweets
                    if ticker.symbol in tweet.text.upper().split()
                    or "$" + ticker.symbol in tweet.text.upper().split()
                ]

                logger.info(
                    f"Evaluating positions in ${ticker.symbol} using {len(cash_ticker_tweets)} tweets for {f}"
                )
                cash_ticker_tweet_dates = sorted(
                    [tweet.created_at.date() for tweet in cash_ticker_tweets]
                )

                # TODO create position
                history = get_ticker_object_history_at_after_date(
                    ticker, cash_ticker_tweet_dates[0]
                )
                if history is None:
                    yf_ticker = yf.Ticker(ticker.symbol)
                    yf_history = yf_ticker.history(period="5y")
                    yf_dates = [d.date() for d in yf_history.index.tolist()]
                    if not min(yf_dates) < cash_ticker_tweet_dates[0] < max(yf_dates):
                        logger.warning(
                            f"Skipping no data: {ticker}. "
                            f"Length DF: {len(yf_history)}. "
                            f"{min(yf_dates)} < {cash_ticker_tweet_dates[0]} < {max(yf_dates)}"
                        )
                        continue
                    populate_ticker_history_from_yf(ticker, yf_history)
                    dbsess.commit()
                history = get_ticker_object_history_at_after_date(
                    ticker, cash_ticker_tweet_dates[0]
                )
                furu_position = FuruTicker(
                    furu=f,
                    ticker=ticker,
                    date_entered=history.date,
                    price_entered=history.get_mid_price_point(),
                )
                dbsess.add(furu_position)
                logger.info(f"Created position: {furu_position}")
                furu_position.date_last_mentioned = cash_ticker_tweet_dates[0]

                i = 1
                while cash_ticker_tweet_dates[i:]:
                    diff_with_last_tweet = (
                        cash_ticker_tweet_dates[i] - furu_position.date_last_mentioned
                    ).days
                    if diff_with_last_tweet > Furu.DAYS_SILENCE_FOR_POSITION_EXIT:
                        # TODO close position
                        closing_date = furu_position.date_last_mentioned + dt.timedelta(
                            days=Furu.DAYS_TAKEN_TO_EXIT_POSITION
                        )
                        history = get_ticker_object_history_at_after_date(
                            ticker, closing_date
                        )
                        if history.date < furu_position.date_entered:
                            closing_date += dt.timedelta(days=7)
                        history = get_ticker_object_history_at_after_date(
                            ticker, closing_date
                        )
                        assert (
                            history is not None
                        ), f"Can not close a position in a ticker with no historical data."
                        furu_position.date_closed = history.date
                        furu_position.price_closed = history.get_mid_price_point()
                        logger.info(f"Closed position: {furu_position}")

                        # TODO create position
                        history = get_ticker_object_history_at_after_date(
                            ticker, cash_ticker_tweet_dates[i]
                        )
                        if history is None:
                            yf_ticker = yf.Ticker(ticker.symbol)
                            yf_history = yf_ticker.history(period="5y")
                            if (
                                not (
                                    min_date := min(
                                        [d.date() for d in yf_history.index.tolist()]
                                    )
                                )
                                < cash_ticker_tweet_dates[0]
                                < (
                                    max_date := max(
                                        [d.date() for d in yf_history.index.tolist()]
                                    )
                                )
                            ):
                                logger.error(
                                    f"BAGHH! no data: {ticker}. "
                                    f"Length DF: {len(yf_history)}. "
                                    f"{min_date.isoformat()} < "
                                    f"{cash_ticker_tweet_dates[0]} < {max_date.isoformat()}"
                                )
                            populate_ticker_history_from_yf(ticker, yf_history)
                            dbsess.commit()
                        history = get_ticker_object_history_at_after_date(
                            ticker, cash_ticker_tweet_dates[i]
                        )
                        furu_position = FuruTicker(
                            furu=f,
                            ticker=ticker,
                            date_entered=history.date,
                            price_entered=history.get_mid_price_point(),
                        )
                        dbsess.add(furu_position)
                        logger.info(f"Created position: {furu_position}")

                    furu_position.date_last_mentioned = cash_ticker_tweet_dates[i]
                    i += 1

                diff_with_today = (
                    dt.date.today() - furu_position.date_last_mentioned
                ).days
                if diff_with_today > Furu.DAYS_SILENCE_FOR_POSITION_EXIT:
                    # TODO close position
                    closing_date = furu_position.date_last_mentioned + dt.timedelta(
                        days=Furu.DAYS_TAKEN_TO_EXIT_POSITION
                    )
                    history = get_ticker_object_history_at_after_date(
                        ticker, closing_date
                    )
                    if history.date < furu_position.date_entered:
                        closing_date += dt.timedelta(days=7)
                    history = get_ticker_object_history_at_after_date(
                        ticker, closing_date
                    )
                    assert (
                        history is not None
                    ), f"Can not close a position in a ticker with no historical data."
                    furu_position.date_closed = history.date
                    furu_position.price_closed = history.get_mid_price_point()
                    logger.info(f"Closed position: {furu_position}")

            silenced_open_positions = [
                p
                for p in f.positions
                if p.date_closed is None
                and (dt.date.today() - p.date_last_mentioned).days
                > Furu.DAYS_SILENCE_FOR_POSITION_EXIT
            ]
            for pos in silenced_open_positions:
                closing_date = pos.date_last_mentioned + dt.timedelta(
                    days=Furu.DAYS_TAKEN_TO_EXIT_POSITION
                )
                history = get_ticker_object_history_at_after_date(
                    pos.ticker, closing_date
                )
                if history.date < pos.date_entered:
                    closing_date += dt.timedelta(days=7)
                history = get_ticker_object_history_at_after_date(
                    pos.ticker, closing_date
                )
                assert (
                    history is not None
                ), f"Can not close a position in a ticker with no historical data."
                pos.date_closed = history.date
                pos.price_closed = history.get_mid_price_point()
                logger.info(f"Closed position: {pos}")

            calculate_furu_performance(f)

            f.date_last_updated = dt.date.today()
            dbsess.commit()
        except Exception as ex:
            dbsess.rollback()
            logger.exception(f"Failed to reset FURU positions. Reason: {ex}")
