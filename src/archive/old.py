from typing import List

import yfinance
from sqlalchemy.orm import Session
from tweepy import API

from rankr.actions.calculates import (get_new_tweets_for_handle,
                                      get_twitter_user_cutoff_date, logger,
                                      score_furu_from_tweets,
                                      update_furu_with_latest_tweets)
from rankr.actions.creates import save_and_return_tweets_for_analysis
from rankr.actions.finds import get_furu_mentioned_tickers
from rankr.db.models import Furu


def update_furu_tweets_multi_threaded(
    dbsess, tweepy_session, db_commit_batch_size=150, workers=None
):
    list_of_furus = dbsess.query(Furu).filter(Furu.status == Furu.Status.ACTIVE).all()
    logger.info(f"Updating tweets for {len(list_of_furus)} furus from Twitter")
    import concurrent.futures as cf

    parallel_data = [(tweepy_session, furu) for furu in list_of_furus]
    i, j = 0, db_commit_batch_size
    while parallel_data[i:]:
        with cf.ThreadPoolExecutor(max_workers=workers) as exe:
            exe.map(update_furu_with_latest_tweets, parallel_data[i:j])
        dbsess.commit()
        i, j = j, j + db_commit_batch_size


def update_all_furu_positions_and_scores(
    dbsess: Session, tweepy_session: API, list_of_furus: List[Furu]
) -> List[Furu]:
    logger.info(
        f"Updating scores and positions for {len([f for f in list_of_furus if f.status == 'ACTV'])} FURUs"
    )
    updated_furus = []
    for furu in list_of_furus:
        try:
            cutoff_date = get_twitter_user_cutoff_date(dbsess, furu.handle)
            new_furu_tweets = get_new_tweets_for_handle(
                tweepy_session, furu.handle, cutoff_date
            )
            tweets_for_positions = save_and_return_tweets_for_analysis(
                furu, new_furu_tweets
            )
            furu = score_furu_from_tweets(dbsess, furu, tweets_for_positions)
            updated_furus.append(furu)
        except Exception as ex:
            dbsess.rollback()
            logger.exception(f"Could not update and score {furu}. Reason: {ex}")

    return updated_furus


def fetch_ticker_symbols_from_furus_tweets_and_open_positions(
    list_of_furus: List[Furu],
) -> List[str]:
    all_mentioned_tickers = set()
    for furu in list_of_furus:
        logger.info(
            f"Fetching update ticker symbols from tweets and open positions for {furu}"
        )
        try:
            # 1. get a set of all mentioned tickers in new furu furu_tweets
            mentioned_tickers = get_furu_mentioned_tickers(furu)
            # 2. add to the set the furu open positions
            furu_open_position_ticker_symbols = {
                pos.alpha_ticker for pos in furu.positions if pos.date_closed is None
            }
            mentioned_tickers = mentioned_tickers.union(
                furu_open_position_ticker_symbols
            )
            all_mentioned_tickers = all_mentioned_tickers.union(mentioned_tickers)
        except Exception as ex:
            logger.error(
                f"Failed to fetch update ticker symbols for {furu}. Reason: {ex}"
            )

    return list(all_mentioned_tickers)


def create_tickers_from_furu_tweets(session):
    ft_ticker_symbols = get_new_ticker_symbols_from_furu_tweet(session)
    logger.info(
        f"Creating tickers from furu tweet for {len(ft_ticker_symbols)} ticker symbols"
    )
    for symbol in ft_ticker_symbols:
        logger.info(f"Creating ticker: ${symbol}")
        ticker_obj = Ticker(symbol=symbol)
        session.add(ticker_obj)
    if ft_ticker_symbols:
        session.commit()


def get_new_ticker_symbols_from_furu_tweet(session) -> set:
    logger.info("Fetching new candidate ticker symbols from new Furu Tweets")
    db_ticker_symbols_dict = {t.symbol: t for t in session.query(Ticker).all()}
    ft_ticker_symbols = {
        word.upper()
        for furu in session.query(Furu).filter(Furu.status == Furu.Status.ACTIVE).all()
        for tweet in furu.get_new_furu_tweets()
        for word in tweet.text.split()
        if word.startswith("$")
        and word[1:].isalpha()
        and word not in db_ticker_symbols_dict
    }
    return ft_ticker_symbols


def reset_furu_position_prices_by_ticker_symbol(
    dbsess: Session, ticker_symbol: str
) -> None:
    ticker: Ticker = (
        dbsess.query(Ticker).filter(Ticker.symbol == ticker_symbol).one_or_none()
    )
    assert ticker is not None, f"No Ticker found for symbol: {ticker_symbol}"
    furu_positions: List[FuruTicker] = ticker.positions
    logger.info(f"Resetting position prices for {len(furu_positions)} positions.")
    for position in furu_positions:
        history = get_ticker_object_history_at_after_date(ticker, position.date_entered)
        position.price_entered = history.get_mid_price_point()
        if position.date_closed is not None:
            history = get_ticker_object_history_at_after_date(
                ticker, position.date_closed
            )
            position.price_closed = history.get_mid_price_point()
    logger.info(f"Done. {len(furu_positions)} positions have updated prices.")
    furus = {fp.furu for fp in furu_positions}
    from rankr.actions.calculates import calculate_furu_performance

    for furu in furus:
        calculate_furu_performance(furu)
    logger.info(f"Done. Updated performance for affected furus.")
    dbsess.commit()


def reset_ticker_history_by_ticker_symbol(ticker_obj: Ticker) -> None:
    period = "3y"
    logger.info(f"Resetting (period: {period}) history for ticker ${ticker_obj.symbol}")
    yf_ticker = yfinance.Ticker(ticker_obj.symbol)
    yf_history = yf_ticker.history(period=period)
    ticker = ticker_obj
    ticker.ticker_history.clear()
    for tup in yf_history.itertuples():
        ticker_history = TickerHistory(
            ticker=ticker,
            date=tup.Index.date(),
            high=tup.High,
            low=tup.Low,
            close=tup.Close,
            open=tup.Open,
            volume=tup.Volume,
        )
        ticker.ticker_history.append(ticker_history)
    logger.info(
        f"Done. Reset for period: "
        f"{min(d.date() for d in yf_history.index.tolist())} "
        f"to {max(d.date() for d in yf_history.index.tolist())}"
    )