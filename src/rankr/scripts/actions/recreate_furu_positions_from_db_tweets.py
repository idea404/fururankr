import concurrent.futures as cf

from sqlalchemy.orm import Session
from structlog import get_logger

from rankr.actions.calculates import calculate_furu_performance
from rankr.actions.creates import (
    create_furu_positions_entries_exits_from_tweets,
    fill_prices_for_raw_furu_positions,
    set_exit_dates_for_furu_unmentioned_positions,
)
from rankr.db import create_db_session_from_cfg
from rankr.db.models import Furu

logger = get_logger()


def recreate_furu_positions_and_scores_from_db(
    dbsess: Session, recreate_furu_positions=True
):
    furus = dbsess.query(Furu).filter(Furu.status == Furu.Status.ACTIVE).all()
    if recreate_furu_positions:
        logger.info(
            f"Recreating furu positions and scores from DB for {len(furus)} furus"
        )
        # create raw positions from db tweets
        _db_commit_batch_size = 50
        i, j = 0, _db_commit_batch_size
        while furus[i:]:
            with cf.ThreadPoolExecutor() as exe:
                exe.map(recreate_furu_raw_positions, furus[i:j])
            dbsess.commit()
            i, j = j, j + _db_commit_batch_size
    # gather all positions that need price data and fetch and save it
    fill_prices_for_raw_furu_positions(dbsess)
    # score furus
    with cf.ThreadPoolExecutor() as exe:
        exe.map(calculate_furu_performance, furus)


def recreate_furu_raw_positions(furu: Furu):
    logger.info(f"Raw updating positions with new FT tweets for {furu}")
    tweets = furu.get_all_furu_tweets()
    furu_cash_tickers = {
        word.upper()
        for tweet in tweets
        for word in tweet.text.split()
        if word.startswith("$") and word[1:].isalpha()
    }
    for cash_ticker in furu_cash_tickers:
        cash_ticker_tweets = [
            tweet
            for tweet in tweets
            if cash_ticker in tweet.text.upper().split()
            or cash_ticker[1:] in tweet.text.upper().split()
        ]
        if cash_ticker_tweets:
            try:
                create_furu_positions_entries_exits_from_tweets(
                    furu, cash_ticker, cash_ticker_tweets
                )
            except Exception as ex:
                logger.exception(
                    f"Failed to assess positions in {cash_ticker}. Reason: {ex}"
                )

    set_exit_dates_for_furu_unmentioned_positions(furu)


if __name__ == "__main__":
    dbsess = create_db_session_from_cfg(False)
    recreate_furu_positions_and_scores_from_db(dbsess)
