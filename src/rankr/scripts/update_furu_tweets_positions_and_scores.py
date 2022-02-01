import concurrent.futures as cf
import datetime as dt
from typing import List

from sqlalchemy.orm import Session, scoped_session
from structlog import get_logger
from tweepy import API

import rankr.db
from rankr.actions import instantiate_api_session_from_cfg
from rankr.actions.calculates import (
    scoped_score_furu_from_tweets,
    score_furu_from_tweets,
    update_furu_tweets_positions_scores_multi_threaded,
    update_furu_with_latest_tweets,
    update_furu_with_latest_tweets_and_score,
)
from rankr.db.models import Furu


logger = get_logger()

BATCH_SIZE = 50


def update_furu_scores_from_new_tweets(db_session, list_of_furus):
    logger.info(f"Updating scores from new tweets for {len(list_of_furus)} FURUs")
    i, j = 0, BATCH_SIZE
    while list_of_furus[i:]:
        for furu in list_of_furus[i:j]:
            if furu.has_new_furu_tweets:
                try:
                    score_furu_from_tweets(db_session, furu, furu.get_new_furu_tweets())
                except KeyError as ex:
                    logger.warning(f"Skipped scoring for {furu}. Reason: {ex}")
                except Exception as ex:
                    logger.exception(f"Failed to score {furu}. Reason: {ex}")
            else:
                logger.warning(f"Skipped scoring because of no tweets for {furu}.")
        db_session.commit()
        i = j
        j += BATCH_SIZE

    return list_of_furus


def update_furu_tweets_multi_threaded(
    db_session: Session, tweepy_session: API, list_of_furus: List[Furu]
) -> List[Furu]:
    twitter_parallel_data = [(tweepy_session, furu) for furu in list_of_furus]
    logger.info(f"Updating scores and positions for {len(twitter_parallel_data)} FURUs")
    i, j = 0, BATCH_SIZE
    while twitter_parallel_data[i:]:
        with cf.ThreadPoolExecutor() as exe:
            exe.map(update_furu_with_latest_tweets, twitter_parallel_data[i:j])
        db_session.commit()
        i = j
        j += BATCH_SIZE

    return list_of_furus


def update_furu_scores_from_new_tweets_multi_threaded(
    session_class: scoped_session, list_of_furus: List[Furu], max_workers=10
) -> List[Furu]:
    parallel_data = [(session_class, furu) for furu in list_of_furus]
    with cf.ThreadPoolExecutor(max_workers=max_workers) as exe:
        exe.map(scoped_score_furu_from_tweets, parallel_data)
    return list_of_furus


def update_furu_tweets_and_scores_multi_threaded(
    session_class: scoped_session,
    tweepy_session: API,
    list_of_furus: List[Furu],
    max_workers=10,
) -> List[Furu]:
    parallel_data = [(session_class, tweepy_session, furu) for furu in list_of_furus]
    with cf.ThreadPoolExecutor(max_workers=max_workers) as exe:
        exe.map(update_furu_with_latest_tweets_and_score, parallel_data)
    return list_of_furus


if __name__ == "__main__":
    Session = rankr.db.create_db_scoped_session()
    dbsess = Session()
    api = instantiate_api_session_from_cfg()

    furu_id_list = [
        furu.id
        for furu in dbsess.query(Furu).all()
        if furu.status == Furu.Status.ACTIVE
        and (furu.date_last_updated is None or furu.date_last_updated < dt.date.today())
    ]

    v = input(
        f"Will update tweets and scores {len(furu_id_list)} FURUs. Are you sure? (Y/N)\n"
    )
    if v.upper() == "Y":
        furus = update_furu_tweets_positions_scores_multi_threaded(
            api, Session, furu_id_list, workers=4
        )
        print(f"Updated {len(furus)} FURUs.")
    else:
        print("Update skipped.")

    Session.remove()
