import datetime as dt
from typing import List

from sqlalchemy.orm import Session
from structlog import get_logger
from tweepy import API

from rankr.actions.calculates import (
    get_furu_tweet_validation_cutoff_date,
    get_new_tweets_for_handle,
)
from rankr.actions.creates import (
    create_furu_from_twitter_user,
    save_and_return_tweets_for_analysis,
)
from rankr.db.models import Furu

logger = get_logger()


def validate_twitter_user(dbsess: Session, tweepy_session: API, twitter_user):
    logger.info(f"Validating Twitter User: @{twitter_user.screen_name}")
    furu_age_in_months = (dt.datetime.now() - twitter_user.created_at).days / 30.5
    is_old_enough = furu_age_in_months > Furu.MIN_MONTHS_OLD
    db_furu = (
        dbsess.query(Furu).filter(Furu.handle == twitter_user.screen_name).one_or_none()
    )
    furu_in_db = db_furu is not None

    if not furu_in_db and not is_old_enough:
        logger.debug(
            f"Twitter User too young with join date: {twitter_user.created_at.date()}"
        )
        return None

    cutoff_date = get_furu_tweet_validation_cutoff_date(db_furu)
    furu_tweets = get_new_tweets_for_handle(
        tweepy_session, twitter_user.screen_name, cutoff_date
    )

    if not furu_in_db:
        is_active_enough = (
            Furu.MIN_TWEETS_PER_MONTH * furu_age_in_months
            <= len(furu_tweets)
            <= Furu.MAX_TWEETS_PER_MONTH * furu_age_in_months
        )
        if not is_active_enough:
            logger.debug(
                f"Twitter User does not meet activity requirements with {len(furu_tweets)} "
                f"tweets in {furu_age_in_months} months"
            )
            return None

        furu_stocks_mentioned = {
            word
            for tweet in furu_tweets
            for word in tweet.text.split()
            if word.startswith("$") and word[1:].isalpha()
        }
        is_varied_enough = (
            Furu.MIN_TICKERS_PER_MONTH * furu_age_in_months
            <= len(furu_stocks_mentioned)
            <= Furu.MAX_TICKERS_PER_MONTH * furu_age_in_months
        )
        if not is_varied_enough:
            logger.debug(
                f"Twitter User does not meet stock variety requirements with {len(furu_stocks_mentioned)} "
                f"stocks in {furu_age_in_months} months"
            )
            return None

    twitter_user.furu_tweets = furu_tweets

    return twitter_user


def validate_score_create_furus_from_twitter_users(
    dbsess: Session, tweepy_session: API, twitter_users: List
) -> List[Furu]:
    logger.info(
        f"Validating, creating and scoring {len(twitter_users)} FURUs from Twitter Users"
    )
    furus = []
    for twitter_user in twitter_users:
        valid_twitter_user = None
        try:
            valid_twitter_user = validate_twitter_user(
                dbsess, tweepy_session, twitter_user
            )
        except Exception as ex:
            logger.error(
                f"Could not validate candidate Twitter FURU User: {twitter_user}. Reason: {ex}"
            )
        if valid_twitter_user is not None:
            try:
                furu = create_furu_from_twitter_user(dbsess, valid_twitter_user)
                furus.append(furu)
            except Exception as ex:
                logger.exception(
                    f"Could not create and save tweets for FURU "
                    f"from Twitter User: @{valid_twitter_user.screen_name}. Reason: {ex}"
                )

    return furus
