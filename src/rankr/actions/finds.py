import datetime as dt
from typing import List, Set

import holidays
import tweepy
from sqlalchemy.orm import Session
from structlog import get_logger
from tweepy import API, User

from rankr.db.models import Furu

logger = get_logger()


def get_nearest_business_day_in_future(date: dt.date) -> dt.date:
    day = date
    while day.weekday() in holidays.WEEKEND or day in holidays.US():
        day += dt.timedelta(days=1)
    return day


def find_candidate_furus_for_ticker(tweepy_session, ticker_string) -> List[User]:
    logger.info(f"Finding Twitter Users for ${ticker_string}")

    cash_ticker = "$" + ticker_string
    tweets = [
        tweet
        for tweet in tweepy.Cursor(tweepy_session.search_tweets, **{"q": cash_ticker}).items(
            1000
        )
    ]
    min_date: dt.date = min(tweet.created_at for tweet in tweets).date()
    min_id = min(tweet.id for tweet in tweets)
    tweets = [tweet for tweet in tweets if cash_ticker in tweet.text]

    while min_date > Furu.NEW_FURU_SEARCH_IN_PAST_DATE:
        logger.info(f"Fetched {len(tweets)} tweets")
        prev_min_id = min_id
        try:
            possible_tweets = [
                tweet
                for tweet in tweepy.Cursor(
                    tweepy_session.search_tweets,
                    **{"q": cash_ticker, "until": min_date.strftime("%Y-%m-%d")},
                ).items(1000)
            ]
        except tweepy.error.TweepError as err:
            logger.error(
                f"Failed to perform tweet search for {cash_ticker}. Breaking out of search. Reason: {err}"
            )
            possible_tweets = []
        if not possible_tweets:
            break

        min_date: dt.date = min(tweet.created_at for tweet in possible_tweets).date()
        min_id = min(tweet.id for tweet in possible_tweets)
        if prev_min_id == min_id:
            break
        tweets += [tweet for tweet in possible_tweets if cash_ticker in tweet.text]

    unique_twitter_users, unique_twitter_users_names = [], []
    for twitter_furu in [tweet.user for tweet in tweets]:
        if twitter_furu.screen_name not in unique_twitter_users_names:
            unique_twitter_users.append(twitter_furu)
            unique_twitter_users_names.append(twitter_furu.screen_name)

    logger.info(
        f"Found {len(unique_twitter_users)} candidate Twitter FURU User for ${ticker_string}"
    )

    return unique_twitter_users


def find_validate_create_score_furus_for_tickers(
    dbsess: Session, tweepy_session: API, list_of_tickers: List[str]
) -> List[Furu]:
    logger.info(
        f"Searching and validating Twitter Users for ticker list: {list_of_tickers}"
    )
    all_twitter_users = []
    for ticker in list_of_tickers:
        ticker = ticker.strip().upper()
        assert (
            len(ticker) < 7
        ), f"Ticker: {ticker} exceeds maximum length of 6 characters."
        try:
            twitter_users = find_candidate_furus_for_ticker(tweepy_session, ticker)
            all_twitter_users += twitter_users
        except Exception as ex:
            logger.exception(f"Could not find furus for {ticker}. Reason: {ex}")

    all_twitter_users = [
        u
        for u in all_twitter_users
        if u.screen_name not in [f.handle for f in dbsess.query(Furu).all()]
    ]

    from rankr.actions.validates import validate_score_create_furus_from_twitter_users

    return validate_score_create_furus_from_twitter_users(
        dbsess, tweepy_session, all_twitter_users
    )


def find_and_create_furus_for_tickers(
    dbsess: Session, tweepy_session: API, list_of_tickers: List[str]
) -> List[Furu]:
    logger.info(
        f"Finding and creating valid FURUs for {len(list_of_tickers)} ticker(s)"
    )
    return find_validate_create_score_furus_for_tickers(
        dbsess, tweepy_session, list_of_tickers
    )


def get_furu_mentioned_tickers(furu: Furu, cutoff_date: dt.date = None) -> Set[str]:
    cutoff_date = cutoff_date or furu.date_last_updated
    tweets = [
        t
        for ft in furu.furu_tweets
        for t in ft.tweets
        if t.created_at.date() >= cutoff_date
    ]
    mentioned_tickers = {
        word[1:].upper()
        for tweet in tweets
        for word in tweet.text.split()
        if word.startswith("$") and word[1:].isalpha()
    }
    return mentioned_tickers


def get_active_furu_ids(session) -> List[int]:
    return [f.id for f in session.query(Furu).filter(Furu.status == Furu.Status.ACTIVE).all()]
