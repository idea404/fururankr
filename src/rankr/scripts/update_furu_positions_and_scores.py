from rankr.db import create_db_session_from_cfg
from rankr.db.models import Furu
from rankr.scripts.update_furu_tweets_positions_and_scores import \
    update_furu_scores_from_new_tweets

if __name__ == "__main__":
    dbsess = create_db_session_from_cfg(echo=False)
    furus = [
        furu for furu in dbsess.query(Furu).all() if furu.status == Furu.Status.ACTIVE
    ]
    v = input(
        f"Will update positions and scores {len(furus)} FURUs with new FuruTweets. Are you sure? (Y/N) "
    )
    if v.upper() == "Y":
        updated_furus = update_furu_scores_from_new_tweets(dbsess, furus)
        print(f"Updated {len(updated_furus)} FURUs.")
    else:
        print("Update skipped.")
