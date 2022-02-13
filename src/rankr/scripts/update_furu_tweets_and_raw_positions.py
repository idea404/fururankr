from rankr.actions import instantiate_api_session_from_cfg
from rankr.actions.calculates import update_tweets_and_raw_positions_multi_threaded_io
from rankr.db import create_db_scoped_session
from rankr.db.models import Furu

if __name__ == "__main__":
    Session = create_db_scoped_session()
    session = Session()
    tweepy = instantiate_api_session_from_cfg()
    furu_list = session.query(Furu).filter(Furu.status == Furu.Status.ACTIVE).all()

    v = input(
        f"Will update {len(furu_list)} furus with new tweets and raw positions. Are you sure? (Y/N)\n"
    )
    if v.upper() == "Y":
        update_tweets_and_raw_positions_multi_threaded_io(
            session=session,
            tweepy_session=tweepy,
            list_of_furus=furu_list,
        )
    else:
        print("Skipped.")

    Session.remove()
