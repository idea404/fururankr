from rankr.actions.calculates import update_furu_scores_multi_threaded
from rankr.actions.creates import fill_prices_for_raw_furu_positions
from rankr.db import create_db_session_from_cfg

if __name__ == "__main__":
    dbsess = create_db_session_from_cfg(False)
    v = input(
        "Will attempt to update raw pricing for all raw positions in DB. Are you sure?\n"
    )
    if v.upper() == "Y":
        fill_prices_for_raw_furu_positions(dbsess)
        update_furu_scores_multi_threaded(dbsess)
    else:
        print("Skipped.")
