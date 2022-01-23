from rankr.actions import instantiate_api_session_from_cfg
from rankr.actions.finds import find_and_create_furus_for_tickers
from rankr.db import create_db_session_from_cfg

if __name__ == "__main__":
    dbsess = create_db_session_from_cfg(echo=False)
    api = instantiate_api_session_from_cfg()
    tickers = input("Please insert tickers separated by spaces:\n").upper().split()
    find_and_create_furus_for_tickers(dbsess, api, tickers)
    dbsess.close()
