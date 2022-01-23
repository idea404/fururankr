from rankr.actions import instantiate_api_session_from_cfg
from rankr.actions.calculates import add_and_score_furu_from_handle
from rankr.db import create_db_session_from_cfg

if __name__ == "__main__":
    session = create_db_session_from_cfg(echo=False)
    api = instantiate_api_session_from_cfg()
    handles = [""]
    for handle in handles:
        add_and_score_furu_from_handle(session, api, handle)
