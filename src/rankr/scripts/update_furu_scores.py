from rankr.actions.calculates import calculate_furu_performance
from rankr.db import create_db_session_from_cfg
from rankr.db.models import Furu

if __name__ == "__main__":
    dbsess = create_db_session_from_cfg(False)
    furus = dbsess.query(Furu).all()
    for furu in furus:
        calculate_furu_performance(furu)
