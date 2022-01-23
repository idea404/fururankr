from rankr.actions.calculates import (calculate_furu_performance,
                                      close_furu_unmentioned_positions)
from rankr.db import create_db_session_from_cfg
from rankr.db.models import Furu, FuruTicker, TickerHistory

# dbsess = create_db_session_from_cfg()
# furus = dbsess.query(Furu).all()
# for f in furus:
#     if f.handle == 'tradez_e' or f.handle == 'bwalk76':
#         close_furu_unmentioned_positions(dbsess, f)
