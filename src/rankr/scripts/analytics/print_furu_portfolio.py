import datetime as dt
from typing import List

import pandas as pd
from sqlalchemy.orm import Session

from rankr.db import create_db_session_from_cfg
from rankr.db.models import Furu


def format_days_as_str(days_float: float) -> str:
    rounded = round(days_float)
    return str(rounded) + "d"


def calc_mid_entry_price(frame_row) -> float:
    if frame_row.high is not None and frame_row.low is not None:
        return (frame_row.high + frame_row.low) / 2
    return None


def calc_days_held(frame_row) -> int:
    return (
        dt.date.today()
        - dt.datetime.strptime(frame_row.earliest_entry, "%Y-%m-%d").date()
    ).days


def count_handles(frame_row) -> int:
    row_handles: str = frame_row.handles
    return row_handles.count("@")


def get_open_trades_by_furus(
    dbsess: Session, list_of_furus: List[Furu]
) -> pd.DataFrame:
    if not list_of_furus:
        list_of_furus: List[Furu] = dbsess.query(Furu).all()
    furu_id_string = ",".join(str(furu.id) for furu in list_of_furus)
    query = f"""
        SELECT
           t.symbol, 
           count(ft.furu_id) AS trader_count,
           GROUP_CONCAT(f.handle, ' ') as handles,
           GROUP_CONCAT(ft.date_entered, ' ') as entry_dates,
           MIN(ft.date_entered) as earliest_entry,
           MAX(ft.date_entered) as lastest_entry,
           MIN(ft.price_entered) as least_price_paid,
           MAX(ft.price_entered) as max_price_paid,
           AVG(f.accuracy) as avg_accuracy,
           AVG(f.average_holding_period_days) as avg_holding_period
        FROM furu_ticker ft
        JOIN furu f ON ft.furu_id = f.id
        JOIN ticker t on ft.ticker_id = t.id
        WHERE ft.date_closed IS NULL AND ft.furu_id IN ({furu_id_string})
        GROUP BY t.symbol
    """
    frame_positions = pd.read_sql(sql=query, con=dbsess.bind)

    frame_positions.sort_values(by="earliest_entry", inplace=True, ascending=False)
    frame_positions["days_held"] = frame_positions.apply(
        lambda x: calc_days_held(x), axis=1
    )

    if len(list_of_furus) > 1:
        frame_positions["most_present_traders"] = ["🥇", "🥈", "🥉"] + [
            "" for _ in range(3, len(frame_positions))
        ]

    frame_positions.sort_values(by="earliest_entry", inplace=True, ascending=False)

    return frame_positions


if __name__ == "__main__":
    dbsess = create_db_session_from_cfg(echo=False)

    # furus_in = input("\nPlease type Furu handle(s) (separated by space):\n")
    # furu_list = furus_in.split() if furus_in is not None else ['CobraOTC']
    # furus = dbsess.query(Furu).filter(Furu.handle.in_(furu_list)).all()
    # furus = []
    df = get_open_trades_by_furus(dbsess, None)

    # df.to_excel(f'outputs/{len(furus)}_furus_open_positions.xlsx')

    print(df.to_string(index=True, max_rows=20))
