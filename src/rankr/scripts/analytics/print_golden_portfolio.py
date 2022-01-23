import datetime as dt

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from rankr.db import create_db_session_from_cfg
from rankr.db.models import Furu


def golden_furu_position_count_score(frame_row: pd.Series) -> float:
    max_furu_concentration = frame_row.max_golden_traders
    return frame_row.furu_count / max_furu_concentration


def ecosystem_saturation_score(frame_row: pd.Series) -> float:
    crowd_size = frame_row.total_trader_count
    max_size = (2 / 3) * frame_row.total_traders_tracked
    if crowd_size > max_size:
        return 0.0
    score = crowd_size / max_size
    return 1.0 - score


def last_mention_score(frame_row_dates: str) -> float:
    last_mentions = [
        dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        for date_str in frame_row_dates.split()
    ]
    total_mentions = len(last_mentions)
    recency_count = 0
    for mention in last_mentions:
        if mention > dt.date.today() - dt.timedelta(days=12):
            recency_count += 1
    return recency_count / total_mentions


def entry_score(frame_row_dates: str) -> float:
    entry_dates = [
        dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        for date_str in frame_row_dates.split()
    ]
    total_entries = len(entry_dates)
    score = 0
    for entry in entry_dates:
        if entry > dt.date.today() - dt.timedelta(days=3):
            score += 1
        elif entry > dt.date.today() - dt.timedelta(days=6):
            score += 2 / 3
        elif entry > dt.date.today() - dt.timedelta(days=9):
            score += 1 / 3
    return score / total_entries


def get_all_open_ticker_saturation(dbsess: Session) -> pd.DataFrame:
    query = text(
        """
        SELECT
            t.symbol as symbol,
            COUNT(ft.furu_id) as total_trader_count
        FROM furu_ticker ft
        JOIN ticker t on t.id = ft.ticker_id
        WHERE ft.date_closed is null
        GROUP BY t.symbol
        ORDER BY COUNT(ft.furu_id) DESC
    """
    )
    return pd.read_sql(query, dbsess.bind)


def get_raw_golden_portfolio(dbsess: Session) -> pd.DataFrame:
    query = text(
        """
        SELECT
           t.symbol, 
           count(ft.furu_id) AS furu_count,
           GROUP_CONCAT(f.handle, ' ') as handles,
           GROUP_CONCAT(ft.date_entered, ' ') as entry_dates,
           GROUP_CONCAT(ft.date_last_mentioned, ' ') as last_mentions,
           MIN(ft.date_entered) as earliest_entry,
           MAX(ft.date_entered) as lastest_entry,
           MIN(ft.price_entered) as least_price_paid,
           MAX(ft.price_entered) as max_price_paid,
           AVG(f.accuracy) as accuracy,
           AVG(f.average_profit) as return,
           AVG(f.average_holding_period_days) as holding_period
        FROM furu_ticker ft
        JOIN furu f ON ft.furu_id = f.id
        JOIN ticker t on ft.ticker_id = t.id
        WHERE f.accuracy > 0.55 
            AND f.performance_score > 0.3 
            AND f.total_trades_measured > 30 
            AND f.average_holding_period_days > 12 
            AND ft.date_closed IS NULL
        GROUP BY t.symbol;
    """
    )
    frame = pd.read_sql(query, dbsess.bind)

    return frame


def add_scores_to_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """
    This function needs the DF to have the following cols:
    max_golden_traders, furu_count, total_trader_count, total_traders_tracked, last_mentions
    """
    frame["golden_furu_pos_count_score"] = frame.apply(
        lambda x: golden_furu_position_count_score(x), axis=1
    )
    frame["ecosystem_saturation_score"] = frame.apply(
        lambda x: ecosystem_saturation_score(x), axis=1
    )
    frame["last_mention_score"] = frame.last_mentions.apply(last_mention_score)
    frame["entry_score"] = frame.entry_dates.apply(entry_score)
    frame["golden_rank"] = (
        (frame.golden_furu_pos_count_score * (5 / 10))
        + (frame.ecosystem_saturation_score * (2 / 10))
        + (frame.last_mention_score * (2 / 10))
        + (frame.entry_score * (1 / 10))
    )
    return frame


def get_golden_portfolio(dbsess: Session) -> pd.DataFrame:
    raw_frame = get_raw_golden_portfolio(dbsess)

    raw_frame["total_traders_tracked"] = len(dbsess.query(Furu).all())
    raw_frame["max_golden_traders"] = max(raw_frame.furu_count)

    sat_frame = get_all_open_ticker_saturation(dbsess)

    merged = pd.merge(raw_frame, sat_frame, on="symbol", how="left")
    merged = add_scores_to_frame(merged)

    merged.drop(columns=["max_golden_traders", "total_traders_tracked"], inplace=True)
    merged.sort_values(by=["golden_rank"], ascending=False, inplace=True)
    merged["position"] = [x for x in range(1, len(merged) + 1)]
    return merged


if __name__ == "__main__":
    dbsess = create_db_session_from_cfg(echo=False)
    golden_folio = get_golden_portfolio(dbsess)
    # golden_folio.to_excel(f'outputs/golden_portfolio.xlsx')

    print(
        golden_folio.head(30).to_string(
            columns=["position", "symbol", "golden_rank"], index=False
        )
    )
