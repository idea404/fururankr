import datetime as dt

import pandas as pd
from sqlalchemy import text

from rankr.db import create_db_session_from_cfg

dbsess = create_db_session_from_cfg(False)


def is_agreed_position(frame_row: pd.Series) -> float:
    dates_entered = frame_row.entry_dates
    dates = sorted([dt.datetime.strptime(date_str, '%Y-%m-%d').date() for date_str in dates_entered.split()])
    if len(dates) > 2:
        date_diffs = [(dates[i] - dates[0]).days for i in range(1, len(dates))]
        counter = 1
        for diff in date_diffs:
            if diff < 3:
                counter += 1
        if counter > 2:
            return 1
    return 0


def calc_returns(frame_row: pd.Series) -> float:
    entry_prices = [float(val) for val in frame_row.entry_prices.split()]
    exit_prices = [float(val) for val in frame_row.exit_prices.split()]
    average = 0.0
    count = 0
    for en, ex in zip(entry_prices, exit_prices):
        count += 1
        pos_return = ((ex / en) - 1)
        average += (pos_return - average) / count
    return average


query = text("""
    SELECT
       t.symbol, 
       count(ft.furu_id) AS furu_count,
       GROUP_CONCAT(f.handle, ' ') as handles,
       GROUP_CONCAT(ft.date_entered, ' ') as entry_dates,
       GROUP_CONCAT(ft.price_entered, ' ') AS entry_prices,
       GROUP_CONCAT(ft.price_closed, ' ') AS exit_prices,
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
    WHERE f.accuracy > 0.7 AND f.average_profit > 1 AND f.total_trades_measured > 30 AND ft.date_closed IS NOT NULL
        AND ft.date_entered > '2019-12-31'
    GROUP BY t.symbol;
""")
frame = pd.read_sql(query, dbsess.bind)


frame['is_agreed_position'] = frame.apply(lambda x: is_agreed_position(x), axis=1)
frame['return'] = frame.apply(lambda x: calc_returns(x), axis=1)

agreed_positions = frame[frame['is_agreed_position'] == 1]
disagreed_positions = frame[frame['is_agreed_position'] == 0]

popular_positions = frame[frame['furu_count'] > 4]
unpopular_positions = frame[frame['furu_count'] <= 4]

print("Agreed:", str(round(agreed_positions['return'].mean() * 100))+'%', f" Positions: {len(agreed_positions)}")
print("Disagreed:", str(round(disagreed_positions['return'].mean() * 100))+'%', f" Positions: {len(disagreed_positions)}")

print("Popular:", str(round(popular_positions['return'].mean() * 100))+'%', f" Positions: {len(popular_positions)}")
print("Unpopular:", str(round(unpopular_positions['return'].mean() * 100))+'%', f" Positions: {len(unpopular_positions)}")
