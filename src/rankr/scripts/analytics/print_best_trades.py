import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from rankr.db import create_db_session_from_cfg


def get_best_trades_df(session: Session) -> pd.DataFrame:
    query = text(
        """
        SELECT * FROM (SELECT
            furu.handle,
            ticker.symbol as ticker_symbol,
            furu_ticker.price_closed / furu_ticker.price_entered as investment_return,
            furu_ticker.price_entered,
            furu_ticker.price_closed,
            furu_ticker.date_entered,
            furu_ticker.date_closed
        FROM
            furu_ticker
            JOIN furu ON furu_ticker.furu_id = furu.id
            JOIN ticker on furu_ticker.ticker_id = ticker.id)
        ORDER BY investment_return DESC 
        LIMIT 100
    """
    )
    return pd.read_sql(query, session.bind)


def get_best_trades_print_string(session: Session) -> str:
    df = get_best_trades_df(session)
    df["investment_return"] = df["investment_return"].apply(
        lambda x: "{:,.2%}".format(x)
    )
    return df.head(30).to_string(index=False)


if __name__ == "__main__":
    dbsess = create_db_session_from_cfg()
    print(get_best_trades_print_string(dbsess))
