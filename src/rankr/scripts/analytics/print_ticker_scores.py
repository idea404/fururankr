from typing import List

import pandas as pd
from sqlalchemy.orm import Session

from rankr.db import create_db_session_from_cfg


def get_ticker_scores_table(dbsess: Session, tickers_list: List[str]) -> pd.DataFrame:
    from rankr.scripts.analytics.print_golden_portfolio import \
        get_golden_portfolio

    golden_portfolio = get_golden_portfolio(dbsess)
    return golden_portfolio[golden_portfolio.symbol.isin(tickers_list)]


if __name__ == "__main__":
    dbsess = create_db_session_from_cfg(False)
    tickers = [
        "IFAN",
        "BABL",
        "DMAN",
        "BRGO",
        "IFXY",
        "MSPC",
        "BSSP",
        "UWMC",
        "WKHS",
        "UATG",
        "SDVI",
        "QUAN",
        "XREG",
        "GRSO",
    ]
    frame = get_ticker_scores_table(dbsess, tickers)
    # frame.to_excel(f'outputs/ticker_scores.xlsx')

    print(
        frame.to_string(
            index=False, columns=["symbol", "last_mention_score", "golden_rank"]
        )
    )
