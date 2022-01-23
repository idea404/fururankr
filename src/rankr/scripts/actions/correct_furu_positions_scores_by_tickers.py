from typing import List

import yfinance as yf
from sqlalchemy.orm import Session
from structlog import get_logger

from rankr.actions.calculates import calculate_furu_performance
from rankr.actions.creates import (
    create_default_ticker_history,
    get_ticker_object_history_at_after_date,
)
from rankr.db import create_db_session_from_cfg
from rankr.db.models import Furu, FuruTicker, Ticker, TickerHistory

logger = get_logger()


def correct_furu_positions_and_scores_from_symbols(
    dbsess: Session, symbols_list: List[str]
) -> List[Furu]:
    # reset_ticker_history_from_symbols(dbsess, symbols_list)
    furus = recalculate_furu_positions_from_symbols(dbsess, symbols_list)
    for furu in furus:
        if furu is None:
            logger.error(f"Found None furu: {furu}")
            continue
        calculate_furu_performance(furu)
    dbsess.commit()

    return list(furus)


def recalculate_furu_positions_from_symbols(dbsess, symbols_list):
    furus = set()
    logger.info(f"Correcting furu positions and scores for {len(symbols_list)} symbols")
    for symbol in symbols_list:
        logger.info(f"Correcting furu positions and scores for {symbol}")
        furu_positions = get_all_furu_positions_by_symbol(dbsess, symbol)
        # TODO -- send this to function
        logger.info(f"Fetched {len(furu_positions)} furu positions")
        for position in furu_positions:
            try:
                entry_history = position.ticker.get_history_at_date(
                    position.date_entered
                ) or get_ticker_object_history_at_after_date(
                    position.ticker, position.date_entered
                )
                position.price_entered = entry_history.get_mid_price_point()
                if position.date_closed is not None:
                    exit_history = position.ticker.get_history_at_date(
                        position.date_closed
                    ) or get_ticker_object_history_at_after_date(
                        position.ticker, position.date_entered
                    )
                    position.price_closed = exit_history.get_mid_price_point()
            except Exception as ex:
                logger.error(
                    f"Failed to correct position data. Removing position. Reason: {ex}"
                )
                dbsess.delete(position)
        # TODO -- until here
        logger.info(
            f"Finished correcting and scoring {len(furu_positions)} furu positions for {symbol}"
        )
        furus.update([p.furu for p in furu_positions])
    logger.info(
        f"Finished correcting furu positions and scores for {len(symbols_list)} symbols"
    )
    return furus


def reset_ticker_history_from_symbols(dbsess, symbols_list):
    logger.info(f"Resetting histories for {len(symbols_list)} tickers")
    for hist_symbol in symbols_list:
        logger.info(f"Resetting histories for {hist_symbol}")
        histories = (
            dbsess.query(TickerHistory)
            .join(Ticker)
            .filter(Ticker.symbol == hist_symbol)
            .all()
        )
        ticker = None
        if histories:
            ticker = histories[0].ticker
            for hist in histories:
                dbsess.delete(hist)
            dbsess.commit()
        ticker = (
            ticker or dbsess.query(Ticker).filter(Ticker.symbol == hist_symbol).first()
        )
        create_default_ticker_history(ticker, yf.Ticker(ticker.symbol))
        dbsess.commit()


def get_all_furu_positions_by_symbol(dbsess, symbol) -> List[FuruTicker]:
    return dbsess.query(FuruTicker).join(Ticker).filter(Ticker.symbol == symbol).all()


if __name__ == "__main__":
    symbols = ["BRTX", "PNPL", "RCIT", "WAYS"]
    correct_furu_positions_and_scores_from_symbols(
        create_db_session_from_cfg(), symbols
    )
