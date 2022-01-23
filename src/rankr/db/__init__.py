import os
import pathlib

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy.pool import QueuePool

DB_PATH = pathlib.Path(os.getcwd()).joinpath("rankr").joinpath("db")


def create_db_session_from_cfg(echo: bool = False) -> Session:
    engine = create_engine(
        url=f"sqlite:///{DB_PATH.joinpath('fururankr.db')}",
        connect_args={"check_same_thread": False},
        echo=echo,
    )
    session_maker = sessionmaker(bind=engine)
    return session_maker()


def create_db_scoped_session(echo: bool = False) -> scoped_session:
    """
    Call this function to a class variable as:

    --

    Session = create_db_scoped_session()

    session_1 = Session()

    session_2 = Session()

    ...

    Session.remove()

    --

    Read more:
    https://coderedirect.com/questions/246376/sqlalchemy-proper-session-handling-in-multi-thread-applications
    """
    engine = create_engine(
        url=f"sqlite:///{DB_PATH.joinpath('fururankr.db')}",
        connect_args={"check_same_thread": False},
        echo=echo,
        poolclass=QueuePool,
    )
    return scoped_session(sessionmaker(bind=engine))
