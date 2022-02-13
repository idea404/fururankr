import dataclasses

from sqlalchemy.orm import Session
from tweepy import API


@dataclasses.dataclass
class SessionConnections:
    session: Session
    tweepy: API
