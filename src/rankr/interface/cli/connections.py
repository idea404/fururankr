import dataclasses

from sqlalchemy.orm import scoped_session
from tweepy import API


@dataclasses.dataclass
class SessionConnections:
    scoped_session_class: scoped_session
    tweepy: API
