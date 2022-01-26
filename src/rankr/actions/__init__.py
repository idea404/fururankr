import os
import pathlib

import tweepy
import yaml
from tweepy import API

PROJECT_PATH = os.path.dirname(os.getcwd())
SETUP_PATH = pathlib.Path(PROJECT_PATH).joinpath("setup")


def load_var_from_config(service: str, var_name: str) -> str:
    with SETUP_PATH.joinpath("keys.yaml").open() as f:
        data_map = yaml.safe_load(f)
    return data_map.get(service, {}).get(var_name)


def instantiate_api_session_from_cfg() -> API:
    bearer = load_var_from_config("twitter", "Bearer")
    auth = tweepy.OAuth2BearerHandler(bearer)
    return tweepy.API(auth, wait_on_rate_limit=True, retry_count=3, retry_delay=30)
