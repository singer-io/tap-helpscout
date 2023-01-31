from abc import ABC

from .abstract import FullStream


class TeamUsers(FullStream, ABC):
    """Class for `team_users` stream"""
    stream = tap_stream_id = "team_users"
    path = "/teams/{}/members"
    key_properties = ["team_id", "user_id"]
    data_key = "users"
    is_child = True
    parent = "team"
