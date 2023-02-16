from .abstract import FullStream


class TeamMembers(FullStream):
    """Class for `team_members` stream"""
    stream = tap_stream_id = "team_members"
    path = "/teams/{}/members"
    key_properties = ["team_id", "user_id"]
    data_key = "users"
    is_child = True
    parent = "team"
