from .abstract import IncrementalStream


class TeamMembers(IncrementalStream):
    """Class for `team_members` stream"""
    stream = tap_stream_id = "team_members"
    path = "/teams/{}/members"
    key_properties = ["team_id", "user_id"]
    replication_key = "updated_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("updated_at",)
    data_key = "users"
    is_child = True
    parent = "teams"
    parent_id_field = "team_id"
