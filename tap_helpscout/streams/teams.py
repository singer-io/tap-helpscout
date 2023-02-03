from .abstract import IncrementalStream


class Teams(IncrementalStream):
    """Class for `conversations` stream"""
    stream = tap_stream_id = "teams"
    path = "/teams"
    key_properties = ["id"]
    replication_key = "updated_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("updated_at",)
    data_key = "teams"
    child_streams = ["team_members"]
    is_child = False
