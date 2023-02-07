from .abstract import IncrementalStream


class Users(IncrementalStream):
    """Class for `users` stream."""
    stream = tap_stream_id = "users"
    path = "/users"
    key_properties = ["id"]
    replication_key = "updated_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("updated_at",)
    data_key = "users"
    is_child = False
