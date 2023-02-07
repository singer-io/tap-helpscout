from .abstract import IncrementalStream


class Conversations(IncrementalStream):
    """Class for `conversations` stream."""
    stream = tap_stream_id = "conversations"
    path = "/conversations"
    key_properties = ["id"]
    replication_key = "updated_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("updated_at",)
    replication_query_field = "modifiedSince"
    data_key = "conversations"
    params = {"status": "all", "sortField": "modifiedAt", "sortOrder": "asc"}
    child_streams = ["conversation_threads"]
    is_child = False
