from .abstract import IncrementalStream


class ConversationThreads(IncrementalStream):
    """Class for `conversation_threads` stream."""
    stream = tap_stream_id = "conversation_threads"
    path = "/conversations/{}/threads"
    key_properties = ["id"]
    replication_key = "created_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("created_at",)
    data_key = "threads"
    is_child = True
    parent = "conversations"
    parent_id_field = "conversation_id"
