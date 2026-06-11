from .abstract import FullStream


class ConversationThreads(FullStream):
    """Class for `conversation_threads` stream."""
    stream = tap_stream_id = "conversation_threads"
    path = "/conversations/{}/threads"
    key_properties = ["id"]
    data_key = "threads"
    is_child = True
    parent = "conversations"
    parent_id_field = "conversation_id"
