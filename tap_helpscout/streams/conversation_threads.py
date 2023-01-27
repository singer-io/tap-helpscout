from abc import ABC

from .abstract import FullStream


class ConversationThreads(FullStream, ABC):
    """Class for `conversation_threads` stream"""
    stream = tap_stream_id = "conversation_threads"
    path = "/conversations/{}/threads"
    key_properties = ["id"]
    data_key = "threads"
    is_child = True
