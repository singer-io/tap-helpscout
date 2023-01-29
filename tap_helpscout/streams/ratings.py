from abc import ABC

from .abstract import FullStream


class Ratings(FullStream, ABC):
    """Class for `ratings` stream"""
    stream = tap_stream_id = "ratings"
    path = "/reports/happiness/ratings"
    key_properties = ["thread_id", "conversation_id", "rating_created_at"]
    data_key = "results"
    is_child = False