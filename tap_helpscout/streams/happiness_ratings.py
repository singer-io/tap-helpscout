from abc import ABC

from .abstract import FullStream


class HappinessRatings(FullStream, ABC):
    """Class for `ratings` stream"""
    stream = tap_stream_id = "happiness_ratings"
    path = "/reports/happiness/ratings"
    key_properties = ["thread_id", "conversation_id", "rating_created_at"]
    data_key = "results"
    is_child = False