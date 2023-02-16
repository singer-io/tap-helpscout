from .abstract import FullStream


class HappinessRatingsReport(FullStream):
    """Class for `happiness_ratings_report` stream"""
    stream = tap_stream_id = "happiness_ratings_report"
    path = "/reports/happiness/ratings"
    key_properties = ["rating_customer_id", "conversation_id", "rating_created_at"]
    data_key = "results"
    is_child = False