from .abstract import IncrementalStream


class Customers(IncrementalStream):
    """Class for `customers` stream."""
    stream = tap_stream_id = "customers"
    path = "/customers"
    key_properties = ["id"]
    replication_key = "updated_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("updated_at",)
    replication_query_field = "modifiedSince"
    data_key = "customers"
    params = {"sortField": "modifiedAt", "sortOrder": "asc"}
    is_child = False
