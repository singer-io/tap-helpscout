from .abstract import IncrementalStream


class Workflows(IncrementalStream):
    """Class for `workflows` stream."""
    stream = tap_stream_id = "workflows"
    path = "/workflows"
    key_properties = ["id"]
    replication_key = "modified_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("modified_at",)
    data_key = "workflows"
    is_child = False
