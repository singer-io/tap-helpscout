from .abstract import IncrementalStream


class MailBoxes(IncrementalStream):
    """Class for `mailboxes` stream."""
    stream = tap_stream_id = "mailboxes"
    path = "/mailboxes"
    key_properties = ["id"]
    replication_key = "updated_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("updated_at",)
    data_key = "mailboxes"
    child_streams = ["mailbox_fields", "mailbox_folders"]
    is_child = False
