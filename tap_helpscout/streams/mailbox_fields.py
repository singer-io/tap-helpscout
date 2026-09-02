from .abstract import IncrementalStream


class MailBoxFields(IncrementalStream):
    """Class for `mailbox_fields` stream."""
    stream = tap_stream_id = "mailbox_fields"
    path = "/mailboxes/{}/fields"
    key_properties = ["id"]
    replication_key = "mailboxes_updated_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("mailboxes_updated_at",)
    data_key = "fields"
    is_child = True
    parent = "mailboxes"
    parent_id_field = "mailbox_id"
    parent_replication_key = "updated_at"
