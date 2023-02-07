from .abstract import IncrementalStream


class MailBoxFolders(IncrementalStream):
    """Class for `mailbox_folders` stream."""

    stream = tap_stream_id = "mailbox_folders"
    path = "/mailboxes/{}/folders"
    key_properties = ["id"]
    replication_key = "updated_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("updated_at",)
    data_key = "folders"
    is_child = True
    parent = "mailbox"
