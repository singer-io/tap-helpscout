from abc import ABC

from .abstract import IncrementalStream


class MailBoxFolders(IncrementalStream, ABC):
    """Class for `mailbox_folders` stream"""
    stream = tap_stream_id = "mailbox_folders"
    path = "/mailboxes/{}/folders"
    key_properties = "id"
    replication_key = "updated_at"
    replication_key_type = "datetime"
    valid_replication_keys = ("updated_at",)
    data_key = "folders"