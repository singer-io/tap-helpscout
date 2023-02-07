from .abstract import FullStream


class MailBoxFields(FullStream):
    """Class for `mailbox_fields` stream."""
    stream = tap_stream_id = "mailbox_fields"
    path = "/mailboxes/{}/fields"
    key_properties = ["id"]
    data_key = "fields"
    is_child = True
    parent = "mailbox"
