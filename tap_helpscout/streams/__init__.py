from .conversation_threads import ConversationThreads
from .conversations import Conversations
from .customers import Customers
from .mailbox_fields import MailBoxFields
from .mailbox_folders import MailBoxFolders
from .mailboxes import MailBoxes
from .users import Users
from .workflows import Workflows

STREAMS = {
    Conversations.tap_stream_id: Conversations,
    ConversationThreads.tap_stream_id: ConversationThreads,
    Customers.tap_stream_id: Customers,
    MailBoxes.tap_stream_id: MailBoxes,
    MailBoxFields.tap_stream_id: MailBoxFields,
    MailBoxFolders.tap_stream_id: MailBoxFolders,
    Users.tap_stream_id: Users,
    Workflows.tap_stream_id: Workflows,
}
