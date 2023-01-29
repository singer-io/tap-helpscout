from .conversations import Conversations
from .conversation_threads import ConversationThreads
from .customers import Customers
from .mailboxes import MailBoxes
from .mailbox_fields import MailBoxFields
from .mailbox_folders import MailBoxFolders
from .users import Users
from .workflows import Workflows
from .ratings import Ratings
from .teams import Teams


STREAMS = {
    Conversations.tap_stream_id: Conversations,
    ConversationThreads.tap_stream_id: ConversationThreads,
    Customers.tap_stream_id: Customers,
    MailBoxes.tap_stream_id: MailBoxes,
    MailBoxFields.tap_stream_id: MailBoxFields,
    MailBoxFolders.tap_stream_id: MailBoxFolders,
    Users.tap_stream_id: Users,
    Workflows.tap_stream_id: Workflows,
    Ratings.tap_stream_id: Ratings,
    Teams.tap_stream_id: Teams
}


