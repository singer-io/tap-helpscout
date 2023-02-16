from .conversations import Conversations
from .conversation_threads import ConversationThreads
from .customers import Customers
from .happiness_ratings_report import HappinessRatingsReport
from .mailboxes import MailBoxes
from .mailbox_fields import MailBoxFields
from .mailbox_folders import MailBoxFolders
from .teams import Teams
from .team_members import TeamMembers
from .users import Users
from .workflows import Workflows


STREAMS = {
    Conversations.tap_stream_id: Conversations,
    ConversationThreads.tap_stream_id: ConversationThreads,
    Customers.tap_stream_id: Customers,
    HappinessRatingsReport.tap_stream_id: HappinessRatingsReport,
    MailBoxes.tap_stream_id: MailBoxes,
    MailBoxFields.tap_stream_id: MailBoxFields,
    MailBoxFolders.tap_stream_id: MailBoxFolders,
    Teams.tap_stream_id: Teams,
    TeamMembers.tap_stream_id: TeamMembers,
    Users.tap_stream_id: Users,
    Workflows.tap_stream_id: Workflows
}
