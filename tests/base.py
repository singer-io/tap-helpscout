"""
Test tap combined
"""

import unittest
import os

from tap_tester import menagerie
import tap_tester.runner as runner
import tap_tester.connections as connections


class BaseHelpscoutTest(unittest.TestCase):
    """ Test the tap combined """

    def name(self):
        return "tap_helpscout_combined_test"

    def tap_name(self):
        """The name of the tap"""
        return "tap-helpscout"

    def get_type(self):
        """the expected url route ending"""
        return "platform.helpscout"

    def expected_check_streams(self):
        return {
            'conversations',
            'conversation_threads',
            'customers',
            'mailboxes',
            'mailbox_fields',
            'mailbox_folders',
            'users',
            'workflows'
        }

    def expected_sync_streams(self):
        return {
            'conversations',
            'conversation_threads',
            'customers',
            'mailboxes',
            'mailbox_fields',
            'mailbox_folders',
            'users',
            'workflows'
        }

    def expected_pks(self):
        return {
            'conversations': {"id"},
            'conversation_threads': {"id"},
            'customers': {"id"},
            'mailboxes': {"id"},
            'mailbox_fields': {"id"},
            'mailbox_folders': {"id"},
            'users': {"id"},
            'workflows': {"id"}
        }

    def get_properties(self):
        """Configuration properties required for the tap."""
        return {'start_date': os.getenv('TAP_HELPSCOUT_START_DATE', '2018-01-01 00:00:00')}

    def get_credentials(self):
        """Authentication information for the test account"""
        return {'refresh_token': os.getenv('TAP_HELPSCOUT_REFRESH_TOKEN'),
                'client_secret': os.getenv('TAP_HELPSCOUT_CLIENT_SECRET'),
                'client_id': os.getenv('TAP_HELPSCOUT_CLIENT_ID')}

    def required_environment_variables(self):
        return set(['TAP_HELPSCOUT_REFRESH_TOKEN',
                    'TAP_HELPSCOUT_CLIENT_SECRET',
                    'TAP_HELPSCOUT_CLIENT_ID'])

    def setUp(self):
        missing_envs = [x for x in self.required_environment_variables() if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables, please set {}." .format(missing_envs))
