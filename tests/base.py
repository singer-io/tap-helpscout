"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
from datetime import timedelta
from datetime import datetime as dt
from tap_tester import connections, menagerie, runner


class HelpscoutBaseTest(unittest.TestCase):

    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """
    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = 400
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT00:00:00+00:00"


    start_date = "2018-01-01T00:00:00Z"


    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-helpscout"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.helpscout"


    def get_properties(self):
        """Configuration properties required for the tap."""
        return {'start_date': os.getenv('TAP_HELPSCOUT_START_DATE', '2018-01-01T00:00:00Z')}

    def get_credentials(self):
        """Authentication information for the test account"""
        return {'refresh_token': os.getenv('TAP_HELPSCOUT_REFRESH_TOKEN'),
                'client_secret': os.getenv('TAP_HELPSCOUT_CLIENT_SECRET'),
                'client_id': os.getenv('TAP_HELPSCOUT_CLIENT_ID')}

    def required_environment_variables(self):
        return set(['TAP_HELPSCOUT_REFRESH_TOKEN',
                    'TAP_HELPSCOUT_CLIENT_SECRET',
                    'TAP_HELPSCOUT_CLIENT_ID'])

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        return {
            "conversations": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"user_updated_at"}
            },
            "conversation_threads": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"created_at"}
            },
            "customers": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"}
            },
            "mailboxes": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"}
            },
            "mailbox_fields": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "mailbox_folders": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"}
            },
            "users": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"}
            },
            "workflows": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"modified_at"}
            }
              }

    def expected_streams(self):
       """A set of expected stream names"""
       return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
       """
       return a dictionary with key of table name
       and value as a set of primary key fields
       """
       return {table: properties.get(self.PRIMARY_KEYS, set())
               for table, properties
               in self.expected_metadata().items()}

    def expected_replication_keys(self):
       """
       return a dictionary with key of table name
       and value as a set of replication key fields
       """
       return {table: properties.get(self.REPLICATION_KEYS, set())
               for table, properties
               in self.expected_metadata().items()}

    def expected_foreign_keys(self):
       """
       return a dictionary with key of table
       and value as a set of foreign key fields
       """
       return {table: properties.get(self.FOREIGN_KEYS, set())
               for table, properties
               in self.expected_metadata().items()}

    def expected_automatic_fields(self):

       auto_fields = {}
       for k,v in self.expected_metadata().items():

          auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) | v.get(self.FOREIGN_KEYS, set())
       return auto_fields

    def expected_replication_method(self):
       """return a dictionary with key of table name and value of replication method"""
       return {table: properties.get(self.REPLICATION_METHOD, None)
               for table, properties
               in self.expected_metadata().items()}

    def setUp(self):
        missing_envs = [x for x in self.required_environment_variables() if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables, please set {}." .format(missing_envs))

    #########################
    #   Helper Methods      #
    #########################

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))

        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs
