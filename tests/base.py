"""Setup expectations for test sub classes Run discovery for as a prerequisite
for most tests."""
import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import dateutil.parser
import pytz
from tap_tester import LOGGER, connections, menagerie, runner


class HelpscoutBaseTest(unittest.TestCase):

    """Setup expectations for test sub classes. Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests. Shared
    tap-specific methods (as needed).
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
    BOOKMARK_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    EXPECTED_PAGE_SIZE = "expected-page-size"
    EXPECTED_PARENT_STREAM = "expected-parent-stream"

    start_date = "2018-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-helpscout"

    @staticmethod
    def get_type():
        """the expected url route ending."""
        return "platform.helpscout"

    def get_properties(self, original=True):
        """Configuration properties required for the tap."""
        return_value = {"start_date": os.getenv("TAP_HELPSCOUT_START_DATE", "2018-01-01T00:00:00Z")}

        if not original:
            return_value["start_date"] = self.start_date

        return return_value

    def get_credentials(self):
        """Authentication information for the test account."""
        return {
            "refresh_token": os.getenv("TAP_HELPSCOUT_REFRESH_TOKEN"),
            "client_secret": os.getenv("TAP_HELPSCOUT_CLIENT_SECRET"),
            "client_id": os.getenv("TAP_HELPSCOUT_CLIENT_ID"),
        }

    def required_environment_variables(self):
        return {"TAP_HELPSCOUT_REFRESH_TOKEN", "TAP_HELPSCOUT_CLIENT_SECRET", "TAP_HELPSCOUT_CLIENT_ID"}

    def expected_metadata(self):
        """The expected streams and metadata about the streams."""
        return {
            "conversations": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.EXPECTED_PAGE_SIZE: 25,
            },
            "conversation_threads": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 50,
                self.EXPECTED_PARENT_STREAM: "conversations",
            },
            "customers": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.EXPECTED_PAGE_SIZE: 50,
            },
            "happiness_ratings_report": {
                self.PRIMARY_KEYS: {"rating_customer_id", "conversation_id", "rating_created_at"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 100
            },
            "mailboxes": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.EXPECTED_PAGE_SIZE: 50,
            },
            "mailbox_fields": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 50,
            },
            "mailbox_folders": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.EXPECTED_PAGE_SIZE: 50,
            },
            "teams": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.EXPECTED_PAGE_SIZE: 50
            },
            "team_members": {
                self.PRIMARY_KEYS: {"team_id", "user_id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 50
            },
            "users": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.EXPECTED_PAGE_SIZE: 50,
            },
            "workflows": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"modified_at"},
                self.EXPECTED_PAGE_SIZE: 50,
            },
        }

    def expected_streams(self):
        """A set of expected stream names."""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        """return a dictionary with key of table name and value as a set of
        primary key fields."""
        return {
            table: properties.get(self.PRIMARY_KEYS, set()) for table, properties in self.expected_metadata().items()
        }

    def expected_foreign_keys(self):
        """return a dictionary with key of table name and value as a set of
        foreign key fields."""
        return {
            table: properties.get(self.FOREIGN_KEYS, set()) for table, properties in self.expected_metadata().items()
        }

    def expected_foreign_keys(self):
       """
       return a dictionary with key of table name
       and value as a set of foregin key fields
       """
       return {table: properties.get(self.FOREIGN_KEYS, set())
               for table, properties
               in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """return a dictionary with key of table name and value as a set of
        replication key fields."""
        return {
            table: properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }

    def expected_automatic_fields(self):

        auto_fields = {}
        for k, v in self.expected_metadata().items():

            auto_fields[k] = (
                v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) | v.get(self.FOREIGN_KEYS, set())
            )
        return auto_fields

    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication
        method."""
        return {
            table: properties.get(self.REPLICATION_METHOD, None)
            for table, properties in self.expected_metadata().items()
        }

    def expected_page_limits(self):
        return {
            table: properties.get(self.EXPECTED_PAGE_SIZE, set())
            for table, properties in self.expected_metadata().items()
        }

    def setUp(self):
        missing_envs = [x for x in self.required_environment_variables() if os.getenv(x) is None]
        if missing_envs:
            raise Exception(f"Missing environment variables, please set {missing_envs}.")

    @staticmethod
    def parse_date(date_value):
        """Pass in string-formatted-datetime, parse the value, and return it as
        an unformatted datetime object."""
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d",
        }
        for date_format in date_formats:
            try:
                return dt.strptime(date_value, date_format)
            except ValueError:
                continue

        raise NotImplementedError(f"Tests do not account for dates of this format: {date_value}")

    def convert_state_to_utc(self, date_str):
        """Convert a saved bookmark value of the form
        '2020-08-25T13:17:36-07:00' to a string formatted utc datetime, in
        order to compare aginast json formatted datetime values."""
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return dt.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            try:
                date_stripped = dt.strptime(dtime, self.BOOKMARK_DATE_FORMAT)
                return_date = date_stripped + timedelta(days=days)

                return dt.strftime(return_date, self.BOOKMARK_DATE_FORMAT)

            except ValueError:
                return Exception(f"Datetime object is not of the format: {self.START_DATE_FORMAT}")

    @staticmethod
    def preserve_refresh_token(existing_conns, payload):
        """This method is used get the refresh token from an existing refresh
        token."""
        if not existing_conns:
            return payload
        conn_with_creds = connections.fetch_existing_connection_with_creds(existing_conns[0]["id"])
        payload["properties"]["refresh_token"] = conn_with_creds["credentials"]["refresh_token"]
        return payload

    def calculated_states_by_stream(self, current_state):
        # {stream_name: [days, hours, minutes], ...}
        timedelta_by_stream = {stream: [5, 0, 0] for stream in self.expected_streams()}

        stream_to_calculated_state = {stream: "" for stream in current_state["bookmarks"].keys()}
        for stream, state in current_state["bookmarks"].items():

            state_as_datetime = dateutil.parser.parse(state)

            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)

            state_format = "%Y-%m-%dT00:00:00Z"
            calculated_state_formatted = dt.strftime(calculated_state_as_datetime, state_format)

            stream_to_calculated_state[stream] = calculated_state_formatted

        return stream_to_calculated_state

    #########################
    #   Helper Methods      #
    #########################

    def run_and_verify_check_mode(self, conn_id):
        """Run the tap in check mode and verify it succeeds. This should be ran
        prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg=f"unable to locate schemas for connection " f"{conn_id}")

        found_catalog_names = {found_catalog["stream_name"] for found_catalog in found_catalogs}
        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        LOGGER.info("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """Run a sync job and make sure it exited properly.

        Return a dictionary with keys of streams synced and values of
        records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys()
        )
        self.assertGreater(
            sum(sync_record_count.values()), 0, msg=f"failed to replicate any data:" f" {sync_record_count}"
        )
        LOGGER.info(f"total replicated row count: {sum(sync_record_count.values())}")

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs, select_all_fields=True):
        """Perform table and field selection based off of the streams to select
        set and field selection parameters.

        Verify this results in the expected streams selected and all or
        no fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields)

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get("stream_name") for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat["stream_id"])

            # Verify all testable streams are selected
            selected = catalog_entry.get("annotated-schema").get("selected")
            LOGGER.info(f"Validating selection on {cat['stream_name']}: {selected}")
            if cat["stream_name"] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue  # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get("annotated-schema").get("properties").items():
                    field_selected = field_props.get("selected")
                    LOGGER.info(f"\tValidating selection on {cat['stream_name']}.{field}:" f" {field_selected}")
                    self.assertTrue(field_selected, msg="Field not selected.")

            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat["stream_name"])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry["metadata"])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field["breadcrumb"]) > 1
            inclusion_automatic_or_selected = (
                field["metadata"]["selected"] is True or field["metadata"]["inclusion"] == "automatic"
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field["breadcrumb"][1])
        return selected_fields

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams."""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {}).keys()

            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema, [], non_selected_properties)

    def expected_child_streams(self):
        return {
            table: properties.get(self.EXPECTED_PARENT_STREAM, set())
            for table, properties in self.expected_metadata().items()
        }
