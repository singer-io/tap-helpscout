from tap_tester import menagerie,connections,runner
import re

from base import HelpscoutBaseTest

class AutomaticFieldsTest(HelpscoutBaseTest):

    def name(self):
        return "tap-helpscout-shared-tests-token-chaining"

    def test_name(self):
        print("Automatic Field Test for tap-helpscout")

    def test_run(self):
        """
        Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py method
        Verify that only the automatic fields are sent to the target.
        """
        self.should_fail_fast()

        # instantiate connection
        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_refresh_token)

        streams_to_test = self.expected_streams()

        print(streams_to_test)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
             conn_id, test_catalogs_automatic_fields, select_all_fields=False)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        all_messages = runner.get_records_from_target_output()
        
        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected values
                expected_keys = self.expected_automatic_fields().get(stream)

                # workaround for TDL-16245 , remove after bug fix
                expected_keys = expected_keys - self.expected_replication_keys().get(stream)

                # collect actual values
                stream_messages = all_messages.get(stream)
                record_messages_keys = [set(message['data'].keys())
                                        for message in stream_messages['messages']
                                        if  message['action'] == 'upsert']


                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # TDL-16245 : BUG : Replication key for all the streams are not being selected automatically
                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)
