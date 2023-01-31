import json

from tap_tester import connections, runner, LOGGER

from base import HelpscoutBaseTest


class AutomaticFieldsTest(HelpscoutBaseTest):

    def name(self):
        return "tap_helpscout_tests_using_shared_token_chaining"

    def test_name(self):
        LOGGER.info("Automatic Field Test for tap-helpscout")

    def test_run(self):
        """
        Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods
        Verify that only the automatic fields are sent to the target.
        Verify that all replicated records have unique primary key values.
        """
        # instantiate connection
        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_refresh_token)

        streams_to_test = self.expected_streams()

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
                expected_primary_keys = self.expected_primary_keys()
                expected_keys = self.expected_automatic_fields().get(stream)

                # collect actual values
                stream_messages = all_messages.get(stream)
                record_messages_keys = [set(message['data'].keys())
                                        for message in stream_messages['messages']
                                        if message['action'] == 'upsert']

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # automatically Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                # Get records
                records = [message.get("data") for message in
                           stream_messages.get('messages', [])
                           if message.get('action') == 'upsert']

                # Remove duplicate records
                records_pks_list = [
                    tuple(message.get(pk) for pk in expected_primary_keys[stream])
                    for message in [json.loads(t) for t in {json.dumps(d) for d in records}]]

                # Remove duplicate primary keys
                records_pks_set = set(records_pks_list)

                # Verify there are no duplicate records
                self.assertEqual(len(records), len(records_pks_set),
                                 msg=f"{stream} contains duplicate records")

                # Verify defined primary key is unique
                self.assertEqual(len(records_pks_set), len(records_pks_list),
                                 msg=f"{expected_primary_keys} are not unique primary keys for {stream} stream.")
