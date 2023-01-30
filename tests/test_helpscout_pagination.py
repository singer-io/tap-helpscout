from tap_tester import connections, runner, LOGGER

from base import HelpscoutBaseTest

class PaginationTest(HelpscoutBaseTest):

    def name(self):
        return "tap_helpscout_tests_using_shared_token_chaining"

    def test_name(self):
        LOGGER.info("Pagination Test for tap-helpscout")

    def test_run(self):

        # instantiate connection
        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_refresh_token)

        streams_to_test = self.expected_streams()

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields, select_all_fields=True)

        # Run sync mode
        sync_record_count = self.run_and_verify_sync(conn_id)
        sync_records = runner.get_records_from_target_output()

        # Test by stream
        for stream in streams_to_test:
            with self.subTest(stream=stream):

                record_count = sync_record_count.get(stream, 0)

                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')

                primary_keys = self.expected_primary_keys().get(stream)

                # Verify the sync meets or exceeds the default record count
                # for streams - users, workflows, mailboxes, mailbox_fields and mailbox_folders creating test data is a
                # challenge in helpscout. So we will be excluding the above streams from this assertion
                # Spike created to address this issue : TDL - 16378

                if stream not in ('users','workflows','mailboxes','mailbox_fields','mailbox_folders','happiness_ratings', 'teams'):
                    stream_page_size = self.expected_page_limits()[stream]
                    self.assertLessEqual(stream_page_size, record_count)

                # Verify there are no duplicates accross pages
                records_pks_set = {tuple([message.get('data').get(primary_key)
                                          for primary_key in primary_keys])
                                   for message in sync_messages}
                records_pks_list = [tuple([message.get('data').get(primary_key)
                                           for primary_key in primary_keys])
                                    for message in sync_messages]

                self.assertCountEqual(records_pks_set, records_pks_list, msg=f"We have duplicate records for {stream}")
c