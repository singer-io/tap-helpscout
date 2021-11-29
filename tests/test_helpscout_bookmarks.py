from tap_tester import menagerie, connections, runner

from base import HelpscoutBaseTest


class BookmarksTest(HelpscoutBaseTest):

    def name(self):
        return "tap_helpscout_tests_using_shared_token_chaining"

    def test_name(self):
        print("Bookmarks Test for tap-helpscout")

    def test_run(self):

        #self.should_fail_fast()

        # instantiate connection
        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_refresh_token)

        streams_to_test = self.expected_streams()

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields, select_all_fields=True)

        ########################
        # Run first sync
        ########################

        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        #######################
        # Update State between Syncs
        #######################

        new_state = {'bookmarks': dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)

        for stream, new_state in simulated_states.items():
            new_state['bookmarks'][stream] = new_state
        menagerie.set_state(conn_id, new_state)

        #######################
        # Run Second sync
        #######################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)


        ########################
        # Test by Stream
        ########################

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                
