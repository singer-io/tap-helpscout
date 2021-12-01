from tap_tester import menagerie, connections, runner

from base import HelpscoutBaseTest


class BookmarksTest(HelpscoutBaseTest):

    def name(self):
        return "tap_helpscout_tests_using_shared_token_chaining"

    def test_name(self):
        print("Bookmarks Test for tap-helpscout")

    def test_run(self):

        # instantiate connection
        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_refresh_token)

        streams_to_test = self.expected_streams()
        # -{"workflows","users","mailbox_fields","mailbox_folders","customers","mailboxes"}

        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

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

        for stream, updated_state in simulated_states.items():
            new_state['bookmarks'][stream] = updated_state
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

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # information required for assetions from sync 1 & 2 based on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in first_sync_records.get(stream, {}).get('messages', [])
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in second_sync_records.get(stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                first_bookmark_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                if stream in self.expected_child_streams().keys():
                    #BUG : TDL-16580 : Child stream not getting the foreign key information from metadata
                    #TODO: if child, check that for every child record the foreign key value
                    # corresponds to a primary key value of a replcated parent record
                    pass

                elif expected_replication_method == self.INCREMENTAL:

                    # collect information specific to incremental streams from sync 1 & 2
                    replication_key = next(iter(expected_replication_keys[stream]))
                    first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                    second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)
                    simulated_bookmark = new_state['bookmarks'][stream]

                    # verify the syncs sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_value)
                    self.assertIsNotNone(second_bookmark_value)

                    # verify the 2nd bookmark is equal to 1st sync bookmark
                    self.assertEqual(first_bookmark_value, second_bookmark_value)

                    for record in first_sync_messages:
                        replication_key_value = record.get(replication_key)
                        # verify 1st sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(replication_key_value, first_bookmark_value_utc, msg="First sync bookmark was set incorrectly, a re                                             cord with a greater replication key value was synced")

                    for record in second_sync_messages:
                        replication_key_value = record.get(replication_key)
                        # verify the 2nd sync replication key value is greater or equal to the 1st sync bookmarks
                        self.assertGreaterEqual(replication_key_value, simulated_bookmark, msg="Second sync records do not respect the previous                                                  bookmark")
                        # verify the 2nd sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(replication_key_value, second_bookmark_value_utc, msg="Second sync bookmark was set incorrectly, a                         record with a greater replication key value was synced")

                    # verify that we get less data in the 2nd sync
                    self.assertLess(second_sync_count, first_sync_count,
                                    msg="Second sync does not have less records, bookmark usage not verified")

                elif expected_replication_method == self.FULL_TABLE:

                    # verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_value)
                    self.assertIsNone(second_bookmark_value)

                    # verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream, expected_replication_method))

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
