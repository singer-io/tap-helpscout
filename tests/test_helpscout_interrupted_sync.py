from tap_tester import runner, connections, menagerie, LOGGER
from base import HelpscoutBaseTest
from dateutil import parser as parser


class HelpscoutInterruptedSyncTest(HelpscoutBaseTest):
    """
    Test to verify that if a sync is interrupted, then the next sync will continue
    from the bookmarks and currently syncing stream.
    """

    def name(self):
        return "tap_helpscout_tests_using_shared_token_chaining"

    def test_name(self):
        LOGGER.info("Interrupted Sync test for tap-helpscout")

    def test_run(self):
        """
        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that
                  `currently_syncing` stream.

        Expected State Structure:
            {
                "currently_syncing": "stream_name",
                "bookmarks": {
                    "stream_1": "2010-10-10T10:10:10.100000",
                    "stream_2": "2010-10-10T10:10:10.100000"
                }
            }

        Test Cases:
        - Verify an interrupted sync can resume based on the `currently_syncing` and stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the
          stream level bookmark are replicated on the resuming sync for the interrupted stream.
        - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync.
        """

        self.start_date = self.get_properties()["start_date"]
        start_date_timestamp = self.dt_to_ts(self.start_date)

        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_refresh_token)
        expected_streams = self.expected_streams()

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in expected_streams]

        # Catalog selection
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields, select_all_fields=True)

        # Run a sync job
        self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()

        post_interrupted_sync_state = menagerie.get_state(conn_id)

        # Simulated interrupted state to run 2nd sync
        interrupted_sync_state = {
            "currently_syncing": "customers",
            "bookmarks": {
                "conversations": "2021-12-02T16:47:26Z",
                "customers": "2019-06-20T17:00:00Z"
            }
        }

        # Set state for 2nd sync
        menagerie.set_state(conn_id, interrupted_sync_state)

        # Run sync after interruption
        post_interrupted_sync_record_count_by_stream = self.run_and_verify_sync(conn_id)
        post_interrupted_sync_records = runner.get_records_from_target_output()

        post_interrupted_sync_state = menagerie.get_state(conn_id)
        currently_syncing = post_interrupted_sync_state.get("bookmarks").get("currently_syncing")

        # Checking that the resuming sync resulted in a successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in the state for sync
            self.assertIsNone(currently_syncing,
                              msg="After final sync bookmarks should not contain 'currently_syncing' key.")

            # Verify bookmarks are saved
            self.assertIsNotNone(post_interrupted_sync_state.get("bookmarks"),
                                 msg="After final sync bookmarks should not be empty.")

            # Verify final_state is equal to uninterrupted sync"s state
            self.assertDictEqual(post_interrupted_sync_state, post_interrupted_sync_state,
                                 msg="Final state after interruption should be equal to full sync")

        # Stream level assertions
        for stream in expected_streams:
            with self.subTest(stream=stream):

                replication_key = self.expected_replication_keys()[stream]
                replication_method = self.expected_replication_method()[stream]

                # Gather actual results
                first_sync_stream_records = [message["data"]
                                             for message
                                             in first_sync_records.get(stream, {}).get("messages", [])]

                post_interrupted_sync_stream_records = [message["data"]
                                                        for message
                                                        in post_interrupted_sync_records.get(stream, {}).get("messages", [])]
                
                # Get record counts
                full_sync_record_count = len(first_sync_stream_records)
                interrupted_record_count = len(post_interrupted_sync_stream_records)

                if replication_method == self.INCREMENTAL:
                    # Final bookmark after interrupted sync
                    final_stream_bookmark = post_interrupted_sync_state["bookmarks"].get(stream, None)

                    # Verify final bookmark matched the formatting standards for the resuming sync
                    self.assertIsNotNone(final_stream_bookmark,
                                         msg="Bookmark can not be 'None'.")
                    self.assertIsInstance(final_stream_bookmark, str,
                                          msg="Bookmark format is not as expected.")

                if stream == interrupted_sync_state["currently_syncing"]:
                    # Assign the start date to the interrupted stream
                    interrupted_stream_datetime = self.dt_to_ts(interrupted_sync_state["bookmarks"][stream])
                    primary_key = self.expected_primary_keys()[stream].pop()

                    # Get primary keys of 1st sync records
                    full_records_primary_keys = [x[primary_key] for x in first_sync_stream_records]

                    for record in post_interrupted_sync_stream_records:
                        record_time = self.dt_to_ts(record.get(list(replication_key)[0]))

                        # Verify resuming sync only replicates records with the replication key
                        # values greater or equal to the state for streams that were replicated
                        # during the interrupted sync.
                        self.assertGreaterEqual(record_time, interrupted_stream_datetime)

                        # Verify the interrupted sync replicates the expected record set all
                        # interrupted records are in full records
                        self.assertIn(record[primary_key], full_records_primary_keys,
                                      msg="Incremental table record in interrupted sync not found in full sync")

                    # Record count for all streams of interrupted sync match expectations
                    records_after_interrupted_bookmark = 0
                    for record in first_sync_stream_records:
                        record_time = self.dt_to_ts(record.get(list(replication_key)[0]))
                        if record_time >= interrupted_stream_datetime:
                            records_after_interrupted_bookmark += 1

                    self.assertEqual(records_after_interrupted_bookmark, interrupted_record_count,
                                    msg="Expected {} records in each sync".format(
                                        records_after_interrupted_bookmark))

                else:
                    # Get the date to start 2nd sync for non-interrupted streams
                    synced_stream_bookmark = interrupted_sync_state["bookmarks"].get(stream, None)

                    if synced_stream_bookmark:
                        synced_stream_datetime = self.dt_to_ts(synced_stream_bookmark)
                    else:
                        synced_stream_datetime = start_date_timestamp

                    # BUG: TDL-21675: interrupted sync does not sync already synced streams
                    if stream not in ["conversations", "conversation_threads"]:
                        # Verify we replicated some records for the non-interrupted streams
                        self.assertGreater(interrupted_record_count, 0,
                                        msg="Un-interrupted streams must sync at least 1 record.")

                    if replication_method == self.INCREMENTAL:

                        for record in post_interrupted_sync_stream_records:
                            record_time = self.dt_to_ts(record.get(list(replication_key)[0]))

                            # Verify resuming sync only replicates records with the replication key
                            # values greater or equal to the state for streams that were replicated
                            # during the interrupted sync.
                            self.assertGreaterEqual(record_time, synced_stream_datetime)

                            # Verify resuming sync replicates all records that were found in the full
                            # sync (non-interrupted)
                            self.assertIn(record, first_sync_stream_records,
                                        msg="Unexpected record replicated in resuming sync.")
                    else:
                        # FULL_TABLE stream records should be same
                        self.assertEqual(interrupted_record_count, full_sync_record_count,
                                          msg=f"Record count of streams with {self.FULL_TABLE} replication method must be equal.")
