"""
Test tap combined
"""

import unittest
import os

from tap_tester import menagerie
import tap_tester.runner as runner
import tap_tester.connections as connections

from base import BaseHelpscoutTest


class HelpscoutCombinedTest(BaseHelpscoutTest):
    """ Test the tap combined """

    def name(self):
        return "tap_helpscout_combined_test"

    def test_run(self):

        def preserve_refresh_token(existing_conns, payload):
            if not existing_conns:
                return payload
            conn_with_creds = connections.fetch_existing_connection_with_creds(existing_conns[0]['id'])
            payload['properties']['refresh_token'] = conn_with_creds['credentials']['refresh_token']
            return payload

        conn_id = connections.ensure_connection(self, payload_hook=preserve_refresh_token)

        # Run the tap in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify the check's exit status
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify that there are catalogs found
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset(found_catalog_names)
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")
        #
        # # Select some catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for catalog in our_catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema, [], [])

        # # Verify that all streams sync at least one row for initial sync
        # # This test is also verifying access token expiration handling. If test fails with
        # # authentication error, refresh token was not replaced after expiring.
        menagerie.set_state(conn_id, {})
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(),
                                                                   self.expected_pks())
        zero_count_streams = {k for k, v in record_count_by_stream.items() if v == 0}
        self.assertFalse(zero_count_streams,
                         msg="The following streams did not sync any rows {}".format(zero_count_streams))

        # # Verify that all streams sync only one row for incremental sync
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(),
                                                                   self.expected_pks())
        # Exclude streams in which multiple rows may exist for a bookmark value
        error_incremental_streams = {k for k, v in record_count_by_stream.items() if
                                     v > 1 and k not in {'mailbox_fields'}}
        self.assertFalse(error_incremental_streams,
                         msg="The following streams synced more than 1 row {}".format(error_incremental_streams))

        # # Verify that bookmark values are correct after incremental sync
        current_state = menagerie.get_state(conn_id)
        conversations_bookmark = current_state['bookmarks']['conversations']

        # NB: We use a greater than or equal to in case data is added
        # after our expected value
        self.assertTrue(conversations_bookmark >= '2019-06-21T20:36:54Z',
                        msg=("The bookmark value does not match the expected result: " +
                             "(Actual {} not >= Expected 2019-06-21T20:36:54Z)".format(conversations_bookmark)))
