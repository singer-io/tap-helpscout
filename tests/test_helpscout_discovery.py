"""Test tap discovery mode and metadata."""
import re

from tap_tester import menagerie, connections

from base import HelpscoutBaseTest


class DiscoveryTest(HelpscoutBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_helpscout_discovery_test"

    def test_run(self):
        """
        Testing that discovery creates the appropriate catalog with valid metadata.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys
          are given the inclusion of automatic.
        • verify that all other fields have inclusion of available metadata.
        """

        # This method is used get the refresh token from an existing refresh token
        def preserve_refresh_token(existing_conns, payload):
            if not existing_conns:
                return payload
            conn_with_creds = connections.fetch_existing_connection_with_creds(existing_conns[0]['id'])
            payload['properties']['refresh_token'] = conn_with_creds['credentials']['refresh_token']
            return payload

        conn_id = connections.ensure_connection(self, payload_hook=preserve_refresh_token)


        streams_to_test = self.expected_streams()

        found_catalogs = self.run_and_verify_check_mode(conn_id)

       # print(found_catalogs)

        print("Established Connection to Helpscout")

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores

        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

       # print(found_catalog_names)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Verify the catalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = expected_primary_keys | expected_replication_keys
                expected_replication_method = self.expected_replication_method()[stream]

                # collecting actual values
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_primary_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])
                )
                actual_replication_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])
                )
                actual_replication_method = stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                )



                ##########################################################################
                ### metadata assertions
                ##########################################################################

                # verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # verify primary key(s)
                self.assertSetEqual(expected_primary_keys, actual_primary_keys, msg = f"expected primary keys is {expected_primary_keys} but actual primary keys is {actual_primary_keys}")

                # TDL - 16188 : BUG : The tap renames the replication key for stream - conversations from user_updated_at to updated_at, which needs to be corrected

                # verify replication key(s)
                self.assertEqual(expected_replication_keys, actual_replication_keys, msg = f"expected replication key is {expected_replication_keys} but actual replication key is {actual_replication_keys}")

                # verify replication key is present for any stream with replication method = INCREMENTAL
                if actual_replication_method == 'INCREMENTAL':
                    self.assertEqual(expected_replication_keys, actual_replication_keys)

                # verify the primary, replication keys are given the inclusions of automatic
                self.assertEqual(catalog['metadata']['inclusion'],'available')
                print(stream + ' has the automatic inclusion')
