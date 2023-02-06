"""Test tap discovery mode and metadata."""
import re

from base import HelpscoutBaseTest
from tap_tester import LOGGER, connections, menagerie


class DiscoveryTest(HelpscoutBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_helpscout_tests_using_shared_token_chaining"

    def test_name(self):
        LOGGER.info("Discovery Test for tap-helpscout")

    def test_run(self):
        """Testing that discovery creates the appropriate catalog with valid
        metadata.

        - Verify number of actual streams discovered match expected.
        - Verify the stream names discovered were what we expect.
        - Verify stream names follow naming convention streams should only have lowercase alphas and
         underscores.
        - Verify there is only 1 top level breadcrumb.
        - Verify replication key(s).
        - Verify primary key(s).
        - Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL.
        - Verify the actual replication matches our expected replication method.
        - Verify that primary, replication and foreign keys are given the inclusion of automatic.
        - Verify that all other fields have inclusion of available metadata.
        """

        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_refresh_token)

        streams_to_test = self.expected_streams()

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        LOGGER.info("Established Connection to Helpscout")

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores

        found_catalog_names = {c["tap_stream_id"] for c in found_catalogs}
        self.assertTrue(
            all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
            msg="One or more streams don't follow standard naming",
        )

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Verify the catalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_foreign_keys = self.expected_foreign_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = self.expected_automatic_fields()[stream]
                expected_replication_method = self.expected_replication_method()[stream]

                if stream == "conversation_threads":
                    expected_automatic_fields -= expected_foreign_keys

                # collecting actual values
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_primary_keys = set(
                    stream_properties[0].get("metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])
                )
                actual_replication_keys = set(
                    stream_properties[0].get("metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])
                )
                actual_replication_method = (
                    stream_properties[0].get("metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                )
                actual_automatic_fields = {
                    item.get("breadcrumb", ["properties", None])[1]
                    for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                }

                ##########################################################################
                # metadata assertions
                ##########################################################################

                # verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(
                    len(stream_properties) == 1,
                    msg=f"There is NOT only one top level breadcrumb for {stream}"
                    + f"\nstream_properties | {stream_properties}",
                )

                # verify primary key(s)
                self.assertSetEqual(
                    expected_primary_keys,
                    actual_primary_keys,
                    msg=f"expected primary keys is {expected_primary_keys} "
                    f"but actual primary keys is {actual_primary_keys}",
                )

                # verify replication method
                self.assertEqual(
                    expected_replication_method,
                    actual_replication_method,
                    msg=f"expected replication method is {expected_replication_method}"
                    f" but actual replication method is {actual_replication_method}",
                )

                # Verify replication key is present for any stream with replication
                # method = INCREMENTAL
                if actual_replication_method == "INCREMENTAL":
                    # verify replication key(s)
                    self.assertEqual(
                        expected_replication_keys,
                        actual_replication_keys,
                        msg=f"expected replication key is {expected_replication_keys}"
                        f" but actual replication key is {actual_replication_keys}",
                    )
                else:
                    self.assertEqual(actual_replication_keys, set())

                # verify the stream is given the inclusion of available
                self.assertEqual(catalog["metadata"]["inclusion"], "available", msg=f"{stream} cannot be selected")

                # verify the primary, replication keys are given the inclusions of automatic
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                # verify all other fields are given inclusion of available
                self.assertTrue(
                    all(
                        {
                            item.get("metadata").get("inclusion") == "available"
                            for item in metadata
                            if item.get("breadcrumb", []) != []
                            and item.get("breadcrumb", ["properties", None])[1] not in actual_automatic_fields
                        }
                    ),
                    msg="Not all non key properties are set to available in metadata",
                )
