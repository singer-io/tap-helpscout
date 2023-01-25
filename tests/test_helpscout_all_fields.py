from tap_tester import menagerie, connections, runner

from base import HelpscoutBaseTest

class AllFieldsTest(HelpscoutBaseTest):

    def name(self):
        return "tap_helpscout_tests_using_shared_token_chaining"

    def test_name(self):
        print("Pagination Test for tap-helpscout")

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

        # Verify no unexpected streams were replicated
        synced_stream_names = set(sync_records.keys())
        self.assertSetEqual(streams_to_test, synced_stream_names)

        # get all fields metadata after performing table and field selection
        catalog_all_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            catalog_all_fields[stream_name] = set(fields_from_field_level_md)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected automatic fields
                expected_automatic_fields = self.expected_automatic_fields().get(stream)

                # expected all fields
                expected_all_fields = catalog_all_fields[stream]

                # collect actual fields
                messages = sync_records.get(stream)
                actual_all_fields = set()  # aggregate across all records
                all_record_fields = [set(message['data'].keys())
                                     for message in messages['messages']
                                     if message['action'] == 'upsert']
                for fields in all_record_fields:
                    actual_all_fields.update(fields)

                # verify that we get some records for each stream
                self.assertGreater(sync_record_count.get(stream), 0)

                # verify all fields for each stream were replicated
                self.assertGreater(len(expected_all_fields), len(expected_automatic_fields))
                self.assertTrue(expected_automatic_fields.issubset(expected_all_fields), msg=f'automatic fields not part of all fields')
                self.assertSetEqual(expected_all_fields, actual_all_fields)
