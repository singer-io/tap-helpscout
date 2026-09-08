"""Unit tests for tap-helpscout sync module."""
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

from tap_helpscout.sync import sync


class TestSync(unittest.TestCase):
    """Test the sync() function."""

    @patch("tap_helpscout.sync.set_currently_syncing")
    @patch("tap_helpscout.sync.write_state")
    @patch("tap_helpscout.sync.write_schema")
    @patch("tap_helpscout.sync.metadata")
    def test_sync_parent_with_selected_child_streams(self, mock_metadata, mock_write_schema, mock_write_state, mock_set_sync):
        """Test sync with parent stream that returns IDs and has selected child streams."""
        mock_client = MagicMock()
        mock_state = {}

        # Setup return value for set_currently_syncing
        mock_set_sync.side_effect = lambda state, stream_id: {**state, "currently_syncing": stream_id}
        mock_metadata.to_map.return_value = {}

        # Create mock stream objects
        mock_parent_stream = MagicMock()
        mock_parent_stream.tap_stream_id = "mailboxes"
        mock_parent_stream.schema.to_dict.return_value = {"type": "object"}
        mock_parent_stream.metadata = []
        mock_parent_stream.replication_key = None

        mock_child_stream = MagicMock()
        mock_child_stream.tap_stream_id = "mailbox_fields"
        mock_child_stream.schema.to_dict.return_value = {"type": "object"}
        mock_child_stream.metadata = []
        mock_child_stream.replication_key = None
        mock_child_stream.is_selected.return_value = True

        # Create mock catalog
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [mock_parent_stream]
        mock_catalog.get_stream.return_value = mock_child_stream

        # Create mock stream classes
        mock_mailboxes_class = MagicMock()
        mock_mailboxes_instance = MagicMock()
        mock_mailboxes_instance.is_child = False
        mock_mailboxes_instance.child_streams = ["mailbox_fields"]
        mock_mailboxes_instance.key_properties = ["id"]
        mock_mailboxes_instance.sync.return_value = {1, 2, 3}  # Returns parent IDs
        mock_mailboxes_class.return_value = mock_mailboxes_instance
        mock_mailboxes_class.is_child = False

        mock_mailbox_fields_class = MagicMock()
        mock_mailbox_fields_instance = MagicMock()
        mock_mailbox_fields_instance.is_child = True
        mock_mailbox_fields_instance.key_properties = ["id"]
        mock_mailbox_fields_class.return_value = mock_mailbox_fields_instance
        mock_mailbox_fields_class.is_child = True

        # Patch STREAMS with our mock classes
        with patch.dict("tap_helpscout.sync.STREAMS", {
            "mailboxes": mock_mailboxes_class,
            "mailbox_fields": mock_mailbox_fields_class,
        }):
            sync(mock_client, mock_catalog, mock_state, "2020-01-01")

        # Verify parent stream sync was called
        mock_mailboxes_instance.sync.assert_called_once()
        # Verify child stream sync was called
        mock_mailbox_fields_instance.sync.assert_called_once()
        # Verify write_schema was called for both
        self.assertEqual(mock_write_schema.call_count, 2)

    @patch("tap_helpscout.sync.set_currently_syncing")
    @patch("tap_helpscout.sync.write_state")
    @patch("tap_helpscout.sync.write_schema")
    @patch("tap_helpscout.sync.metadata")
    def test_sync_parent_child_skipped_if_not_selected(self, mock_metadata, mock_write_schema, mock_write_state, mock_set_sync):
        """Test that child streams are skipped if not selected."""
        mock_client = MagicMock()
        mock_state = {}

        mock_set_sync.side_effect = lambda state, stream_id: {**state, "currently_syncing": stream_id}
        mock_metadata.to_map.return_value = {}

        mock_parent_stream = MagicMock()
        mock_parent_stream.tap_stream_id = "mailboxes"
        mock_parent_stream.schema.to_dict.return_value = {"type": "object"}
        mock_parent_stream.metadata = []
        mock_parent_stream.replication_key = None

        mock_child_stream = MagicMock()
        mock_child_stream.is_selected.return_value = False  # Child NOT selected

        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [mock_parent_stream]
        mock_catalog.get_stream.return_value = mock_child_stream

        mock_mailboxes_instance = MagicMock()
        mock_mailboxes_instance.is_child = False
        mock_mailboxes_instance.child_streams = ["mailbox_fields"]
        mock_mailboxes_instance.key_properties = ["id"]
        mock_mailboxes_instance.sync.return_value = {1, 2}
        mock_mailboxes_class = MagicMock(return_value=mock_mailboxes_instance, is_child=False)

        with patch.dict("tap_helpscout.sync.STREAMS", {"mailboxes": mock_mailboxes_class}):
            sync(mock_client, mock_catalog, mock_state, "2020-01-01")

        # Write schema only called once (parent only)
        self.assertEqual(mock_write_schema.call_count, 1)

    @patch("tap_helpscout.sync.set_currently_syncing")
    @patch("tap_helpscout.sync.write_state")
    @patch("tap_helpscout.sync.write_schema")
    @patch("tap_helpscout.sync.metadata")
    def test_sync_parent_no_ids_skips_child_sync(self, mock_metadata, mock_write_schema, mock_write_state, mock_set_sync):
        """Test that child streams are not synced if parent returns no IDs."""
        mock_client = MagicMock()
        mock_state = {}

        mock_set_sync.side_effect = lambda state, stream_id: {**state, "currently_syncing": stream_id}
        mock_metadata.to_map.return_value = {}

        mock_parent_stream = MagicMock()
        mock_parent_stream.tap_stream_id = "mailboxes"
        mock_parent_stream.schema.to_dict.return_value = {"type": "object"}
        mock_parent_stream.metadata = []
        mock_parent_stream.replication_key = None

        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [mock_parent_stream]

        mock_mailboxes_instance = MagicMock()
        mock_mailboxes_instance.is_child = False
        mock_mailboxes_instance.child_streams = ["mailbox_fields"]
        mock_mailboxes_instance.key_properties = ["id"]
        mock_mailboxes_instance.sync.return_value = set()  # Returns empty set
        mock_mailboxes_class = MagicMock(return_value=mock_mailboxes_instance, is_child=False)

        with patch.dict("tap_helpscout.sync.STREAMS", {"mailboxes": mock_mailboxes_class}):
            sync(mock_client, mock_catalog, mock_state, "2020-01-01")

        # Write schema only called once (parent only, no child)
        self.assertEqual(mock_write_schema.call_count, 1)

    @patch("tap_helpscout.sync.set_currently_syncing")
    @patch("tap_helpscout.sync.write_state")
    @patch("tap_helpscout.sync.write_schema")
    @patch("tap_helpscout.sync.metadata")
    def test_sync_skips_child_stream_in_selected_list(self, mock_metadata, mock_write_schema, mock_write_state, mock_set_sync):
        """Test that child streams in get_selected_streams are skipped."""
        mock_client = MagicMock()
        mock_state = {}

        mock_set_sync.side_effect = lambda state, stream_id: {**state, "currently_syncing": stream_id}
        mock_metadata.to_map.return_value = {}

        mock_parent_stream = MagicMock()
        mock_parent_stream.tap_stream_id = "mailboxes"
        mock_parent_stream.schema.to_dict.return_value = {"type": "object"}
        mock_parent_stream.metadata = []
        mock_parent_stream.replication_key = None

        mock_child_stream = MagicMock()
        mock_child_stream.tap_stream_id = "mailbox_fields"
        mock_child_stream.schema.to_dict.return_value = {"type": "object"}
        mock_child_stream.metadata = []
        mock_child_stream.replication_key = None

        mock_catalog = MagicMock()
        # Both parent and child in selected streams
        mock_catalog.get_selected_streams.return_value = [mock_parent_stream, mock_child_stream]

        mock_mailboxes_instance = MagicMock()
        mock_mailboxes_instance.is_child = False
        mock_mailboxes_instance.child_streams = []
        mock_mailboxes_instance.key_properties = ["id"]
        mock_mailboxes_instance.sync.return_value = set()
        mock_mailboxes_class = MagicMock(return_value=mock_mailboxes_instance, is_child=False)

        mock_mailbox_fields_class = MagicMock(is_child=True)

        with patch.dict("tap_helpscout.sync.STREAMS", {
            "mailboxes": mock_mailboxes_class,
            "mailbox_fields": mock_mailbox_fields_class,
        }):
            sync(mock_client, mock_catalog, mock_state, "2020-01-01")

        # Write schema only called once (for parent, child skipped)
        self.assertEqual(mock_write_schema.call_count, 1)
