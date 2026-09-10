"""Unit tests for tap-helpscout stream methods."""
import unittest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

from tap_helpscout.streams.abstract import IncrementalStream, FullStream
from tap_helpscout.streams.conversations import Conversations
from tap_helpscout.streams.conversation_threads import ConversationThreads
from tap_helpscout.streams.mailbox_fields import MailBoxFields
from tap_helpscout.streams.happiness_ratings_report import HappinessRatingsReport


class TestMakeRequestParams(unittest.TestCase):
    """Test BaseStream.make_request_params() method."""

    def test_make_request_params_no_replication_query_field(self):
        """Test params generation for stream without replication query field."""
        stream = Conversations(client=MagicMock(), start_date="2020-01-01")
        result = stream.make_request_params({})
        self.assertIn("status=all", result)
        self.assertIn("sortField=modifiedAt", result)

    def test_make_request_params_with_bookmark(self):
        """Test params generation with bookmark value."""
        stream = Conversations(client=MagicMock(), start_date="2020-01-01")
        state = {
            "bookmarks": {"conversations": "2020-06-01"}
        }
        result = stream.make_request_params(state)
        self.assertIn("modifiedSince=2020-06-01", result)

    def test_make_request_params_happiness_ratings_report(self):
        """Test params for happiness_ratings_report special handling."""
        stream = HappinessRatingsReport(client=MagicMock(), start_date="2020-01-01")
        result = stream.make_request_params({})
        self.assertIn("start=2020-01-01", result)
        self.assertIn("end=", result)  # end should be current datetime

    def test_make_request_params_with_empty_state(self):
        """Test params generation with empty state uses start_date."""
        stream = Conversations(client=MagicMock(), start_date="2020-01-01")
        result = stream.make_request_params({})
        self.assertIn("modifiedSince=2020-01-01", result)


class TestTransformRecords(unittest.TestCase):
    """Test BaseStream.transform_records() method."""

    def test_transform_records_happiness_ratings_with_data_key(self):
        """Test transform_records for happiness_ratings_report."""
        stream = HappinessRatingsReport(client=MagicMock(), start_date="2020-01-01")
        data = {
            "ratings": [
                {"id": 1, "rating": 5},
                {"id": 2, "rating": 4},
            ]
        }
        with patch("tap_helpscout.streams.abstract.transform_json") as mock_transform:
            mock_transform.return_value = {"results": data["ratings"]}
            result = stream.transform_records(data)
            self.assertEqual(result, data["ratings"])

    def test_transform_records_with_embedded(self):
        """Test transform_records with _embedded data."""
        stream = Conversations(client=MagicMock(), start_date="2020-01-01")
        data = {
            "_embedded": {
                "conversations": [
                    {"id": 1, "subject": "test"},
                ]
            }
        }
        with patch("tap_helpscout.streams.abstract.transform_json") as mock_transform:
            mock_transform.return_value = {"conversations": data["_embedded"]["conversations"]}
            result = stream.transform_records(data)
            self.assertEqual(len(result), 1)

    def test_transform_records_no_data(self):
        """Test transform_records with no embedded data."""
        stream = Conversations(client=MagicMock(), start_date="2020-01-01")
        data = {"page": {"number": 1}}
        result = stream.transform_records(data)
        self.assertEqual(result, [])


class TestGetRecords(unittest.TestCase):
    """Test BaseStream.get_records() method."""

    def test_get_records_single_page(self):
        """Test get_records with single page response."""
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "page": {"number": 1, "totalPages": 1},
            "_embedded": {"conversations": [{"id": 1}]}
        }
        stream = Conversations(client=mock_client, start_date="2020-01-01")

        with patch.object(stream, "transform_records", return_value=[{"id": 1}]):
            records = list(stream.get_records({}))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 1)

    def test_get_records_multiple_pages(self):
        """Test get_records with multiple pages."""
        mock_client = MagicMock()
        mock_client.get.side_effect = [
            {"page": {"number": 1, "totalPages": 2}, "_embedded": {"conversations": [{"id": 1}]}},
            {"page": {"number": 2, "totalPages": 2}, "_embedded": {"conversations": [{"id": 2}]}},
        ]
        stream = Conversations(client=mock_client, start_date="2020-01-01")

        with patch.object(stream, "transform_records", side_effect=[[{"id": 1}], [{"id": 2}]]):
            records = list(stream.get_records({}))

        self.assertEqual(len(records), 2)
        self.assertEqual(mock_client.get.call_count, 2)

    def test_get_records_with_parent_id(self):
        """Test get_records with parent_id formatting."""
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "page": {"number": 1, "totalPages": 1},
            "_embedded": {"threads": [{"id": 1}]}
        }
        stream = ConversationThreads(client=mock_client, start_date="2020-01-01")

        with patch.object(stream, "transform_records", return_value=[{"id": 1}]):
            list(stream.get_records({}, parent_id=123))

        # Verify path was formatted with parent_id
        call_args = mock_client.get.call_args
        self.assertIn("123", call_args[0][0])


class TestProcessRecords(unittest.TestCase):
    """Test BaseStream.process_records() method."""

    @patch("tap_helpscout.streams.abstract.singer.write_record")
    @patch("tap_helpscout.streams.abstract.Transformer")
    def test_process_records_full_table_stream(self, mock_transformer_class, mock_write_record):
        """Test process_records for FULL_TABLE stream."""
        mock_client = MagicMock()
        mock_transformer = MagicMock()
        mock_transformer_class.return_value.__enter__.return_value = mock_transformer

        mock_transformer.transform.return_value = {"id": 1, "name": "test"}

        stream = ConversationThreads(client=mock_client, start_date="2020-01-01")

        with patch.object(stream, "get_records", return_value=[{"id": 1, "name": "test"}]):
            parent_ids = stream.process_records({}, {}, [], is_parent=False)

        mock_write_record.assert_called_once()
        self.assertEqual(parent_ids, set())

    @patch("tap_helpscout.streams.abstract.singer.write_record")
    @patch("tap_helpscout.streams.abstract.Transformer")
    def test_process_records_incremental_stream(self, mock_transformer_class, mock_write_record):
        """Test process_records for INCREMENTAL stream with bookmark."""
        mock_client = MagicMock()
        mock_transformer = MagicMock()
        mock_transformer_class.return_value.__enter__.return_value = mock_transformer

        mock_transformer.transform.return_value = {
            "id": 1,
            "updated_at": "2020-06-15T10:00:00Z"
        }

        stream = Conversations(client=mock_client, start_date="2020-01-01")

        with patch.object(stream, "get_records", return_value=[{"id": 1, "updated_at": "2020-06-15T10:00:00Z"}]):
            with patch.object(stream, "write_bookmark"):
                parent_ids = stream.process_records({}, {}, [], is_parent=False)

        mock_write_record.assert_called_once()

    @patch("tap_helpscout.streams.abstract.singer.write_record")
    @patch("tap_helpscout.streams.abstract.Transformer")
    def test_process_records_child_stream_inherits_parent_bookmark(
        self, mock_transformer_class, mock_write_record
    ):
        """Test child incremental streams derive record bookmarks from the parent stream."""
        mock_client = MagicMock()
        mock_transformer = MagicMock()
        mock_transformer_class.return_value.__enter__.return_value = mock_transformer

        mock_transformer.transform.return_value = {
            "id": 1,
            "mailbox_id": 10,
        }

        state = {
            "bookmarks": {
                "mailboxes": "2020-06-15T10:00:00Z",
            }
        }
        stream = MailBoxFields(client=mock_client, start_date="2020-01-01")

        with patch.object(stream, "get_records", return_value=[{"id": 1, "mailbox_id": 10}]):
            with patch.object(stream, "write_bookmark") as mock_write_bookmark:
                stream.process_records(state, {}, [], is_parent=False, parent_id=10)

        mock_write_record.assert_called_once_with(
            "mailbox_fields",
            {"id": 1, "mailbox_id": 10, "mailboxes_updated_at": "2020-06-15T10:00:00Z"},
        )
        mock_write_bookmark.assert_called_once_with(state, "2020-06-15T10:00:00Z")

    @patch("tap_helpscout.streams.abstract.singer.write_record")
    @patch("tap_helpscout.streams.abstract.Transformer")
    def test_process_records_parent_stream_collects_ids(self, mock_transformer_class, mock_write_record):
        """Test that process_records collects parent IDs."""
        mock_client = MagicMock()
        mock_transformer = MagicMock()
        mock_transformer_class.return_value.__enter__.return_value = mock_transformer

        mock_transformer.transform.return_value = {
            "id": 123,
            "updated_at": "2020-06-15T10:00:00Z"
        }

        stream = Conversations(client=mock_client, start_date="2020-01-01")

        with patch.object(stream, "get_records", return_value=[{"id": 123, "updated_at": "2020-06-15T10:00:00Z"}]):
            with patch.object(stream, "write_bookmark"):
                parent_ids = stream.process_records({}, {}, [], is_parent=True)

        self.assertIn(123, parent_ids)

    @patch("tap_helpscout.streams.abstract.singer.write_record")
    @patch("tap_helpscout.streams.abstract.Transformer")
    def test_process_records_respects_bookmark_filter(self, mock_transformer_class, mock_write_record):
        """Test that old records are filtered by bookmark."""
        mock_client = MagicMock()
        mock_transformer = MagicMock()
        mock_transformer_class.return_value.__enter__.return_value = mock_transformer

        mock_transformer.transform.return_value = {
            "id": 1,
            "updated_at": "2020-01-01T10:00:00Z"  # older than bookmark
        }

        stream = Conversations(client=mock_client, start_date="2020-01-01")
        state = {"bookmarks": {"conversations": "2020-06-01T00:00:00Z"}}

        with patch.object(stream, "get_records", return_value=[{"id": 1, "updated_at": "2020-01-01T10:00:00Z"}]):
            stream.process_records(state, {}, [], is_parent=False)

        # Record should not be written because it's older than bookmark
        mock_write_record.assert_not_called()

    @patch("tap_helpscout.streams.abstract.singer.write_record")
    @patch("tap_helpscout.streams.abstract.Transformer")
    def test_process_records_handles_none_record_bookmark(self, mock_transformer_class, mock_write_record):
        """Test incremental processing safely handles records with null replication values."""
        mock_client = MagicMock()
        mock_transformer = MagicMock()
        mock_transformer_class.return_value.__enter__.return_value = mock_transformer

        mock_transformer.transform.return_value = {
            "id": 1,
            "updated_at": None,
        }

        stream = Conversations(client=mock_client, start_date="2020-01-01")
        state = {"bookmarks": {"conversations": "2020-06-01T00:00:00Z"}}

        with patch.object(stream, "get_records", return_value=[{"id": 1, "updated_at": None}]):
            with patch.object(stream, "write_bookmark") as mock_write_bookmark:
                stream.process_records(state, {}, [], is_parent=False)

        mock_write_record.assert_called_once_with("conversations", {"id": 1, "updated_at": None})
        mock_write_bookmark.assert_called_once_with(state, "2020-06-01T00:00:00Z")


class TestSync(unittest.TestCase):
    """Test BaseStream.sync() method."""

    @patch("tap_helpscout.streams.abstract.IncrementalStream.process_records")
    def test_sync_parent_stream(self, mock_process):
        """Test sync for parent stream."""
        mock_client = MagicMock()
        mock_process.return_value = {123, 456}

        stream = Conversations(client=mock_client, start_date="2020-01-01")
        result = stream.sync({}, {}, [], is_child=False)

        mock_process.assert_called_once()
        self.assertEqual(result, {123, 456})

    @patch("tap_helpscout.streams.abstract.IncrementalStream.process_records")
    def test_sync_child_stream_persists_bookmark_once_after_all_parents(self, mock_process):
        """Test child incremental streams use the starting bookmark for all parents."""
        mock_client = MagicMock()
        stream = ConversationThreads(client=mock_client, start_date="2020-01-01")
        state = {"bookmarks": {"conversation_threads": "2020-01-01T00:00:00Z"}}
        seen_bookmarks = []

        def process_side_effect(child_state, schema, stream_metadata, is_parent, parent_id, persist_bookmark=True):
            seen_bookmarks.append(child_state["bookmarks"]["conversation_threads"])
            if parent_id == 100:
                child_state["bookmarks"]["conversation_threads"] = "2020-01-02T00:00:00Z"
            elif parent_id == 200:
                child_state["bookmarks"]["conversation_threads"] = "2020-01-03T00:00:00Z"
            return set()

        mock_process.side_effect = process_side_effect

        with patch.object(stream, "write_bookmark") as mock_write_bookmark:
            stream.sync(state, {}, [], parent_ids=[100, 200], is_child=True)

        self.assertEqual(mock_process.call_count, 2)
        first_call = mock_process.call_args_list[0]
        second_call = mock_process.call_args_list[1]
        self.assertEqual(seen_bookmarks, ["2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"])
        self.assertFalse(first_call.kwargs["persist_bookmark"])
        self.assertFalse(second_call.kwargs["persist_bookmark"])
        mock_write_bookmark.assert_called_once_with(state, "2020-01-03T00:00:00Z")

    @patch("tap_helpscout.streams.abstract.IncrementalStream.process_records")
    def test_sync_child_stream_handles_none_child_bookmark(self, mock_process):
        """Test child sync does not parse None bookmarks while aggregating max bookmark."""
        mock_client = MagicMock()
        stream = ConversationThreads(client=mock_client, start_date="2020-01-01")
        state = {"bookmarks": {"conversation_threads": "2020-01-01T00:00:00Z"}}

        def process_side_effect(child_state, schema, stream_metadata, is_parent, parent_id, persist_bookmark=True):
            if parent_id == 100:
                child_state["bookmarks"]["conversation_threads"] = None
            elif parent_id == 200:
                child_state["bookmarks"]["conversation_threads"] = "2020-01-03T00:00:00Z"
            return set()

        mock_process.side_effect = process_side_effect

        with patch.object(stream, "write_bookmark") as mock_write_bookmark:
            stream.sync(state, {}, [], parent_ids=[100, 200], is_child=True)

        mock_write_bookmark.assert_called_once_with(state, "2020-01-03T00:00:00Z")
