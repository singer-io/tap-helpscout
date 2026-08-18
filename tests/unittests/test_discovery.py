"""Unit tests for the tap-helpscout discovery module."""
import unittest
from unittest.mock import MagicMock, patch

from tap_helpscout.discover import (
    _apply_access_checks,
    _prune_inaccessible_children,
    discover,
    get_schemas,
)
from tap_helpscout.exceptions import Http403Error
from tap_helpscout.streams import STREAMS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(forbidden_streams=None):
    """Return a mock HelpScoutClient whose .get() raises Http403Error for any
    stream whose *path* is in *forbidden_streams* (a collection of path prefixes
    such as '/conversations')."""
    forbidden_streams = forbidden_streams or []

    def _get(path, **kwargs):
        for forbidden_path in forbidden_streams:
            if path.startswith(forbidden_path):
                raise Http403Error()
        return {}

    client = MagicMock()
    client.get.side_effect = _get
    return client


# ---------------------------------------------------------------------------
# get_schemas
# ---------------------------------------------------------------------------

class TestGetSchemas(unittest.TestCase):
    def test_returns_all_streams(self):
        schemas, metadata = get_schemas()
        self.assertEqual(set(schemas.keys()), set(STREAMS.keys()))
        self.assertEqual(set(metadata.keys()), set(STREAMS.keys()))

    def test_child_replication_method_matches_parent(self):
        _, metadata = get_schemas()

        def top_level_metadata(stream_name):
            return next(item["metadata"] for item in metadata[stream_name] if item.get("breadcrumb") == ())

        for stream_name, stream_cls in STREAMS.items():
            if not stream_cls.parent:
                continue

            with self.subTest(stream=stream_name):
                child_metadata = top_level_metadata(stream_name)
                parent_metadata = top_level_metadata(stream_cls.parent)
                self.assertEqual(
                    parent_metadata.get("forced-replication-method"),
                    child_metadata.get("forced-replication-method"),
                    f"Child stream {stream_name} should match parent {stream_cls.parent} replication method",
                )

    def test_schemas_are_dicts(self):
        schemas, _ = get_schemas()
        for name, schema in schemas.items():
            self.assertIsInstance(schema, dict, f"Schema for {name} should be a dict")

    def test_metadata_are_lists(self):
        _, metadata = get_schemas()
        for name, meta in metadata.items():
            self.assertIsInstance(meta, list, f"Metadata for {name} should be a list")

    def test_schema_has_properties(self):
        schemas, _ = get_schemas()
        for name, schema in schemas.items():
            self.assertIn("properties", schema, f"Schema for {name} should have properties")


# ---------------------------------------------------------------------------
# discover() – no client (legacy / backward-compatible path)
# ---------------------------------------------------------------------------

class TestDiscoverNoClient(unittest.TestCase):
    def test_returns_catalog_with_all_streams(self):
        catalog = discover()
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(stream_ids, set(STREAMS.keys()))

    def test_catalog_entries_have_key_properties(self):
        catalog = discover()
        for entry in catalog.streams:
            self.assertTrue(
                len(entry.key_properties) > 0,
                f"Stream {entry.tap_stream_id} should have key_properties",
            )

    def test_catalog_entries_have_schema(self):
        catalog = discover()
        for entry in catalog.streams:
            self.assertIsNotNone(entry.schema)


# ---------------------------------------------------------------------------
# discover() – with client, all streams accessible
# ---------------------------------------------------------------------------

class TestDiscoverAllAccessible(unittest.TestCase):
    def test_all_streams_present_when_fully_accessible(self):
        client = _make_client(forbidden_streams=[])
        catalog = discover(client)
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(stream_ids, set(STREAMS.keys()))


# ---------------------------------------------------------------------------
# _apply_access_checks – partial access
# ---------------------------------------------------------------------------

class TestApplyAccessChecksPartialAccess(unittest.TestCase):
    def test_inaccessible_parent_removed_from_schemas(self):
        schemas, metadata = get_schemas()
        client = _make_client(forbidden_streams=["/conversations"])
        _apply_access_checks(client, schemas, metadata)
        self.assertNotIn("conversations", schemas)
        self.assertNotIn("conversations", metadata)

    def test_inaccessible_parent_child_also_removed(self):
        """conversation_threads is a child of conversations."""
        schemas, metadata = get_schemas()
        client = _make_client(forbidden_streams=["/conversations"])
        _apply_access_checks(client, schemas, metadata)
        self.assertNotIn("conversation_threads", schemas)
        self.assertNotIn("conversation_threads", metadata)

    def test_accessible_streams_remain(self):
        schemas, metadata = get_schemas()
        client = _make_client(forbidden_streams=["/conversations"])
        _apply_access_checks(client, schemas, metadata)
        # All other parent streams should still be present
        for name in ("mailboxes", "customers", "teams", "users", "workflows"):
            self.assertIn(name, schemas)

    def test_warning_logged_for_inaccessible_stream(self):
        schemas, metadata = get_schemas()
        client = _make_client(forbidden_streams=["/conversations"])
        with self.assertLogs(level="WARNING") as log:
            _apply_access_checks(client, schemas, metadata)
        # At least one warning about the excluded stream
        combined = " ".join(log.output)
        self.assertIn("conversations", combined)

    def test_multiple_inaccessible_parents_removed(self):
        schemas, metadata = get_schemas()
        client = _make_client(forbidden_streams=["/conversations", "/mailboxes"])
        _apply_access_checks(client, schemas, metadata)
        for name in ("conversations", "conversation_threads",
                     "mailboxes", "mailbox_fields", "mailbox_folders"):
            self.assertNotIn(name, schemas, f"{name} should be removed")

    def test_mailbox_children_removed_when_parent_inaccessible(self):
        schemas, metadata = get_schemas()
        client = _make_client(forbidden_streams=["/mailboxes"])
        _apply_access_checks(client, schemas, metadata)
        self.assertNotIn("mailbox_fields", schemas)
        self.assertNotIn("mailbox_folders", schemas)

    def test_team_members_removed_when_teams_inaccessible(self):
        schemas, metadata = get_schemas()
        client = _make_client(forbidden_streams=["/teams"])
        _apply_access_checks(client, schemas, metadata)
        self.assertNotIn("team_members", schemas)


# ---------------------------------------------------------------------------
# _apply_access_checks – complete denial (all parents forbidden)
# ---------------------------------------------------------------------------

class TestApplyAccessChecksCompleteDenial(unittest.TestCase):
    def _all_parent_paths(self):
        return [s.path for s in STREAMS.values() if not s.parent]

    def test_raises_http403_when_no_parent_accessible(self):
        schemas, metadata = get_schemas()
        client = _make_client(forbidden_streams=self._all_parent_paths())
        with self.assertRaises(Http403Error):
            _apply_access_checks(client, schemas, metadata)

    def test_discover_raises_http403_when_no_parent_accessible(self):
        client = _make_client(forbidden_streams=self._all_parent_paths())
        with self.assertRaises(Http403Error):
            discover(client)


# ---------------------------------------------------------------------------
# _prune_inaccessible_children
# ---------------------------------------------------------------------------

class TestPruneInaccessibleChildren(unittest.TestCase):
    def test_child_pruned_when_parent_missing(self):
        schemas, metadata = get_schemas()
        # Simulate parent already removed
        schemas.pop("conversations")
        metadata.pop("conversations")
        _prune_inaccessible_children(schemas, metadata)
        self.assertNotIn("conversation_threads", schemas)

    def test_child_kept_when_parent_present(self):
        schemas, metadata = get_schemas()
        _prune_inaccessible_children(schemas, metadata)
        self.assertIn("conversation_threads", schemas)

    def test_no_error_when_no_children_present(self):
        schemas = {"conversations": {}, "customers": {}}
        metadata = {"conversations": [], "customers": []}
        # Should not raise even with no child-stream keys in schemas
        _prune_inaccessible_children(schemas, metadata)


# ---------------------------------------------------------------------------
# check_access() on stream instances
# ---------------------------------------------------------------------------

class TestCheckAccess(unittest.TestCase):
    def test_child_stream_always_returns_true(self):
        from tap_helpscout.streams.conversation_threads import ConversationThreads
        stream = ConversationThreads(client=MagicMock())
        self.assertTrue(stream.check_access())

    def test_parent_stream_returns_true_on_success(self):
        from tap_helpscout.streams.conversations import Conversations
        client = MagicMock()
        client.get.return_value = {}
        stream = Conversations(client=client)
        self.assertTrue(stream.check_access())

    def test_parent_stream_returns_false_on_403(self):
        from tap_helpscout.streams.conversations import Conversations
        client = MagicMock()
        client.get.side_effect = Http403Error()
        stream = Conversations(client=client)
        self.assertFalse(stream.check_access())

    def test_non_403_exception_propagates(self):
        from tap_helpscout.streams.conversations import Conversations
        from tap_helpscout.exceptions import Http500Error
        client = MagicMock()
        client.get.side_effect = Http500Error()
        stream = Conversations(client=client)
        with self.assertRaises(Http500Error):
            stream.check_access()
