"""Unit tests for tap-helpscout transform module."""
import unittest
from unittest.mock import MagicMock, patch

from tap_helpscout.transform import (
    transform_json,
    convert_json,
    convert,
    convert_array,
    remove_embedded_links,
    denest_embedded_nodes,
    transform_conversations,
    transform_ratings,
    transform_team_users,
)


class TestConvert(unittest.TestCase):
    """Test the convert() function."""

    def test_convert_camel_case_simple(self):
        """Test converting simple camelCase to snake_case."""
        self.assertEqual(convert("userId"), "user_id")
        self.assertEqual(convert("firstName"), "first_name")

    def test_convert_camel_case_complex(self):
        """Test converting complex camelCase."""
        self.assertEqual(convert("userUpdatedAt"), "user_updated_at")
        self.assertEqual(convert("conversationStatus"), "conversation_status")

    def test_convert_single_word(self):
        """Test converting single word (no camelCase)."""
        self.assertEqual(convert("id"), "id")
        self.assertEqual(convert("name"), "name")

    def test_convert_all_caps(self):
        """Test converting all caps."""
        self.assertEqual(convert("API"), "api")
        self.assertEqual(convert("ID"), "id")


class TestConvertJson(unittest.TestCase):
    """Test the convert_json() function."""

    def test_convert_json_simple_dict(self):
        """Test converting simple dict with camelCase keys."""
        data = {"userId": 1, "firstName": "John"}
        result = convert_json(data)
        self.assertEqual(result["user_id"], 1)
        self.assertEqual(result["first_name"], "John")

    def test_convert_json_nested_dict(self):
        """Test converting nested dict."""
        data = {
            "userId": 1,
            "addressData": {
                "cityName": "New York",
                "stateName": "NY",
            }
        }
        result = convert_json(data)
        self.assertEqual(result["user_id"], 1)
        self.assertEqual(result["address_data"]["city_name"], "New York")

    def test_convert_json_with_arrays(self):
        """Test converting dict with arrays."""
        data = {
            "userId": 1,
            "emailList": [
                {"emailAddress": "john@example.com"},
                {"emailAddress": "john2@example.com"},
            ]
        }
        result = convert_json(data)
        self.assertEqual(result["user_id"], 1)
        self.assertEqual(result["email_list"][0]["email_address"], "john@example.com")


class TestConvertArray(unittest.TestCase):
    """Test the convert_array() function."""

    def test_convert_array_simple(self):
        """Test converting array of dicts."""
        data = [
            {"userId": 1, "firstName": "John"},
            {"userId": 2, "firstName": "Jane"},
        ]
        result = convert_array(data)
        self.assertEqual(result[0]["user_id"], 1)
        self.assertEqual(result[1]["first_name"], "Jane")

    def test_convert_array_nested_arrays(self):
        """Test converting nested arrays."""
        data = [[{"userId": 1}], [{"userId": 2}]]
        result = convert_array(data)
        self.assertEqual(result[0][0]["user_id"], 1)

    def test_convert_array_primitives(self):
        """Test array with primitive values."""
        data = [1, "string", 3.14, None]
        result = convert_array(data)
        self.assertEqual(result, [1, "string", 3.14, None])


class TestRemoveEmbeddedLinks(unittest.TestCase):
    """Test the remove_embedded_links() function."""

    def test_remove_embedded_links_from_dict(self):
        """Test removing _embedded and _links from dict."""
        data = {
            "id": 1,
            "name": "Test",
            "_embedded": {"customer": {"id": 100}},
            "_links": {"self": {"href": "/test"}},
        }
        result = remove_embedded_links(data)
        self.assertNotIn("_embedded", result)
        self.assertNotIn("_links", result)
        self.assertEqual(result["id"], 1)

    def test_remove_embedded_links_from_list(self):
        """Test removing _embedded and _links from list."""
        data = [
            {"id": 1, "_embedded": {}},
            {"id": 2, "_links": {}},
        ]
        result = remove_embedded_links(data)
        self.assertNotIn("_embedded", result[0])
        self.assertNotIn("_links", result[1])

    def test_remove_embedded_links_nested(self):
        """Test removing _embedded and _links from nested structures."""
        data = {
            "id": 1,
            "_embedded": {
                "customer": {
                    "id": 100,
                    "_links": {},
                }
            }
        }
        result = remove_embedded_links(data)
        self.assertNotIn("_embedded", result)


class TestDenestEmbeddedNodes(unittest.TestCase):
    """Test the denest_embedded_nodes() function."""

    def test_denest_embedded_nodes_address(self):
        """Test denesting address from _embedded."""
        data = {
            "conversations": [
                {
                    "id": 1,
                    "_embedded": {
                        "address": {"city": "New York", "state": "NY"},
                    }
                }
            ]
        }
        result = denest_embedded_nodes(data, "conversations")
        self.assertIn("address", result["conversations"][0])
        self.assertEqual(result["conversations"][0]["address"]["city"], "New York")

    def test_denest_embedded_nodes_multiple_fields(self):
        """Test denesting multiple fields from _embedded."""
        data = {
            "customers": [
                {
                    "id": 1,
                    "_embedded": {
                        "address": {"city": "NY"},
                        "emails": [{"value": "test@example.com"}],
                    }
                }
            ]
        }
        result = denest_embedded_nodes(data, "customers")
        self.assertIn("address", result["customers"][0])
        self.assertIn("emails", result["customers"][0])

    def test_denest_embedded_nodes_no_embedded(self):
        """Test denesting when _embedded is not present."""
        data = {
            "conversations": [
                {"id": 1, "subject": "Test"}
            ]
        }
        result = denest_embedded_nodes(data, "conversations")
        self.assertEqual(result["conversations"][0]["id"], 1)

    def test_denest_embedded_nodes_none_path(self):
        """Test denesting with None path returns data as-is."""
        data = {"conversations": [{"id": 1}]}
        result = denest_embedded_nodes(data, None)
        self.assertEqual(result, data)


class TestTransformConversations(unittest.TestCase):
    """Test the transform_conversations() function."""

    def test_transform_conversations_adds_updated_at(self):
        """Test that transform_conversations adds updated_at field."""
        data = {
            "conversations": [
                {
                    "id": 1,
                    "user_updated_at": "2020-06-15T10:00:00Z",
                    "customer_waiting_since": {"time": "2020-06-14T10:00:00Z"},
                }
            ]
        }
        result = transform_conversations(data, "conversations")
        self.assertIn("updated_at", result["conversations"][0])
        self.assertEqual(
            result["conversations"][0]["updated_at"],
            "2020-06-15T10:00:00Z"
        )

    def test_transform_conversations_no_path(self):
        """Test transform_conversations with None path."""
        data = {"conversations": [{"id": 1}]}
        result = transform_conversations(data, None)
        self.assertEqual(result, data)


class TestTransformRatings(unittest.TestCase):
    """Test the transform_ratings() function."""

    def test_transform_ratings_remaps_ids(self):
        """Test that transform_ratings remaps IDs correctly."""
        data = {
            "ratings": [
                {
                    "id": 1,
                    "threadid": 100,
                    "rating": 5,
                }
            ]
        }
        result = transform_ratings(data, "ratings")
        self.assertEqual(result["ratings"][0]["conversation_id"], 1)
        self.assertEqual(result["ratings"][0]["thread_id"], 100)
        self.assertNotIn("id", result["ratings"][0])
        self.assertNotIn("threadid", result["ratings"][0])


class TestTransformTeamUsers(unittest.TestCase):
    """Test the transform_team_users() function."""

    def test_transform_team_users_adds_user_id(self):
        """Test that transform_team_users adds user_id."""
        data = {
            "team_members": [
                {"id": 100, "name": "John"},
                {"id": 101, "name": "Jane"},
            ]
        }
        result = transform_team_users(data, "team_members")
        self.assertEqual(result["team_members"][0]["user_id"], 100)
        self.assertEqual(result["team_members"][1]["user_id"], 101)


class TestTransformJson(unittest.TestCase):
    """Test the transform_json() function."""

    def test_transform_json_conversations_stream(self):
        """Test transform_json with conversations stream."""
        data = {
            "conversations": [
                {
                    "id": 1,
                    "userId": 100,
                    "_embedded": {},
                    "_links": {},
                    "user_updated_at": "2020-06-15T10:00:00Z",
                }
            ]
        }
        result = transform_json(data, "conversations", "conversations")
        self.assertNotIn("_embedded", str(result))
        self.assertNotIn("_links", str(result))
        self.assertIn("user_id", result["conversations"][0])

    def test_transform_json_happiness_ratings_stream(self):
        """Test transform_json with happiness_ratings_report stream."""
        data = {
            "ratings": [
                {"id": 1, "threadid": 100, "rating": 5}
            ]
        }
        result = transform_json(data, "ratings", "happiness_ratings_report")
        self.assertEqual(result["ratings"][0]["conversation_id"], 1)

    def test_transform_json_team_members_stream(self):
        """Test transform_json with team_members stream."""
        data = {
            "team_members": [
                {"id": 100, "firstName": "John"}
            ]
        }
        result = transform_json(data, "team_members", "team_members")
        self.assertEqual(result["team_members"][0]["user_id"], 100)
        self.assertEqual(result["team_members"][0]["first_name"], "John")
