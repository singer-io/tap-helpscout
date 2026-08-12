"""Unit tests for tap-helpscout helpers module."""
import unittest
import os
from datetime import datetime

from tap_helpscout.helpers import get_abs_path, parse_date


class TestGetAbsPath(unittest.TestCase):
    """Test the get_abs_path() function."""

    def test_get_abs_path_returns_absolute_path(self):
        """Test that get_abs_path returns an absolute path."""
        result = get_abs_path("schemas/conversations.json")
        self.assertTrue(os.path.isabs(result))
        self.assertIn("schemas/conversations.json", result)

    def test_get_abs_path_with_nested_path(self):
        """Test get_abs_path with nested directories."""
        result = get_abs_path("schemas/nested/test.json")
        self.assertTrue(os.path.isabs(result))
        self.assertIn("schemas/nested/test.json", result)


class TestParseDate(unittest.TestCase):
    """Test the parse_date() function."""

    def test_parse_date_iso_format_with_microseconds_z(self):
        """Test parsing ISO format with microseconds and Z suffix."""
        result = parse_date("2020-01-15T10:30:45.123456Z")
        self.assertEqual(result.year, 2020)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)
        self.assertEqual(result.hour, 10)
        self.assertEqual(result.minute, 30)
        self.assertEqual(result.second, 45)

    def test_parse_date_iso_format_no_microseconds_z(self):
        """Test parsing ISO format without microseconds and Z suffix."""
        result = parse_date("2020-01-15T10:30:45Z")
        self.assertEqual(result.year, 2020)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)
        self.assertEqual(result.hour, 10)
        self.assertEqual(result.minute, 30)
        self.assertEqual(result.second, 45)

    def test_parse_date_iso_format_with_microseconds_plus_offset(self):
        """Test parsing ISO format with microseconds and +00:00 offset."""
        result = parse_date("2020-01-15T10:30:45.123456+00:00")
        self.assertEqual(result.year, 2020)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)

    def test_parse_date_iso_format_no_microseconds_plus_offset(self):
        """Test parsing ISO format without microseconds and +00:00 offset."""
        result = parse_date("2020-01-15T10:30:45+00:00")
        self.assertEqual(result.year, 2020)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)

    def test_parse_date_simple_date_format(self):
        """Test parsing simple date format YYYY-MM-DD."""
        result = parse_date("2020-01-15")
        self.assertEqual(result.year, 2020)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)
        self.assertEqual(result.hour, 0)
        self.assertEqual(result.minute, 0)

    def test_parse_date_with_various_dates(self):
        """Test parsing various valid dates."""
        test_dates = [
            "2021-12-31T23:59:59.999999Z",
            "2020-02-29T00:00:00Z",  # leap year
            "2019-06-01T12:00:00+00:00",
        ]
        for date_str in test_dates:
            result = parse_date(date_str)
            self.assertIsInstance(result, datetime)

    def test_parse_date_invalid_format_returns_none(self):
        """Test that parsing an invalid date returns None."""
        result = parse_date("invalid-date-format")
        self.assertIsNone(result)

    def test_parse_date_invalid_empty_string_returns_none(self):
        """Test that parsing empty string returns None."""
        result = parse_date("")
        self.assertIsNone(result)
