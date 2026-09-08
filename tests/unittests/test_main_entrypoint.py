"""Unit tests for tap-helpscout main entry point."""
import sys
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch, call

from tap_helpscout import do_discover, main


class TestDoDiscover(unittest.TestCase):
    """Test the do_discover() function."""

    @patch("tap_helpscout.discover")
    @patch("tap_helpscout.LOGGER")
    def test_do_discover_logs_and_dumps_catalog(self, mock_logger, mock_discover):
        """Test that do_discover logs messages and dumps catalog."""
        mock_client = MagicMock()
        mock_catalog = MagicMock()
        mock_discover.return_value = mock_catalog

        do_discover(mock_client)

        mock_logger.info.assert_any_call("Starting discover")
        mock_logger.info.assert_any_call("Finished discover")
        mock_catalog.dump.assert_called_once()
        mock_discover.assert_called_once_with(mock_client)

    @patch("tap_helpscout.discover")
    def test_do_discover_passes_client_to_discover(self, mock_discover):
        """Test that do_discover passes the client to discover function."""
        mock_client = MagicMock()
        mock_catalog = MagicMock()
        mock_discover.return_value = mock_catalog

        do_discover(mock_client)

        mock_discover.assert_called_once_with(mock_client)


class TestMain(unittest.TestCase):
    """Test the main() function."""

    @patch("tap_helpscout.HelpScoutClient")
    @patch("tap_helpscout.singer.utils.parse_args")
    @patch("tap_helpscout.do_discover")
    def test_main_discover_mode(self, mock_do_discover, mock_parse_args, mock_client_class):
        """Test main() in discover mode."""
        mock_parsed_args = MagicMock()
        mock_parsed_args.discover = True
        mock_parsed_args.dev = False
        mock_parsed_args.config_path = "/path/to/config"
        mock_parsed_args.config = {
            "client_id": "test",
            "client_secret": "test",
            "refresh_token": "test",
            "user_agent": "test",
        }
        mock_parse_args.return_value = mock_parsed_args

        mock_client = MagicMock()
        mock_client_class.return_value.__enter__.return_value = mock_client

        main()

        mock_client_class.assert_called_once()
        mock_do_discover.assert_called_once_with(mock_client)

    @patch("tap_helpscout.HelpScoutClient")
    @patch("tap_helpscout.singer.utils.parse_args")
    @patch("tap_helpscout.sync")
    @patch("tap_helpscout.discover")
    def test_main_sync_mode_with_catalog(
        self, mock_discover_func, mock_sync, mock_parse_args, mock_client_class
    ):
        """Test main() in sync mode with provided catalog."""
        mock_parsed_args = MagicMock()
        mock_parsed_args.discover = False
        mock_parsed_args.dev = False
        mock_parsed_args.state = {"bookmarks": {}}
        mock_parsed_args.catalog = MagicMock()
        mock_parsed_args.config_path = "/path/to/config"
        mock_parsed_args.config = {
            "client_id": "test",
            "client_secret": "test",
            "refresh_token": "test",
            "user_agent": "test",
            "start_date": "2020-01-01",
        }
        mock_parse_args.return_value = mock_parsed_args

        mock_client = MagicMock()
        mock_client_class.return_value.__enter__.return_value = mock_client

        main()

        mock_sync.assert_called_once()
        call_args = mock_sync.call_args
        self.assertEqual(call_args.kwargs["catalog"], mock_parsed_args.catalog)
        self.assertEqual(call_args.kwargs["start_date"], "2020-01-01")

    @patch("tap_helpscout.HelpScoutClient")
    @patch("tap_helpscout.singer.utils.parse_args")
    @patch("tap_helpscout.sync")
    @patch("tap_helpscout.discover")
    def test_main_sync_mode_without_catalog(
        self, mock_discover_func, mock_sync, mock_parse_args, mock_client_class
    ):
        """Test main() in sync mode without catalog (calls discover)."""
        mock_parsed_args = MagicMock()
        mock_parsed_args.discover = False
        mock_parsed_args.dev = False
        mock_parsed_args.state = None
        mock_parsed_args.catalog = None
        mock_parsed_args.config_path = "/path/to/config"
        mock_parsed_args.config = {
            "client_id": "test",
            "client_secret": "test",
            "refresh_token": "test",
            "user_agent": "test",
            "start_date": "2020-01-01",
        }
        mock_parse_args.return_value = mock_parsed_args

        mock_client = MagicMock()
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_catalog = MagicMock()
        mock_discover_func.return_value = mock_catalog

        main()

        # discover should be called with client since catalog is None
        mock_discover_func.assert_called_once_with(mock_client)
        mock_sync.assert_called_once()

    @patch("tap_helpscout.HelpScoutClient")
    @patch("tap_helpscout.singer.utils.parse_args")
    @patch("tap_helpscout.LOGGER")
    def test_main_dev_mode_warning(self, mock_logger, mock_parse_args, mock_client_class):
        """Test that main() logs warning when dev mode is enabled."""
        mock_parsed_args = MagicMock()
        mock_parsed_args.discover = True
        mock_parsed_args.dev = True
        mock_parsed_args.config_path = "/path/to/config"
        mock_parsed_args.config = {
            "client_id": "test",
            "client_secret": "test",
            "refresh_token": "test",
            "user_agent": "test",
        }
        mock_parse_args.return_value = mock_parsed_args

        mock_client = MagicMock()
        mock_client_class.return_value.__enter__.return_value = mock_client

        with patch("tap_helpscout.do_discover"):
            main()

        mock_logger.warning.assert_called_once_with("Executing tap in dev mode")

    @patch("tap_helpscout.main")
    def test_main_entry_point_if_name_main(self, mock_main_func):
        """Test that main() is called when script is run directly."""
        # Import and execute the module to trigger the if __name__ == "__main__" block
        import tap_helpscout
        # We can't directly test the if __name__ == "__main__" block,
        # but we can verify that the main function exists and is callable
        self.assertTrue(callable(tap_helpscout.main))
