"""Comprehensive tests for tap-helpscout client module."""
import unittest
from unittest.mock import MagicMock, patch, mock_open, call
import json
from datetime import datetime, timedelta, timezone
import requests

from tap_helpscout.client import HelpScoutClient, raise_for_error
from tap_helpscout.exceptions import (
    Http400Error,
    Http401Error,
    Http403Error,
    Http404Error,
    Http409Error,
    Http412Error,
    Http413Error,
    Http415Error,
    Http500Error,
    Http503Error,
    Http504Error,
    Http429Error,
    HttpClientException,
    AccessTokenMissing,
)


class TestRaiseForError(unittest.TestCase):
    """Test the raise_for_error() function."""

    def test_raise_for_error_400(self):
        """Test raising Http400Error for 400 status."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.raise_for_status.side_effect = requests.HTTPError("400 error")

        with self.assertRaises(Http400Error):
            raise_for_error(mock_response)

    def test_raise_for_error_401(self):
        """Test raising Http401Error for 401 status."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.HTTPError("401 error")

        with self.assertRaises(Http401Error):
            raise_for_error(mock_response)

    def test_raise_for_error_403(self):
        """Test raising Http403Error for 403 status."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = requests.HTTPError("403 error")

        with self.assertRaises(Http403Error):
            raise_for_error(mock_response)

    def test_raise_for_error_404(self):
        """Test raising Http404Error for 404 status."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 error")

        with self.assertRaises(Http404Error):
            raise_for_error(mock_response)

    def test_raise_for_error_409(self):
        """Test raising Http409Error for 409 status."""
        mock_response = MagicMock()
        mock_response.status_code = 409
        mock_response.raise_for_status.side_effect = requests.HTTPError("409 error")

        with self.assertRaises(Http409Error):
            raise_for_error(mock_response)

    def test_raise_for_error_412(self):
        """Test raising Http412Error for 412 status."""
        mock_response = MagicMock()
        mock_response.status_code = 412
        mock_response.raise_for_status.side_effect = requests.HTTPError("412 error")

        with self.assertRaises(Http412Error):
            raise_for_error(mock_response)

    def test_raise_for_error_413(self):
        """Test raising Http413Error for 413 status."""
        mock_response = MagicMock()
        mock_response.status_code = 413
        mock_response.raise_for_status.side_effect = requests.HTTPError("413 error")

        with self.assertRaises(Http413Error):
            raise_for_error(mock_response)

    def test_raise_for_error_415(self):
        """Test raising Http415Error for 415 status."""
        mock_response = MagicMock()
        mock_response.status_code = 415
        mock_response.raise_for_status.side_effect = requests.HTTPError("415 error")

        with self.assertRaises(Http415Error):
            raise_for_error(mock_response)

    def test_raise_for_error_500(self):
        """Test raising Http500Error for 500 status."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.HTTPError("500 error")

        with self.assertRaises(Http500Error):
            raise_for_error(mock_response)

    def test_raise_for_error_503(self):
        """Test raising Http503Error for 503 status."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.raise_for_status.side_effect = requests.HTTPError("503 error")

        with self.assertRaises(Http503Error):
            raise_for_error(mock_response)

    def test_raise_for_error_504(self):
        """Test raising Http504Error for 504 status."""
        mock_response = MagicMock()
        mock_response.status_code = 504
        mock_response.raise_for_status.side_effect = requests.HTTPError("504 error")

        with self.assertRaises(Http504Error):
            raise_for_error(mock_response)

    def test_raise_for_error_429(self):
        """Test raising Http429Error for 429 status."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = requests.HTTPError("429 error")

        with self.assertRaises(Http429Error):
            raise_for_error(mock_response)

    def test_raise_for_error_unknown_status(self):
        """Test raising HttpClientException for unknown status."""
        mock_response = MagicMock()
        mock_response.status_code = 418
        mock_response.raise_for_status.side_effect = requests.HTTPError("418 error")

        with self.assertRaises(HttpClientException):
            raise_for_error(mock_response)

    def test_raise_for_error_attribute_error(self):
        """Test raising HttpClientException when getattr fails."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.raise_for_status.side_effect = requests.HTTPError("400 error")

        with patch("tap_helpscout.client.errors.HttpClientException") as mock_exc:
            mock_exc.side_effect = AttributeError("Cannot get attribute")
            with self.assertRaises(AttributeError):
                raise_for_error(mock_response)

    def test_raise_for_error_connection_error(self):
        """Test handling of ConnectionError."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.ConnectionError("Connection failed")

        with self.assertRaises(HttpClientException):
            raise_for_error(mock_response)


class TestHelpScoutClientInit(unittest.TestCase):
    """Test HelpScoutClient initialization."""

    def test_client_init_with_all_config(self):
        """Test client initialization with all config keys."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "existing_token",
            "start_date": "2024-01-01",
        }

        client = HelpScoutClient("/path/to/config", config, dev_mode=False)

        self.assertEqual(client._HelpScoutClient__client_id, "test_id")
        self.assertEqual(client._HelpScoutClient__client_secret, "test_secret")
        self.assertEqual(client._HelpScoutClient__refresh_token, "test_token")
        self.assertEqual(client._HelpScoutClient__user_agent, "test_agent")
        self.assertEqual(client._HelpScoutClient__access_token, "existing_token")
        self.assertEqual(client.start_date, "2024-01-01")
        self.assertFalse(client._HelpScoutClient__dev_mode)

    def test_client_init_without_optional_keys(self):
        """Test client initialization without optional config keys."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
        }

        client = HelpScoutClient("/path/to/config", config)

        self.assertIsNone(client._HelpScoutClient__access_token)
        self.assertIsNone(client.start_date)

    def test_client_init_dev_mode(self):
        """Test client initialization in dev mode."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "existing_token",
        }

        client = HelpScoutClient("/path/to/config", config, dev_mode=True)
        self.assertTrue(client._HelpScoutClient__dev_mode)


class TestHelpScoutClientContextManager(unittest.TestCase):
    """Test HelpScoutClient context manager."""

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session")
    def test_client_context_manager_enter_exit(self, mock_session_class, mock_get_token):
        """Test context manager enters and exits correctly."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
        }

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        with HelpScoutClient("/path/to/config", config) as client:
            mock_get_token.assert_called_once()
            self.assertIsNotNone(client)

        # Session should be closed on exit
        mock_session.close.assert_called_once()

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session")
    def test_client_exit_with_exception(self, mock_session_class, mock_get_token):
        """Test context manager still closes on exception."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
        }

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        try:
            with HelpScoutClient("/path/to/config", config) as client:
                raise ValueError("Test error")
        except ValueError:
            pass

        mock_session.close.assert_called_once()


class TestHelpScoutClientTokenRefresh(unittest.TestCase):
    """Test HelpScoutClient token refresh scenarios."""

    @patch("tap_helpscout.client.requests.Session.post")
    def test_get_access_token_dev_mode_with_existing_token(self, mock_post):
        """Test get_access_token in dev mode with existing token doesn't make request."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "existing_token",
        }

        client = HelpScoutClient("/path/to/config", config, dev_mode=True)
        client.get_access_token()

        mock_post.assert_not_called()

    def test_get_access_token_dev_mode_without_token_raises(self):
        """Test get_access_token in dev mode without token raises AccessTokenMissing."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
        }

        client = HelpScoutClient("/path/to/config", config, dev_mode=True)

        with self.assertRaises(AccessTokenMissing):
            client.get_access_token()

    @patch("tap_helpscout.client.requests.Session.post")
    def test_get_access_token_with_valid_token_and_not_expired(self, mock_post):
        """Test get_access_token returns early if token not expired."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "valid_token",
        }

        client = HelpScoutClient("/path/to/config", config, dev_mode=False)
        # Set expires to future
        client._HelpScoutClient__expires = datetime.now(timezone.utc) + timedelta(hours=1)

        client.get_access_token()

        mock_post.assert_not_called()

    @patch("builtins.open", new_callable=mock_open)
    @patch("tap_helpscout.client.requests.Session.post")
    def test_get_access_token_refreshes_and_saves_config(self, mock_post, mock_file):
        """Test get_access_token refreshes token and saves to config file."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "old_token",
            "user_agent": "test_agent",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_response
        mock_file.return_value.read.return_value = json.dumps(config)

        client = HelpScoutClient("/path/to/config", config, dev_mode=False)
        client.get_access_token()

        self.assertEqual(client._HelpScoutClient__access_token, "new_access_token")
        self.assertEqual(client._HelpScoutClient__refresh_token, "new_refresh_token")

        # Verify config was saved
        mock_file.assert_called()

    @patch("builtins.open", new_callable=mock_open)
    @patch("tap_helpscout.client.requests.Session.post")
    def test_get_access_token_without_user_agent(self, mock_post, mock_file):
        """Test get_access_token without user_agent doesn't add header."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": None,
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_token",
            "refresh_token": "new_refresh",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_response
        mock_file.return_value.read.return_value = json.dumps(config)

        client = HelpScoutClient("/path/to/config", config, dev_mode=False)
        client.get_access_token()

        # Verify headers were not added
        call_args = mock_post.call_args
        self.assertNotIn("User-Agent", call_args.kwargs.get("headers", {}))

    @patch("tap_helpscout.client.requests.Session.post")
    def test_get_access_token_error_response(self, mock_post):
        """Test get_access_token raises error on 400+ status."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
        }

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_post.return_value = mock_response

        client = HelpScoutClient("/path/to/config", config, dev_mode=False)

        with patch("tap_helpscout.client.raise_for_error", side_effect=Http401Error()):
            with self.assertRaises(Http401Error):
                client.get_access_token()


class TestHelpScoutClientRequest(unittest.TestCase):
    """Test HelpScoutClient request method."""

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session.request")
    def test_request_successful_200_response(self, mock_session_request, mock_get_token):
        """Test successful request with 200 status."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "valid_token",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_session_request.return_value = mock_response

        with HelpScoutClient("/path/to/config", config) as client:
            result = client.request("GET", "/test")

        self.assertEqual(result, {"data": "test"})

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session.request")
    def test_request_builds_url_from_path(self, mock_session_request, mock_get_token):
        """Test request builds URL from base_url and path."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "valid_token",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_session_request.return_value = mock_response

        with HelpScoutClient("/path/to/config", config) as client:
            client.request("GET", "/customers")

        # Verify URL was built
        call_args = mock_session_request.call_args
        url = call_args[0][1] if len(call_args[0]) > 1 else call_args.kwargs.get("url")
        self.assertIn("https://api.helpscout.net/v2", url)

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session.request")
    def test_request_with_custom_url(self, mock_session_request, mock_get_token):
        """Test request with custom URL overrides path."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "valid_token",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_session_request.return_value = mock_response

        with HelpScoutClient("/path/to/config", config) as client:
            client.request("GET", "/path", url="https://custom.url/endpoint")

        call_args = mock_session_request.call_args
        url = call_args[0][1] if len(call_args[0]) > 1 else call_args.kwargs.get("url")
        self.assertEqual(url, "https://custom.url/endpoint")

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session.request")
    def test_request_adds_authorization_header(self, mock_session_request, mock_get_token):
        """Test request adds authorization header."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "my_token",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_session_request.return_value = mock_response

        with HelpScoutClient("/path/to/config", config) as client:
            client.request("GET", "/test")

        call_args = mock_session_request.call_args
        headers = call_args.kwargs.get("headers", {})
        self.assertEqual(headers.get("Authorization"), "Bearer my_token")

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session.request")
    def test_request_post_adds_content_type(self, mock_session_request, mock_get_token):
        """Test POST request adds Content-Type header."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "valid_token",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "created"}
        mock_session_request.return_value = mock_response

        with HelpScoutClient("/path/to/config", config) as client:
            client.request("POST", "/test", json={"key": "value"})

        call_args = mock_session_request.call_args
        headers = call_args.kwargs.get("headers", {})
        self.assertEqual(headers.get("Content-Type"), "application/json")

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session.request")
    def test_request_with_endpoint_param(self, mock_session_request, mock_get_token):
        """Test request extracts endpoint param."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "valid_token",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_session_request.return_value = mock_response

        with HelpScoutClient("/path/to/config", config) as client:
            # endpoint param should be removed and not passed to session.request
            client.request("GET", "/test", endpoint="customers")

        call_args = mock_session_request.call_args
        self.assertNotIn("endpoint", call_args.kwargs)

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session.request")
    def test_request_with_user_agent(self, mock_session_request, mock_get_token):
        """Test request adds user agent header."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "my-agent/1.0",
            "access_token": "valid_token",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_session_request.return_value = mock_response

        with HelpScoutClient("/path/to/config", config) as client:
            client.request("GET", "/test")

        call_args = mock_session_request.call_args
        headers = call_args.kwargs.get("headers", {})
        self.assertEqual(headers.get("User-Agent"), "my-agent/1.0")

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session.request")
    def test_request_error_response(self, mock_session_request, mock_get_token):
        """Test request raises error on non-200 status."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "valid_token",
        }

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_session_request.return_value = mock_response

        with patch("tap_helpscout.client.raise_for_error", side_effect=Http403Error()):
            with HelpScoutClient("/path/to/config", config) as client:
                with self.assertRaises(Http403Error):
                    client.request("GET", "/test")


class TestHelpScoutClientMethods(unittest.TestCase):
    """Test GET and POST convenience methods."""

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session.request")
    def test_get_method(self, mock_session_request, mock_get_token):
        """Test GET method."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "valid_token",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_session_request.return_value = mock_response

        with HelpScoutClient("/path/to/config", config) as client:
            result = client.get("/customers")

        self.assertEqual(result, {"data": "test"})

        call_args = mock_session_request.call_args
        self.assertEqual(call_args[0][0], "GET")

    @patch.object(HelpScoutClient, "get_access_token")
    @patch("tap_helpscout.client.requests.Session.request")
    def test_post_method(self, mock_session_request, mock_get_token):
        """Test POST method."""
        config = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "refresh_token": "test_token",
            "user_agent": "test_agent",
            "access_token": "valid_token",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "created"}
        mock_session_request.return_value = mock_response

        with HelpScoutClient("/path/to/config", config) as client:
            result = client.post("/test", json={"key": "value"})

        self.assertEqual(result, {"data": "created"})

        call_args = mock_session_request.call_args
        self.assertEqual(call_args[0][0], "POST")
