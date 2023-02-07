import unittest
from unittest import mock

import requests

import tap_helpscout.client as client
from tap_helpscout import exceptions


class MockResponse:
    def __init__(self, status_code, json, raise_error, content=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = content

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text


def get_response(status_code, json=None, raise_error=False, content=None):
    if json is None:
        json = {}
    return MockResponse(status_code, json, raise_error, content=content)


def mock_config_params():
    return {
        "client_id": "client_id",
        "client_secret": "client_secret",
        "refresh_token": "refresh_token",
        "user_agent": "user_agent",
    }


@mock.patch("requests.Session.request")
@mock.patch("tap_helpscout.client.HelpScoutClient.get_access_token")
class TestExceptionHandling(unittest.TestCase):
    def test_400_error_response(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(400, raise_error=True, content="")
        hs_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http400Error) as err:
            hs_client.request("GET", "/customers")
        self.assertEqual(
            str(err.exception), "Bad Request. Client error - the request doesn't " "meet all requirements."
        )

    def test_401_error_response(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(401, raise_error=True, content="")
        hs_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http401Error) as err:
            hs_client.request("GET", "/customers")
        self.assertEqual(str(err.exception), "Not Authorized. OAuth2 token is either not provided " "or not valid.")

    def test_403_error_response(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(403, raise_error=True, content="")
        hs_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http403Error) as err:
            hs_client.request("GET", "/customers")
        self.assertEqual(
            str(err.exception),
            "Access denied. Your OAuth2 token is valid, but you are"
            " denied access - the response should contain details.",
        )

    def test_404_error_response(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(404, raise_error=True, content="")
        hs_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http404Error) as err:
            hs_client.request("GET", "/customers")
        self.assertEqual(
            str(err.exception), "Not Found. Resource was not found - " "it doesn't exist or it was deleted."
        )

    def test_409_error_response(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(409, raise_error=True, content="")
        hs_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http409Error) as err:
            hs_client.request("GET", "/customers")
        self.assertEqual(
            str(err.exception), "Conflict. Resource cannot be created because" " conflicting entity already exists."
        )

    def test_412_error_response(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(412, raise_error=True, content="")
        hp_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http412Error) as err:
            hp_client.request("GET", "/customers")
        self.assertEqual(
            str(err.exception),
            "Precondition failed. The request was well " "formed and " "valid, but some other conditions were not met.",
        )

    def test_413_error_response(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(413, raise_error=True, content="")
        hp_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http413Error) as err:
            hp_client.request("GET", "/customers")
        self.assertEqual(
            str(err.exception),
            "Payload Too Large. The request was well formed and " "valid, but some other conditions were not met.",
        )

    def test_415_error_response(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(415, raise_error=True, content="")
        hp_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http415Error) as err:
            hp_client.request("GET", "/customers")
        self.assertEqual(
            str(err.exception), "Unsupported Media Type. The API is unable to work" " with the provided payload."
        )

    @mock.patch("time.sleep")
    def test_429_error_response(self, mocked_sleep, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(429, raise_error=True, content="")
        hp_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http429Error) as err:
            hp_client.request("GET", "/customers")
        self.assertEqual(
            str(err.exception), "Too Many Requests. You reached the rate limit," " Please retry after sometime."
        )
        self.assertEqual(mocked_request.call_count, 7)

    @mock.patch("time.sleep")
    def test_500_error_response(self, mocked_sleep, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(500, raise_error=True, content="")
        hp_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http500Error) as err:
            hp_client.request("GET", "/customers")
        self.assertEqual(str(err.exception), "Internal Server Error.")
        self.assertEqual(mocked_request.call_count, 7)

    @mock.patch("time.sleep")
    def test_503_error_response(self, mocked_sleep, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(503, raise_error=True, content="")
        hp_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http503Error) as err:
            hp_client.request("GET", "/customers")
        self.assertEqual(
            str(err.exception), "Service Unavailable. The API cannot process the" " request at the moment."
        )
        self.assertEqual(mocked_request.call_count, 7)

    @mock.patch("time.sleep")
    def test_504_error_response(self, mocked_sleep, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(504, raise_error=True, content="")
        hp_client = client.HelpScoutClient("", mock_config_params(), dev_mode=False)
        with self.assertRaises(exceptions.Http504Error) as err:
            hp_client.request("GET", "/customers")
        self.assertEqual(
            str(err.exception),
            "Gateway Timeout. An internal call timed-out and the" " API was not able to finish your request.",
        )
        self.assertEqual(mocked_request.call_count, 7)
