import json
import os
import unittest
from unittest import mock

from tap_helpscout.client import HelpScoutClient


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


def get_mocked_response():
    return MockResponse(
        {"access_token": "new_access_token", "refresh_token": "new_refresh_token", "expires_in": 27800}, status_code=200
    )


class TestDevModeImplementation(unittest.TestCase):
    def setUp(self):
        """Creates a sample config params and sample config file."""
        self.config_params = {
            "client_id": "client_id",
            "client_secret": "client_secret",
            "refresh_token": "old_refresh_token",
            "user_agent": "user_agent",
            "access_token": "old_access_token",
        }
        self.invalid_access_token_params = self.config_params.copy()
        self.invalid_access_token_params["access_token"] = None
        self.config_file_name = "sample_config_file.json"

        # Serializing json
        json_object = json.dumps(self.config_params, indent=4)
        # Writing to sample_config_file.json
        with open(self.config_file_name, "w") as outfile:
            outfile.write(json_object)

    def tearDown(self):
        """Deletes the sample config."""
        if os.path.isfile(self.config_file_name):
            os.remove(self.config_file_name)

    def test_dev_mode_with_valid_access_token(self):
        """Verifies whether the __access_token attr has value from config in
        dev_mode implementation."""
        helpscout_client_object = HelpScoutClient(self.config_file_name, self.config_params, True)
        helpscout_client_object.get_access_token()
        self.assertEqual("old_access_token", helpscout_client_object._HelpScoutClient__access_token)

    def test_dev_mode_with_invalid_access_token(self):
        """Verifies whether the get_access_token method throws an exception
        when access_token is None in config."""
        with self.assertRaises(Exception) as err:
            HelpScoutClient(self.config_file_name, self.invalid_access_token_params, True)
            self.assertEqual(str(err), "Access token is missing, unable to authenticate in dev mode")
