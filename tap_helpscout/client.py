import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping

import backoff
import requests

from singer import metrics
from . import exceptions as errors


def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception.

    Takes in a response object, checks the status code, and throws the
    associated exception based on the status code.
    """
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as _:
        try:
            error_code = response.status_code
            client_exception = getattr(
                errors, f"Http{error_code}Error", errors.HttpClientException(message="Undefined " "Exception")
            )
            raise client_exception from None
        except (ValueError, TypeError, AttributeError):
            raise errors.HttpClientException(_) from None


class HelpScoutClient:
    def __init__(self, config_path: str, config: Dict, dev_mode: bool = False):
        self.__config_path = config_path
        self.__client_id = config["client_id"]
        self.__client_secret = config["client_secret"]
        self.__refresh_token = config["refresh_token"]
        self.__user_agent = config["user_agent"]
        self.__access_token = config.get("access_token")
        self.__dev_mode = dev_mode
        # This is to make sure a new access token gets generated on every extraction
        self.__expires = datetime.now(timezone.utc) - timedelta(seconds=10)
        self.__session = requests.Session()
        self.__base_url = None

    def __enter__(self):
        self.get_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(errors.Http500Error, errors.Http503Error, errors.Http504Error, errors.Http429Error),
        max_tries=7,
        factor=3,
    )
    def get_access_token(self):
        """Generates access token required to send http requests."""
        # If tap is being executed in dev_mode then disable tap from creating new refresh and
        # access tokens
        if self.__dev_mode:
            if self.__access_token:
                return

            raise errors.AccessTokenMissing

        # Return nothing, If a valid access token is already available in the existing client object
        if self.__access_token and self.__expires > datetime.now(timezone.utc):
            return

        headers = {}
        if self.__user_agent:
            headers["User-Agent"] = self.__user_agent

        response = self.__session.post(
            url="https://api.helpscout.net/v2/oauth2/token",
            headers=headers,
            data={
                "grant_type": "refresh_token",
                "client_id": self.__client_id,
                "client_secret": self.__client_secret,
                "refresh_token": self.__refresh_token,
            },
        )

        if response.status_code >= 400:
            raise_for_error(response)

        data = response.json()

        self.__access_token = data["access_token"]
        self.__refresh_token = data["refresh_token"]

        # Refresh token rotates on every re-auth
        with open(self.__config_path) as file:
            config = json.load(file)
        config["refresh_token"] = self.__refresh_token
        config["access_token"] = self.__access_token
        with open(self.__config_path, "w") as file:
            json.dump(config, file, indent=2)

        expires_seconds = data["expires_in"] - 60  # pad by 60 seconds
        self.__expires = datetime.now(timezone.utc) + timedelta(seconds=expires_seconds)

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(errors.Http500Error, errors.Http503Error, errors.Http504Error, errors.Http429Error),
        max_tries=7,
        factor=3,
    )
    def request(self, method: str, path: str, url: str = "", **kwargs) -> Mapping[Any, Any]:
        """Makes an HTTP Request based on given params.

        Args:
            method (str): Http method
            path (str): endpoint for Http request
            url (str): Base url for Http request

        Returns:
            Returns a json object for a successful http request
        """
        self.get_access_token()

        if not url and self.__base_url is None:
            self.__base_url = "https://api.helpscout.net/v2"

        if not url and path:
            url = self.__base_url + path

        if "endpoint" in kwargs:
            endpoint = kwargs["endpoint"]
            del kwargs["endpoint"]
        else:
            endpoint = None

        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]["Authorization"] = f"Bearer {self.__access_token}"

        if self.__user_agent:
            kwargs["headers"]["User-Agent"] = self.__user_agent

        if method == "POST":
            kwargs["headers"]["Content-Type"] = "application/json"

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code == 200:
            return response.json()

        raise_for_error(response)

    def get(self, path: str, **kwargs):
        """Initiates HTTP requests for GET method."""
        return self.request("GET", path=path, **kwargs)

    def post(self, path: str, **kwargs):
        """Initiates HTTP requests for POST method."""
        return self.request("POST", path=path, **kwargs)
