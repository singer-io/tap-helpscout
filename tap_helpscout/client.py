import json
from datetime import datetime, timedelta
import backoff
import requests
from singer import metrics
from singer import utils

class Server5xxError(Exception):
    pass

class Server429Error(Exception):
    pass

class HelpScoutClient:

    # pylint: disable=too-many-instance-attributes
    # Nine is reasonable in this case.

    def __init__(self,
                 config_path,
                 client_id,
                 client_secret,
                 refresh_token,
                 user_agent):
        self.__config_path = config_path
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__refresh_token = refresh_token
        self.__user_agent = user_agent
        self.__access_token = None
        self.__expires = None
        self.__session = requests.Session()
        self.__base_url = None

    def __enter__(self):
        self.get_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def get_access_token(self):
        if self.__access_token is not None and self.__expires > datetime.utcnow():
            return

        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent

        response = self.__session.post(
            url='https://api.helpscout.net/v2/oauth2/token',
            headers=headers,
            data={
                'grant_type': 'refresh_token',
                'client_id': self.__client_id,
                'client_secret': self.__client_secret,
                'refresh_token': self.__refresh_token,
            })

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            helpscout_response = response.json()
            helpscout_response.update(
                {'status': response.status_code})
            raise Exception(
                'Unable to authenticate (HelpScout response: `{}`)'.format(
                    helpscout_response))

        data = response.json()

        self.__access_token = data['access_token']
        self.__refresh_token = data['refresh_token']

        ## refresh_token rotates on every reauth
        with open(self.__config_path) as file:
            config = json.load(file)
        config['refresh_token'] = data['refresh_token']
        with open(self.__config_path, 'w') as file:
            json.dump(config, file, indent=2)

        expires_seconds = data['expires_in'] - 60 # pad by 60 seconds
        self.__expires = datetime.utcnow() + timedelta(seconds=expires_seconds)


    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    @utils.ratelimit(400, 60)
    def request(self, method, path=None, url=None, **kwargs):

        self.get_access_token()

        if not url and self.__base_url is None:
            self.__base_url = 'https://api.helpscout.net/v2'

        if not url and path:
            url = self.__base_url + path

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        #Use retry functionality in backoff to wait and retry if
        #response code equals 429 because rate limit has been exceeded
        if response.status_code == 429:
            raise Server429Error()

        response.raise_for_status()

        return response.json()

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)
