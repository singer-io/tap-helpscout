#!/usr/bin/env python3

import asyncio
import os
import re
import json
import backoff
import requests
import pendulum
import singer
from singer import Transformer, utils

LOGGER = singer.get_logger()
SESSION = requests.Session()
REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "user_agent",
]

BASE_API_URL = "https://api.helpscout.net/v2/"
CONFIG = {}
STATE = {}
AUTH = {}


class Auth:
    def __init__(self, client_id, client_secret):
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_access_token()


    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
        factor=2)
    def _make_token_request(self):
        return requests.request('POST',
                                url=BASE_API_URL + 'oauth2/token',
                                data={
                                    'client_id': self._client_id,
                                    'client_secret': self._client_secret,
                                    'grant_type': 'client_credentials'
                                },
                                headers={"User-Agent": CONFIG.get("user_agent")})

    def _refresh_access_token(self):
        LOGGER.info("Refreshing access token")
        
        resp = self._make_token_request()
        resp_json = {}
        try:
            resp_json = resp.json()
            expires_in_seconds = resp_json['expires_in'] - 120  # pad by 120 seconds
            self._expires_at = pendulum.now().add(seconds=expires_in_seconds)
            
            self._access_token = resp_json['access_token']

        except KeyError as key_err:
            if resp_json.get('error'):
                LOGGER.critical(resp_json.get('error'))
            if resp_json.get('error_description'):
                LOGGER.critical(resp_json.get('error_description'))
            raise key_err
        LOGGER.info("Got refreshed access token")

    def get_access_token(self):
        if self._access_token is not None and self._expires_at > pendulum.now():
            return self._access_token

        self._refresh_access_token()
        return self._access_token


# Remove all _links nodes from json
def remove_links(d):
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [remove_links(v) for v in d]
    return {k: remove_links(v) for k, v in d.items()
            if k not in {'_links'}}


# Removed unwanted _embedded nodes
def remove_embedded(d):
    if '_embedded' in d:
        if 'emails' in d['_embedded']: del d['_embedded']['emails']
        if 'websites' in d['_embedded']: del d['_embedded']['websites']
        if 'chats' in d['_embedded']: del d['_embedded']['chats']
        if 'phones' in d['_embedded']: del d['_embedded']['phones']
        if 'social_profiles' in d['_embedded']: del d['_embedded']['social_profiles']
    return d

# Convert camelCase to snake_case
# Reference: https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
def convert(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


# Convert keys in json array
def convert_array(a):
    new_arr = []
    for i in a:
        if isinstance(i,list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr


# Convert keys in json
def convert_json(j):
    out = {}
    for k in j:
        new_k = convert(k)
        if isinstance(j[k],dict):
            out[new_k] = convert_json(j[k])
        elif isinstance(j[k],list):
            out[new_k] = convert_array(j[k])
        else:
            out[new_k] = j[k]
    return out

# Flatten nested dict elements of json
# Reference: https://stackoverflow.com/questions/51359783/python-flatten-multilevel-json
def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict: # flatten dict elements
            for a in x:
                flatten(x[a], name.replace('_embedded_','') + a + '_')
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


def get_abs_path(path):
    return os.path.join(str(os.path.dirname(os.path.realpath(__file__))), str(path))


def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def load_and_write_schema(name, key_properties='id', bookmark_property='modified_at'):
    schema = load_schema(name)
    singer.write_schema(name, schema, key_properties, bookmark_properties=[bookmark_property])
    return schema


def get_start(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return STATE[key]


def get_url(endpoint):
    return BASE_API_URL + endpoint


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=5,
    giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
    factor=2)
@utils.ratelimit(100, 15)
def request(url, params=None):
    params = params or {}
    access_token = AUTH.get_access_token()
    headers = {"Accept": "application/json",
               "Authorization": "Bearer " + access_token,
               "User-Agent": CONFIG.get("user_agent")}
    req = requests.Request("GET", url=url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)
    resp.raise_for_status()
    return resp.json()


# Any date-times values can either be a string or a null.
# If null, parsing the date results in an error.
# Instead, removing the attribute before parsing ignores this error.
def remove_empty_date_times(item, schema):
    fields = []
    for key in schema['properties']:
        subschema = schema['properties'][key]
        if subschema.get('format') == 'date-time':
            fields.append(key)
    for field in fields:
        if item.get(field) is None:
            del item[field]


def sync_endpoint(schema_name, endpoint=None, path=None, with_updated_since=False, bookmark_property=None, for_each_handler=None, map_handler=None):
    schema = load_schema(schema_name)

    singer.write_schema(schema_name,
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start(schema_name)
    start_dt = pendulum.parse(start)
    updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    with Transformer() as transformer:
        page = 1
        total_pages = 1  # initial value, set with first API call
        while page <= total_pages:
            url = get_url(endpoint or schema_name)
            if with_updated_since:
                params =    {
                                "sortField": "modifiedAt", 
                                "sortOrder": "asc",
                                "modifiedSince": updated_since
                            }
            else:
                params = {}
            params['page'] = page
            response = request(url, params)
            path = path or schema_name
            data = flatten_json(convert_json(remove_embedded(remove_links(response['_embedded'][path]))))
            time_extracted = utils.now()

            for row in data:
                if map_handler is not None:
                    row = map_handler(row)

                remove_empty_date_times(row, schema)

                item = transformer.transform(row, schema)

                if item[bookmark_property] >= start:
                    singer.write_record(schema_name,
                                        item,
                                        time_extracted=time_extracted)

                    # take any additional actions required for the currently loaded endpoint
                    if for_each_handler is not None:
                        for_each_handler(row, time_extracted=time_extracted)

                    utils.update_state(STATE, schema_name, item[bookmark_property])
            page = response['page']['number'] + 1
            total_pages = response['page']['totalPages']

    singer.write_state(STATE)


def sync_conversations():
    def for_each_conversation(conversation, time_extracted):
        def map_conversation_thread(thread):
            thread['conversation_id'] = conversation['id']
            return thread

        # Sync conversation threads
        sync_endpoint(schema_name="conversation_threads", endpoint=("conversations/{}/threads".format(conversation['id'])), \
            path="threads", map_handler=map_conversation_thread)
                      
    sync_endpoint(schema_name="conversations", with_updated_since=True, bookmark_property="user_updated_at", \
        for_each_handler=for_each_conversation)


def sync_mailboxes():
    def for_each_mailbox(mailbox, time_extracted):
        def map_mailbox_field(field):
            field['mailbox_id'] = mailbox['id']
            return field

        def map_mailbox_folder(folder):
            folder['mailbox_id'] = mailbox['id']
            return folder

        # Sync mailbox fields
        sync_endpoint(schema_name="mailbox_fields", endpoint=("mailboxes/{}/fields".format(mailbox['id'])), path="fields", \
            map_handler=map_mailbox_field)
        
        # Sync mailbox folders
        sync_endpoint(schema_name="mailbox_folders", endpoint=("mailboxes/{}/folders".format(mailbox['id'])), path="folders", \
            bookmark_property="updated_at", map_handler=map_mailbox_folder)

    sync_endpoint(schema_name="mailboxes", bookmark_property="updated_at", for_each_handler=for_each_mailbox)


def do_sync():
    LOGGER.info("Starting sync")

    # Sync objects
    sync_endpoint(schema_name="customers", with_updated_since=True, bookmark_property="updated_at")
    sync_endpoint(schema_name="users", bookmark_property="updated_at")
    sync_endpoint(schema_name="workflows", bookmark_property="modifed_at")
    sync_mailboxes()
    sync_conversations()

    LOGGER.info("Sync complete")


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    global AUTH  # pylint: disable=global-statement

    config_path = get_abs_path(args.config)
    AUTH = Auth(CONFIG['client_id'], CONFIG['client_secret'])
    STATE.update(args.state)
    do_sync()


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
