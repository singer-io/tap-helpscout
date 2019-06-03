#!/usr/bin/env python3

import os
import re
import sys
import json
import pprint
from datetime import datetime
import backoff
import requests
import pendulum
import singer
from singer import Transformer, utils
import pytz

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


def _make_token_request():
    response = requests.post(url=BASE_API_URL + 'oauth2/token',
                             data={'client_id': CONFIG['client_id'],
                                   'client_secret': CONFIG['client_secret'],
                                   'grant_type': 'client_credentials'},
                             headers={"User-Agent": CONFIG['user_agent']})
    return response

def _refresh_access_token():
    LOGGER.info("Refreshing access token")
    resp = _make_token_request()
    resp_json = {}
    try:
        resp_json = resp.json()
        expires_in_seconds = resp_json['expires_in'] - 120  # pad by 120 seconds
        _expires_at = pendulum.now().add(seconds=expires_in_seconds)
        _access_token = resp_json['access_token']
    except KeyError as key_err:
        if resp_json.get('error'):
            LOGGER.critical(resp_json.get('error'))
        if resp_json.get('error_description'):
            LOGGER.critical(resp_json.get('error_description'))
        raise key_err
    LOGGER.info("Got refreshed access token")
    return _access_token, _expires_at

def get_access_token(_access_token=None, _expires_at=None):
    if _access_token is not None and _expires_at is not None:
        if _expires_at > pendulum.now():
            return _access_token, _expires_at
    return _refresh_access_token()


def request(url, params=None):
    params = params or {}
    AUTH['access_token'], AUTH['expires_at'] = get_access_token(AUTH['access_token'], \
        AUTH['expires_at'])
    headers = {"Accept": "application/json",
               "Authorization": "Bearer " + AUTH['access_token'],
               "User-Agent": CONFIG.get("user_agent")}
    resp = requests.get(url=url, params=params, headers=headers)
    resp.raise_for_status()
    resp_json = resp.json()
    return resp_json


# Convert camelCase to snake_case
def convert(name):
    regsub = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', regsub).lower()


# Convert keys in json array
def convert_array(arr):
    new_arr = []
    for i in arr:
        if isinstance(i, list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr


# Convert keys in json
def convert_json(this_json):
    out = {}
    for key in this_json:
        new_key = convert(key)
        if isinstance(this_json[key], dict):
            out[new_key] = convert_json(this_json[key])
        elif isinstance(this_json[key], list):
            out[new_key] = convert_array(this_json[key])
        else:
            out[new_key] = this_json[key]
    return out


def denest_embedded_nodes(this_json, path=None):
    if path is None:
        return this_json
    i = 0
    nodes = ['attachments', "address", "chats", "emails", "phones", "social_profiles", "websites"]
    for record in this_json[path]:
        if "_embedded" in record:
            for node in nodes:
                if node in record['_embedded']:
                    this_json[path][i][node] = this_json[path][i]['_embedded'][node]
            
        i = i + 1
    return this_json


# Remove all _links and _embedded nodes from json
def remove_embedded_links(this_json):
    if not isinstance(this_json, (dict, list)):
        return this_json
    if isinstance(this_json, list):
        return [remove_embedded_links(vv) for vv in this_json]
    return {kk: remove_embedded_links(vv) for kk, vv in this_json.items()
            if kk not in {'_embedded', '_links'}}


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
            try:
                del item[field]
            except KeyError as key_err:
                LOGGER.info('Error: {}'.format(key_err))


def sync_endpoint(schema_name, endpoint=None, path=None, with_updated_since=False, #pylint: disable=too-many-arguments
                  bookmark_property=None, for_each_handler=None, map_handler=None): #pylint: disable=too-many-arguments
    LOGGER.info("Syncing: {}".format(schema_name))
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
        # pagination loop
        while page <= total_pages:
            url = get_url(endpoint or schema_name)
            LOGGER.info('URL: {}'.format(url))
            # some objects allow modifiedSince and sorting parameters
            if with_updated_since:
                params = {
                    "sortField": "modifiedAt",
                    "sortOrder": "asc",
                    "modifiedSince": updated_since
                    }
            # some objects do not allow parameters (except page)
            else:
                params = {}
            params['page'] = page
            response = request(url, params)
            path = path or schema_name
            data = {}
            if '_embedded' in response:
                # transform response: denest embedded nodes, remove _embedded and _links
                data = convert_json(remove_embedded_links(\
                    denest_embedded_nodes(response['_embedded'], path)))[path]
            for row in data:
                # map_handler allows for additional object transforms
                if map_handler is not None:
                    row = map_handler(row)
                # date-times = None cause problems for some targets; prune these nodes
                remove_empty_date_times(row, schema)
                item = transformer.transform(row, schema)
                if item.get(bookmark_property) is None:
                    singer.write_record(schema_name, item)
                    # if object has sub-objects, loop thru for each id
                    if for_each_handler is not None:
                        for_each_handler(row)
                # filter records where bookmark datetime after start
                elif datetime.strptime(item[bookmark_property], "%Y-%m-%dT%H:%M:%SZ") >=\
                    datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ"):
                    singer.write_record(schema_name, item)
                    # if object has sub-objects, loop thru for each id
                    if for_each_handler is not None:
                        for_each_handler(row)
                    utils.update_state(STATE, schema_name, item[bookmark_property])
            # set page and total_pages for pagination
            page = response['page']['number']
            total_pages = response['page']['totalPages']
            LOGGER.info("Sync page {} of {}".format(page, total_pages))
            if page == 0 or page > 100:
                break
            page = page + 1
    singer.write_state(STATE)


def sync_conversations():
    def for_each_conversation(conversation):
        def map_conversation_thread(thread):
            # add parent conversation_id to thread
            thread['conversation_id'] = conversation['id']
            return thread
        # Sync conversation threads
        sync_endpoint("conversation_threads",
                      endpoint=("conversations/{}/threads".format(conversation['id'])),
                      path="threads",
                      bookmark_property=None,
                      map_handler=map_conversation_thread)
    sync_endpoint("conversations",
                  with_updated_since=True,
                  bookmark_property="user_updated_at",
                  for_each_handler=for_each_conversation)


def sync_mailboxes():
    def for_each_mailbox(mailbox):
        def map_mailbox_field(field):
            field['mailbox_id'] = mailbox['id']
            return field
        def map_mailbox_folder(folder):
            folder['mailbox_id'] = mailbox['id']
            return folder
        # Sync mailbox fields
        sync_endpoint("mailbox_fields",
                      endpoint=("mailboxes/{}/fields".format(mailbox['id'])),
                      path="fields",
                      bookmark_property=None,
                      map_handler=map_mailbox_field)
        # Sync mailbox folders
        sync_endpoint("mailbox_folders",
                      endpoint=("mailboxes/{}/folders".format(mailbox['id'])),
                      path="folders",
                      bookmark_property="updated_at",
                      map_handler=map_mailbox_folder)
    sync_endpoint("mailboxes",
                  bookmark_property="updated_at",
                  for_each_handler=for_each_mailbox)


def do_sync():
    LOGGER.info("Starting sync")
    # Sync objects
    sync_endpoint("customers",
                  with_updated_since=True,
                  bookmark_property="updated_at")
    sync_endpoint("users",
                  bookmark_property="updated_at")
    sync_endpoint("workflows",
                  bookmark_property="modified_at")
    sync_mailboxes()
    sync_conversations()
    LOGGER.info("Sync complete")


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    AUTH['access_token'], AUTH['expires_at'] = get_access_token()
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
