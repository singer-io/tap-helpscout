#!/usr/bin/env python3

import sys
import json
import argparse

import singer
from singer import metadata
from tap_helpscout.client import HelpScoutClient
from tap_helpscout.discover import discover
from tap_helpscout.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'client_id',
    'client_secret',
    'refresh_token',
    'user_agent'
]

def do_discover(client):

<<<<<<< HEAD
    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')
=======
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
            thread['conversation_user_updated_at'] = conversation['user_updated_at']
            return thread
        # Sync conversation threads
        sync_endpoint("conversation_threads",
                      endpoint=("conversations/{}/threads".format(conversation['id'])),
                      path="threads",
                      bookmark_property="conversation_user_updated_at",
                      map_handler=map_conversation_thread)
    sync_endpoint("conversations",
                  with_updated_since=True,
                  bookmark_property="user_updated_at",
                  for_each_handler=for_each_conversation)


def sync_mailboxes():
    def for_each_mailbox(mailbox):
        def map_mailbox_field(field):
            field['mailbox_id'] = mailbox['id']
            field['mailbox_updated_at'] = mailbox['updated_at']
            return field
        def map_mailbox_folder(folder):
            folder['mailbox_id'] = mailbox['id']
            folder['mailbox_updated_at'] = mailbox['updated_at']
            return folder
        # Sync mailbox fields
        sync_endpoint("mailbox_fields",
                      endpoint=("mailboxes/{}/fields".format(mailbox['id'])),
                      path="fields",
                      bookmark_property="mailbox_updated_at",
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

>>>>>>> master

@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with HelpScoutClient(parsed_args.config_path,
                      parsed_args.config['client_id'],
                      parsed_args.config['client_secret'],
                      parsed_args.config['refresh_token'],
                      parsed_args.config['user_agent']) as client:
        
        if parsed_args.discover:
            do_discover(client)
        elif parsed_args.catalog:
            sync(client,
                 parsed_args.catalog,
                 parsed_args.state,
                 parsed_args.config['start_date'])

if __name__ == '__main__':
    main()
