import re
import json
import time
import random
import tarfile
from datetime import datetime, timedelta

import requests
import singer
from singer import metrics, metadata, Transformer

LOGGER = singer.get_logger()

MIN_RETRY_INTERVAL = 2 # 2 seconds
MAX_RETRY_INTERVAL = 300 # 5 minutes
MAX_RETRY_ELAPSED_TIME = 3600 # 1 hour

def next_sleep_interval(previous_sleep_interval):
    min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
    max_interval = previous_sleep_interval * 2 or MIN_RETRY_INTERVAL
    return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))

def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    singer.write_schema(stream_name, schema, stream.key_properties)

def process_records(catalog,
                    stream_name,
                    records,
                    persist=True,
                    bookmark_field=None,
                    max_bookmark_field=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)
    with metrics.record_counter(stream_name) as counter:
        for record in records:
            if bookmark_field:
                if max_bookmark_field is None or \
                    record[bookmark_field] > max_bookmark_field:
                    max_bookmark_field = record[bookmark_field]
            if persist:
                with Transformer() as transformer:
                    record = transformer.transform(record,
                                                   schema,
                                                   stream_metadata)
                singer.write_record(stream_name, record)
                counter.increment()
        return max_bookmark_field

def get_bookmark(state, path, default):
    dic = state
    for key in (['bookmarks'] + path):
        if key in dic:
            dic = dic[key]
        else:
            return default
    return dic

def nested_set(dic, path, value):
    for key in path[:-1]:
        dic = dic.setdefault(key, {})
    dic[path[-1]] = value

def write_bookmark(state, path, value):
    nested_set(state, ['bookmarks'] + path, value)
    singer.write_state(state)

def sync_endpoint(client,
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  persist,
                  path,
                  data_key,
                  static_params,
                  bookmark_path,
                  bookmark_query_field,
                  bookmark_field):
    bookmark_path = bookmark_path + ['datetime']
    last_datetime = get_bookmark(state, bookmark_path, start_date)
    ids = []
    max_bookmark_field = last_datetime

    def transform(record):
        _id = record.get('id')
        if _id:
            ids.append(_id)
        del record['_links']
        return record

    write_schema(catalog, stream_name)

    count = 1000
    offset = 0
    has_more = True
    while has_more:
        params = {
            'count': count,
            'offset': offset,
            **static_params
        }

        if bookmark_query_field:
            params[bookmark_query_field] = last_datetime

        LOGGER.info('{} - Syncing - {}count: {}, offset: {}'.format(
            stream_name,
            'since: {}, '.format(last_datetime) if bookmark_query_field else '',
            count,
            offset))

        data = client.get(
            path,
            params=params,
            endpoint=stream_name)

        raw_records = data.get(data_key)

        if len(raw_records) < count:
            has_more = False

        max_bookmark_field = process_records(catalog,
                                             stream_name,
                                             map(transform, raw_records),
                                             persist=persist,
                                             bookmark_field=bookmark_field,
                                             max_bookmark_field=max_bookmark_field)

        if bookmark_field:
            write_bookmark(state,
                           bookmark_path,
                           max_bookmark_field)

        offset += count

    return ids

def get_dependants(endpoint_config):
    dependants = endpoint_config.get('dependants', [])
    for stream_name, child_endpoint_config in endpoint_config.get('children', {}).items():
        dependants.append(stream_name)
        dependants += get_dependants(child_endpoint_config)
    return dependants

def sync_stream(client,
                catalog,
                state,
                start_date,
                streams_to_sync,
                id_bag,
                stream_name,
                endpoint_config,
                bookmark_path=None,
                id_path=None):
    if not bookmark_path:
        bookmark_path = [stream_name]
    if not id_path:
        id_path = []

    dependants = get_dependants(endpoint_config)
    should_stream, should_persist = should_sync_stream(streams_to_sync,
                                                       dependants,
                                                       stream_name)
    if should_stream:
        path = endpoint_config.get('path').format(*id_path)
        stream_ids = sync_endpoint(client,
                                   catalog,
                                   state,
                                   start_date,
                                   stream_name,
                                   should_persist,
                                   path,
                                   endpoint_config.get('data_path', stream_name),
                                   endpoint_config.get('params', {}),
                                   bookmark_path,
                                   endpoint_config.get('bookmark_query_field'),
                                   endpoint_config.get('bookmark_field'))

        if endpoint_config.get('store_ids'):
            id_bag[stream_name] = stream_ids
        
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                for _id in stream_ids:
                    sync_stream(client,
                                catalog,
                                state,
                                start_date,
                                streams_to_sync,
                                id_bag,
                                child_stream_name,
                                child_endpoint_config,
                                bookmark_path=bookmark_path + [_id, child_stream_name],
                                id_path=id_path + [_id])


def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)

def should_sync_stream(streams_to_sync, dependants, stream_name):
    selected_streams = streams_to_sync['selected_streams']
    should_persist = stream_name in selected_streams
    last_stream = streams_to_sync['last_stream']
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            streams_to_sync['last_stream'] = None
            return True, should_persist
        if should_persist or set(dependants).intersection(selected_streams):
            return True, should_persist
    return False, should_persist

def sync(client, catalog, state, start_date):
    streams_to_sync = {
        'selected_streams': get_selected_streams(catalog),
        'last_stream': state.get('current_stream')
    }

    if not streams_to_sync['selected_streams']:
        return

    id_bag = {}

    endpoints = {
        'conversations': {
            'path': '/conversations',
            'params': {
                'sortField': 'modifiedAt',
                'sortOrder': 'asc'
            },
            'data_path': 'conversations',
            'bookmark_query_field': 'modifiedSince',
            'bookmark_field': 'user_updated_at',
            'children': {
               'conversation_threads': {
                    'path': '/conversations/{}/threads',
                    'data_path': 'threads',
                    'bookmark_field': 'created_at'
                }
            }
        },
        
        'customers': {
            'path': '/customers',
            'params': {
                'sortField': 'modifiedAt',
                'sortOrder': 'asc'
            },
            'data_path': 'customers',
            'bookmark_query_field': 'modifiedSince',
            'bookmark_field': 'updated_at'
        },
        
        'mailboxes': {
            'path': '/mailboxes',
            'data_path': 'mailboxes',
            'bookmark_field': 'updated_at',
            'children': {
               'mailbox_fields': {
                    'path': '/mailboxes/{}/fields',
                    'data_path': 'fields'
                },
                'mailbox_folders': {
                    'path': '/mailboxes/{}/folders',
                    'data_path': 'folders',
                    'bookmark_field': 'updated_at'
                }
            }
        },
        
        'users': {
            'path': '/users',
            'data_path': 'users',
            'bookmark_field': 'updated_at'
        },
        
        'workflows': {
            'path': '/workflows',
            'data_path': 'workflows',
            'bookmark_field': 'modified_at'
        }
    }

    for stream_name, endpoint_config in endpoints.items():
        sync_stream(client,
                    catalog,
                    state,
                    start_date,
                    streams_to_sync,
                    id_bag,
                    stream_name,
                    endpoint_config)


