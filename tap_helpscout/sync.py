import re
import json
import time
import random
import tarfile
from datetime import datetime, timedelta

import requests
import singer
from singer import metrics, metadata, Transformer
from tap_helpscout.transform import transform_json

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    singer.write_schema(stream_name, schema, stream.key_properties)

def process_records(catalog,
                    stream_name,
                    records,
                    persist=True,
                    bookmark_field=None,
                    max_bookmark_field=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)
    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id
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
                  bookmark_field,
                  parent=None,
                  parent_id=None):
    bookmark_path = bookmark_path + ['datetime']
    last_datetime = get_bookmark(state, bookmark_path, start_date)
    ids = []
    max_bookmark_field = last_datetime

    def transform(record):
        _id = record.get('id')
        if _id:
            ids.append(_id)
        return record

    write_schema(catalog, stream_name)

    # pagination: loop thru all pages of data
    page = 1
    total_pages = 1  # initial value, set with first API call
    while page <= total_pages:
        params = {
            'page': page,
            **static_params
        }

        if bookmark_query_field:
            params[bookmark_query_field] = last_datetime

        LOGGER.info('{} - Sync start'.format(
            stream_name,
            'since: {}, '.format(last_datetime) if bookmark_query_field else ''))

        data = client.get(
            path,
            params=params,
            endpoint=stream_name)

        raw_records = []
        if '_embedded' in data:
            raw_records = transform_json(data["_embedded"], data_key)[data_key]

        max_bookmark_field = process_records(catalog=catalog,
                                             stream_name=stream_name,
                                             records=map(transform, raw_records),
                                             persist=persist,
                                             bookmark_field=bookmark_field,
                                             max_bookmark_field=max_bookmark_field,
                                             parent=parent,
                                             parent_id=parent_id)

        if bookmark_field:
            write_bookmark(state,
                           bookmark_path,
                           max_bookmark_field)

        # set page and total_pages for pagination
        page = data['page']['number']
        total_pages = data['page']['totalPages']
        LOGGER.info('{} - Synced - page: {}, total pages: {}'.format(
            stream_name,
            page,
            total_pages))
        if page == 0 or page > 100:
            break
        page = page + 1

    return ids

def get_dependents(endpoint_config):
    dependents = endpoint_config.get('dependents', [])
    for stream_name, child_endpoint_config in endpoint_config.get('children', {}).items():
        dependents.append(stream_name)
        dependents += get_dependents(child_endpoint_config)
    return dependents

def sync_stream(client,
                catalog,
                state,
                start_date,
                streams_to_sync,
                id_bag,
                stream_name,
                endpoint_config,
                bookmark_path=None,
                id_path=None,
                parent=None,
                parent_id=None):
    if not bookmark_path:
        bookmark_path = [stream_name]
    if not id_path:
        id_path = []

    dependents = get_dependents(endpoint_config)
    should_stream, should_persist = should_sync_stream(streams_to_sync,
                                                       dependents,
                                                       stream_name)
    if should_stream:
        path = endpoint_config.get('path').format(*id_path)
        stream_ids = sync_endpoint(client=client,
                                   catalog=catalog,
                                   state=state,
                                   start_date=start_date,
                                   stream_name=stream_name,
                                   persist=should_persist,
                                   path=path,
                                   data_key=endpoint_config.get('data_path', stream_name),
                                   static_params=endpoint_config.get('params', {}),
                                   bookmark_path=bookmark_path,
                                   bookmark_query_field=endpoint_config.get('bookmark_query_field'),
                                   bookmark_field=endpoint_config.get('bookmark_field'),
                                   parent=endpoint_config.get('parent'),
                                   parent_id=parent_id)

        if endpoint_config.get('store_ids'):
            id_bag[stream_name] = stream_ids
        
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                for _id in stream_ids:
                    sync_stream(client=client,
                                catalog=catalog,
                                state=state,
                                start_date=start_date,
                                streams_to_sync=streams_to_sync,
                                id_bag=id_bag,
                                stream_name=child_stream_name,
                                endpoint_config=child_endpoint_config,
                                bookmark_path=bookmark_path + [_id, child_stream_name],
                                id_path=id_path + [_id],
                                parent=child_endpoint_config.get('parent'),
                                parent_id=_id)


def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)

def should_sync_stream(streams_to_sync, dependents, stream_name):
    selected_streams = streams_to_sync['selected_streams']
    should_persist = stream_name in selected_streams
    last_stream = streams_to_sync['last_stream']
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            streams_to_sync['last_stream'] = None
            return True, should_persist
        if should_persist or set(dependents).intersection(selected_streams):
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
            'store_ids': True,
            'children': {
               'conversation_threads': {
                    'path': '/conversations/{}/threads',
                    'data_path': 'threads',
                    'bookmark_field': 'created_at',
                    'parent': 'conversation'
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
            'store_ids': True,
            'children': {
               'mailbox_fields': {
                    'path': '/mailboxes/{}/fields',
                    'data_path': 'fields',
                    'parent': 'mailbox'
                },
                'mailbox_folders': {
                    'path': '/mailboxes/{}/folders',
                    'data_path': 'folders',
                    'bookmark_field': 'updated_at',
                    'parent': 'mailbox'
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
