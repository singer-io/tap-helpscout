from datetime import datetime
import singer
from singer import metrics, metadata, Transformer, utils
from tap_helpscout.transform import transform_json

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    singer.write_schema(stream_name, schema, stream.key_properties)


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)


def process_records(catalog,
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    bookmark_type=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    last_integer=None,
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

            # Reset max_bookmark_value to new value if higher
            if bookmark_field and (bookmark_field in record):
                if (max_bookmark_value is None) or \
                    (record[bookmark_field] > max_bookmark_value):
                    max_bookmark_value = record[bookmark_field]

            # Transform record for Singer.io
            with Transformer() as transformer:
                record = transformer.transform(record,
                                               schema,
                                               stream_metadata)

            if bookmark_field and (bookmark_field in record):
                if bookmark_type == 'integer':
                    # Keep only records whose bookmark is after the last_integer
                    if record[bookmark_field] >= last_integer:
                        singer.write_record(stream_name, record, time_extracted=time_extracted)
                        counter.increment()
                elif bookmark_type == 'datetime':
                    # Keep only records whose bookmark is after the last_datetime
                    if datetime.strptime(record[bookmark_field], "%Y-%m-%dT%H:%M:%S.%fZ") >= \
                        datetime.strptime(last_datetime, "%Y-%m-%dT%H:%M:%SZ"):
                        singer.write_record(stream_name, record, time_extracted=time_extracted)
                        counter.increment()
            else:
                singer.write_record(stream_name, record, time_extracted=time_extracted)
                counter.increment()
        return max_bookmark_value, counter.value


# Sync a specific parent or child endpoint.
def sync_endpoint(client,
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  path,
                  endpoint_config,
                  data_key,
                  static_params,
                  bookmark_query_field=None,
                  bookmark_field=None,
                  bookmark_type=None,
                  id_field=None,
                  parent=None,
                  parent_id=None):

    total_records = 0
    # Get the latest bookmark for the stream and set the last_integer/datetime
    last_datetime = None
    last_integer = None
    max_bookmark_value = None
    if bookmark_type == 'integer':
        last_integer = get_bookmark(state, stream_name, 0)
        max_bookmark_value = last_integer
    else:
        last_datetime = get_bookmark(state, stream_name, start_date)
        max_bookmark_value = last_datetime

    write_schema(catalog, stream_name)

    # pagination: loop thru all pages of data
    page = 1
    total_pages = 1  # initial value, set with first API call
    while page <= total_pages:
        params = {
            'page': page,
            **static_params # adds in endpoint specific, sort, filter params
        }

        if bookmark_query_field:
            if bookmark_type == 'datetime':
                params[bookmark_query_field] = last_datetime
            elif bookmark_type == 'integer':
                params[bookmark_query_field] = last_integer

        LOGGER.info('{} - Sync start {}'.format(
            stream_name,
            'since: {}, '.format(last_datetime) if bookmark_query_field else ''))

        # Squash params to query-string params
        querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
        LOGGER.info('URL for {}: https://api.helpscout.net/v2{}?{}'\
            .format(stream_name, path, querystring))

        # Get data, API request
        data = client.get(
            path,
            params=querystring,
            endpoint=stream_name)
        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()

        # Transform data with transform_json from transform.py
        #  This function denests _embedded, removes _embedded/_links, and
        #  converst camelCase to snake_case for fieldname keys.
        # For the HelpScout API, _embedded is always the root element.
        # The data_key identifies the collection of records below the _embedded element
        transformed_data = []
        if '_embedded' in data:
            transformed_data = transform_json(data["_embedded"], data_key)[data_key]

        # Process records and get the max_bookmark_value for the set of records
        max_bookmark_value, record_count = process_records(
            catalog=catalog,
            stream_name=stream_name,
            records=transformed_data,
            time_extracted=time_extracted,
            bookmark_field=bookmark_field,
            bookmark_type=bookmark_type,
            max_bookmark_value=max_bookmark_value,
            last_datetime=last_datetime,
            last_integer=last_integer,
            parent=parent,
            parent_id=parent_id)

        total_records = total_records + record_count
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                should_stream, last_stream_child = should_sync_stream(get_selected_streams(catalog),
                                                            None,
                                                            child_stream_name)
                if should_stream:
                    for record in transformed_data:
                        parent_id = record.get('id')
                        LOGGER.info('Syncing: {}, parent_id: {}'.format(child_stream_name, parent_id))
                        child_path = child_endpoint_config.get('path').format(str(parent_id))
                        child_total_records = sync_endpoint(
                            client=client,
                            catalog=catalog,
                            state=state,
                            start_date=start_date,
                            stream_name=child_stream_name,
                            path=child_path,
                            endpoint_config=child_endpoint_config,
                            data_key=child_endpoint_config.get('data_key', stream_name),
                            static_params=child_endpoint_config.get('params', {}),
                            bookmark_query_field=child_endpoint_config.get('bookmark_query_field'),
                            bookmark_field=child_endpoint_config.get('bookmark_field'),
                            bookmark_type=child_endpoint_config.get('bookmark_type'),
                            id_field=child_endpoint_config.get('id_field'),
                            parent=child_endpoint_config.get('parent'),
                            parent_id=parent_id)
                        LOGGER.info('Synced: {}, parent_id: {}, total_records: {}'.format(
                            child_stream_name, 
                            parent_id,
                            child_total_records))

        # Update the state with the max_bookmark_value for the stream
        if bookmark_field:
            write_bookmark(state, stream_name, max_bookmark_value)

        # set page and total_pages for pagination
        page = data['page']['number']
        total_pages = data['page']['totalPages']
        LOGGER.info('{} - Synced - page: {}, records: {}, total pages: {}'.format(
            stream_name,
            page,
            record_count,
            total_pages))
        # Some streams (workflows) erroneously return page=0 if the first page is the only page.
        #  Includeing a while-loop break for this edge case.
        if page == 0:
            break
        page = page + 1

    # Return the total_records synced in the stream
    return total_records


# Review catalog and make a list of selected streams
def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


# Review last_stream (last currently syncing stream), if any,
#  and continue where it left off in the selected streams.
# Or begin from the beginning, if no last_stream, and sync
#  all selected steams.
# Returns should_sync_stream (true/false) and last_stream.
def should_sync_stream(selected_streams, last_stream, stream_name):
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            last_stream = None
        if stream_name in selected_streams:
            return True, last_stream
    return False, last_stream


def sync(client, catalog, state, start_date):
    selected_streams = get_selected_streams(catalog)
    LOGGER.info('selected_streams: {}'.format(selected_streams))
    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)

    # endpoints: API URL endpoints to be called
    # properties:
    #   <root node>: Plural stream name for the endpoint
    #   path: API endpoint relative path, when added to the base URL, creates the full path
    #   params: Query, sort, and other endpoint specific parameters
    #   data_key: JSON element containing the records for the endpoint
    #   bookmark_query_field: Typically a date-time field used for filtering the query
    #   bookmark_field: Replication key field, typically a date-time, used for filtering the results
    #        and setting the state
    #   bookmark_type: Data type for bookmark, integer or datetime
    #   id_field: Primary key property for the record
    #   children: A collection of child endpoints (where the endpoint path includes the parent id)
    #   parent: On each of the children, the singular stream name for parent element
    endpoints = {
        'conversations': {
            'path': '/conversations',
            'params': {
                'status': 'all',
                'sortField': 'modifiedAt',
                'sortOrder': 'asc'
            },
            'data_key': 'conversations',
            'bookmark_query_field': 'modifiedSince',
            'bookmark_field': 'user_updated_at',
            'bookmark_type': 'datetime',
            'id_field': 'id',
            'children': {
                'conversation_threads': {
                    'path': '/conversations/{}/threads',
                    'data_key': 'threads',
                    'id_field': 'id',
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
            'data_key': 'customers',
            'bookmark_query_field': 'modifiedSince',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_field': 'id'
        },

        'mailboxes': {
            'path': '/mailboxes',
            'data_key': 'mailboxes',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_field': 'id',
            'children': {
                'mailbox_fields': {
                    'path': '/mailboxes/{}/fields',
                    'data_key': 'fields',
                    'id_field': 'id',
                    'parent': 'mailbox'
                },
                'mailbox_folders': {
                    'path': '/mailboxes/{}/folders',
                    'data_key': 'folders',
                    'bookmark_field': 'updated_at',
                    'bookmark_type': 'datetime',
                    'id_field': 'id',
                    'parent': 'mailbox'
                }
            }
        },

        'users': {
            'path': '/users',
            'data_key': 'users',
            'bookmark_field': 'updated_at',
            'bookmark_type': 'datetime',
            'id_field': 'id'
        },

        'workflows': {
            'path': '/workflows',
            'data_key': 'workflows',
            'bookmark_field': 'modified_at',
            'bookmark_type': 'datetime',
            'id_field': 'id'
        }
    }

    # For each endpoint (above), determine if the stream should be streamed
    #   (based on the catalog and last_stream), then sync those streams.
    for stream_name, endpoint_config in endpoints.items():
        should_stream, last_stream = should_sync_stream(selected_streams,
                                                        last_stream,
                                                        stream_name)
        if should_stream:
            LOGGER.info('START Syncing: {}'.format(stream_name))
            update_currently_syncing(state, stream_name)

            path = endpoint_config.get('path')
            total_records = sync_endpoint(
                client=client,
                catalog=catalog,
                state=state,
                start_date=start_date,
                stream_name=stream_name,
                path=path,
                endpoint_config=endpoint_config,
                data_key=endpoint_config.get('data_key', stream_name),
                static_params=endpoint_config.get('params', {}),
                bookmark_query_field=endpoint_config.get('bookmark_query_field'),
                bookmark_field=endpoint_config.get('bookmark_field'),
                bookmark_type=endpoint_config.get('bookmark_type'),
                id_field=endpoint_config.get('id_field'))

            update_currently_syncing(state, None)
            LOGGER.info('Synced: {}, total_records: {}'.format(
                            stream_name, 
                            total_records))
            LOGGER.info('FINISHED Syncing: {}'.format(stream_name))
