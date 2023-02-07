from typing import Dict

from singer import (
    Catalog,
    get_logger,
    metadata,
    set_currently_syncing,
    write_schema,
    write_state,
)

from .client import HelpScoutClient
from .streams import STREAMS

logger = get_logger()


def sync(client: HelpScoutClient, catalog: Catalog, state: Dict, start_date: str) -> None:
    """Starts performing sync operation for selected streams."""
    for stream in catalog.get_selected_streams(state):
        tap_stream_id = stream.tap_stream_id
        stream_schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)
        # Skip syncing child streams, they'll be synced as part of parent streams
        if STREAMS[tap_stream_id].is_child:
            continue
        logger.info(f"Starting sync for stream {tap_stream_id}")
        stream_obj = STREAMS[tap_stream_id](client, start_date)
        state = set_currently_syncing(state, tap_stream_id)
        write_state(state)
        write_schema(tap_stream_id, stream_schema, stream_obj.key_properties, stream.replication_key)
        parent_ids = stream_obj.sync(state, stream_schema, stream_metadata)
        # Starts the sync for child streams associated with current parent stream
        if parent_ids and stream_obj.child_streams:
            for child in stream_obj.child_streams:
                child_stream = catalog.get_stream(child)
                # Sync only if the child stream is selected
                if child_stream.is_selected():
                    child_stream_id = child_stream.tap_stream_id
                    child_stream_schema = child_stream.schema.to_dict()
                    child_stream_metadata = metadata.to_map(child_stream.metadata)
                    child_stream_obj = STREAMS[child_stream_id](client, start_date)
                    write_schema(
                        child_stream_id,
                        child_stream_schema,
                        child_stream_obj.key_properties,
                        child_stream.replication_key,
                    )
                    child_stream_obj.sync(state, child_stream_schema, child_stream_metadata, parent_ids, True)

    state = set_currently_syncing(state, None)
    write_state(state)
