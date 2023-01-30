from singer import Catalog, set_currently_syncing, write_state, metadata, get_logger, write_schema
from typing import Dict
from .streams import STREAMS
from .client import HelpScoutClient

logger = get_logger()


def sync(client: HelpScoutClient, catalog: Catalog, state: Dict, start_date: str) -> None:
    """Starts performing sync operation for selected streams"""
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
        write_schema(tap_stream_id, stream_schema, stream_obj.key_properties,
                     stream.replication_key)
        stream_obj.sync(state, stream_schema, stream_metadata)

    state = set_currently_syncing(state, None)
    write_state(state)
