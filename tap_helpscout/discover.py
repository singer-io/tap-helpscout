import json
from typing import Dict, Tuple

from singer import metadata
from singer.catalog import Catalog

from tap_helpscout.helpers import get_abs_path
from tap_helpscout.streams import STREAMS


def get_schemas() -> Tuple[Dict, Dict]:
    """Builds the singer schema and metadata dictionaries."""
    streams, stream_metadata = {}, {}

    for stream_name, stream in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")

        with open(schema_path, encoding="utf-8") as file:
            schema = json.load(file)

        streams[stream_name], stream_metadata[stream_name] = schema, stream.get_metadata(schema)

    return streams, stream_metadata


def discover():
    """Starts discover process."""
    schemas, schema_metadata = get_schemas()
    streams = []

    for schema_name, schema in schemas.items():
        stream_cls = STREAMS[schema_name]
        schema_meta = schema_metadata[schema_name]
        mdata = metadata.to_map(schema_meta)

        # Add parent-tap-stream-id if this is a child stream
        is_child = getattr(stream_cls, "is_child", False)
        parent_tap_stream_id = getattr(stream_cls, "parent", None) if is_child else None
        if parent_tap_stream_id:
            mdata = metadata.write(mdata, (), "parent-tap-stream-id", parent_tap_stream_id)
        
        mdata = metadata.to_list(mdata)
        
        streams.append(
            {
                "stream": schema_name,
                "tap_stream_id": schema_name,
                "key_properties": stream_cls.key_properties,
                "schema": schema,
                "metadata": mdata,
            }
        )
    return Catalog.from_dict({"streams": streams})
