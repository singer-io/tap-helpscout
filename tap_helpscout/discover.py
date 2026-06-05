import json
from typing import Dict, Tuple

import singer
from singer.catalog import Catalog

from tap_helpscout.exceptions import Http403Error
from tap_helpscout.helpers import get_abs_path
from tap_helpscout.streams import STREAMS

LOGGER = singer.get_logger()


def get_schemas() -> Tuple[Dict, Dict]:
    """Builds the singer schema and metadata dictionaries."""
    streams, stream_metadata = {}, {}

    for stream_name, stream in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")

        with open(schema_path, encoding="utf-8") as file:
            schema = json.load(file)

        streams[stream_name], stream_metadata[stream_name] = schema, stream.get_metadata(schema)

    return streams, stream_metadata


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """Remove child streams from the catalog whose parent stream was excluded.

    Mutates schemas and field_metadata in place.
    """
    for name, stream_cls in list(STREAMS.items()):
        if name not in schemas:
            continue
        if not stream_cls.parent:
            continue
        # Resolve the parent's tap_stream_id using prefix matching
        # (e.g. child.parent="mailbox" -> parent tap_stream_id="mailboxes")
        parent_stream_id = next(
            (
                sid
                for sid, cls in STREAMS.items()
                if not cls.parent and sid.startswith(stream_cls.parent)
            ),
            None,
        )
        if parent_stream_id and parent_stream_id not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream "
                "'%s' is not accessible.",
                name,
                parent_stream_id,
            )
            schemas.pop(name)
            field_metadata.pop(name)


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Probe each parent stream for read access and remove inaccessible streams.

    Removes inaccessible streams (and their children) from schemas and
    field_metadata in place.  Raises Http403Error if no parent streams are
    accessible.
    """
    inaccessible_streams = [
        stream_name
        for stream_name, stream_obj in STREAMS.items()
        if stream_name in schemas and not stream_obj(client=client, start_date=client.start_date).check_access()
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise Http403Error(
            "HTTP-error-code: 403, Error: The account credentials supplied do not have "
            "'read' access to any of the streams. Please recheck configuration."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the following "
            "stream(s): %s. These streams have been excluded from the catalog.",
            ", ".join(inaccessible_streams),
        )


def discover(client=None) -> Catalog:
    """Run discovery mode, build and return the Singer catalog.

    When a client is provided, each parent stream is probed for read access
    and streams the credentials cannot access (HTTP 403) are excluded from
    the returned catalog.
    """
    schemas, schema_metadata = get_schemas()

    if client is not None:
        _apply_access_checks(client, schemas, schema_metadata)

    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schema_metadata[schema_name]
        streams.append(
            {
                "stream": schema_name,
                "tap_stream_id": schema_name,
                "key_properties": STREAMS[schema_name].key_properties,
                "schema": schema,
                "metadata": schema_meta,
            }
        )
    return Catalog.from_dict({"streams": streams})
