from abc import abstractmethod, ABC
from typing import Dict, Tuple
from singer.metadata import get_standard_metadata, to_list, to_map, write
from singer.bookmarks import ensure_bookmark_path
import singer

logger = singer.get_logger()


class BaseStream:
    """Base class representing generic stream methods and meta-attributes."""

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def forced_replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_key(self) -> str:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def valid_replication_keys(self) -> Tuple[str, str]:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """The unique identifier for the stream.

        This is allowed to be different from the name of the stream in
        order to allow for sources that have duplicate stream names.
        """

    @abstractmethod
    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict):
        """Performs Sync."""

    def __init__(self, client=None, start_date=None) -> None:
        self.client = client
        self.start_date = start_date

    def get_bookmark(self, state: Dict) -> str:
        """Retrieves bookmark value for a given stream from state file"""
        return state.get("bookmarks", {}).get(self.tap_stream_id, self.start_date)

    def write_bookmark(self, state: Dict, value: str) -> Dict:
        """Writes bookmark value for a given stream to state file"""
        state = ensure_bookmark_path(state, ["bookmarks", self.tap_stream_id])
        state["bookmarks"][self.tap_stream_id] = value
        return state

    def get_records(self):
        """Retrieves records from API as paginated streams"""


    @classmethod
    def get_metadata(cls, schema) -> Dict[str, str]:
        """Returns a `dict` for generating stream metadata."""
        stream_metadata = get_standard_metadata(**{
            "schema": schema,
            "key_properties": cls.key_properties,
            "valid_replication_keys": cls.valid_replication_keys,
            "replication_method": cls.replication_method or cls.forced_replication_method,
        })
        stream_metadata = to_map(stream_metadata)
        if cls.valid_replication_keys is not None:
            for key in cls.valid_replication_keys:
                stream_metadata = write(stream_metadata, ("properties", key), "inclusion",
                                        "automatic")
        stream_metadata = to_list(stream_metadata)
        return stream_metadata


class IncrementalStream(BaseStream, ABC):
    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict):
        pass


class FullStream(BaseStream, ABC):

    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    replication_key = None
    valid_replication_keys = None

    def get_records(self):
        pass




