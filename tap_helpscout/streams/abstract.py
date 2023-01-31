from abc import abstractmethod, ABC
from typing import Dict, Tuple, List
from singer.metadata import get_standard_metadata, to_list, to_map, write
from singer.bookmarks import ensure_bookmark_path
from singer import metrics, Transformer, write_state
import singer

from tap_helpscout.transform import transform_json

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
    def replication_query_field(self) -> str:
        """Defines the replication query field in the request field."""

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
    def params(self) -> Dict:
        """API params to make a request"""

    @property
    @abstractmethod
    def path(self) -> str:
        """API endpoint for the stream"""

    @property
    @abstractmethod
    def data_key(self) -> str:
        """Key value in API response which identifies the collection of records
         below the _embedded element"""

    @property
    @abstractmethod
    def bookmark_value(self) -> str:
        """Bookmark value for a given incremental stream"""

    @property
    @abstractmethod
    def child_streams(self) -> List:
        """Stores the list of child streams for a given parent stream"""

    @property
    @abstractmethod
    def parent(self) -> List:
        """Parent stream name for a child stream"""

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """The unique identifier for the stream.

        This is allowed to be different from the name of the stream in
        order to allow for sources that have duplicate stream names.
        """

    def __init__(self, client=None, start_date=None) -> None:
        self._path = None
        self._bookmark_value = None
        self.client = client
        self.start_date = start_date

    def get_bookmark(self, state: Dict) -> str:
        """Retrieves bookmark value for a given stream from state file"""
        return state.get("bookmarks", {}).get(self.tap_stream_id, self.start_date)

    def write_bookmark(self, state: Dict, value: str) -> Dict:
        """Writes bookmark value for a given stream to state file"""
        state = ensure_bookmark_path(state, ["bookmarks", self.tap_stream_id])
        state["bookmarks"][self.tap_stream_id] = value
        write_state(state)

    def make_request_params(self, state):
        """Generates request params required to send an API request"""
        if self.replication_query_field:
            self.params[self.replication_query_field] = self.get_bookmark(state)
        return '&'.join([f'{key}={value}' for (key, value) in self.params.items()])

    def get_records(self, state: Dict, parent_id=None):
        """Retrieves records from API as paginated streams"""
        page = total_pages = 1
        path = self.path.format(parent_id) if parent_id else self.path
        query_string = self.make_request_params(state)
        while page <= total_pages:
            query_string_tmp = f"{query_string}&page={page}"
            logger.info(f'URL for {self.tap_stream_id}: https://api.helpscout.net/v2{path}?'
                        f'{query_string_tmp}')
            data = self.client.get(path, params=query_string_tmp, endpoint=self.tap_stream_id)
            yield from self.transform_records(data)
            page = data["page"]["number"]
            total_pages = data["page"]["totalPages"]
            if page == 0:
                break
            page += 1

    def transform_records(self, data: Dict) -> List:
        """Transforms keys in extracted data"""
        return transform_json(data["_embedded"], self.data_key)[self.data_key] if '_embedded' in \
                                                                                  data else []

    def process_records(self, state: Dict, schema: Dict, stream_metadata: Dict, is_parent=False,
                        parent_id=None):
        """Processes and writes transformed data"""
        parent_ids = set()
        max_bookmark_value = self.get_bookmark(state)
        with Transformer() as transformer:
            with metrics.record_counter(self.tap_stream_id) as counter:
                for record in self.get_records(state, parent_id):
                    if parent_id:
                        record[f"{self.parent}_id"] = parent_id
                    record = transformer.transform(record, schema, stream_metadata)
                    # Insert the parentId into each child record
                    if self.replication_key and self.replication_key in record:
                        record_bookmark = record[self.replication_key]
                        if record_bookmark >= max_bookmark_value:
                            max_bookmark_value = record_bookmark
                            singer.write_record(self.tap_stream_id, record)
                            counter.increment()
                            if is_parent:
                                parent_ids.add(record["id"])
                    else:
                        singer.write_record(self.tap_stream_id, record)
                        counter.increment()
                if self.replication_method == "INCREMENTAL":
                    self.write_bookmark(state, max_bookmark_value)
        return parent_ids

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, parent_ids=None,
             is_child=False):
        """
        1. Gets bookmark value for currently syncing stream.
        2. Generates request params required to make API call.
        3. Extracts data as paginated streams.
        4. Transforms data as per metadata fields.
        5. Processes each record and writes it to stdout and saves the bookmark.
        6. Checks if the current stream as children. If yes, repeats same process for child stream
        """
        is_parent = bool(self.child_streams)
        if not is_child:
            return self.process_records(state, schema, stream_metadata, is_parent)
        for parent_id in parent_ids:
            logger.info(f"Starting sync for child stream {self.tap_stream_id} of parent"
                        f" {self.parent} for "
                        f"Id {parent_id}")
            self.process_records(state, schema, stream_metadata, is_parent, parent_id)

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

    @bookmark_value.setter
    def bookmark_value(self, value):
        self._bookmark_value = value

    @path.setter
    def path(self, value):
        self._path = value


class IncrementalStream(BaseStream, ABC):
    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"
    params = {}
    replication_query_field = ""
    child_streams = []


class FullStream(BaseStream, ABC):

    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    replication_key = None
    replication_query_field = ""
    valid_replication_keys = None
    params = {}




