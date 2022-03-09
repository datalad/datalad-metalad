import logging
import time
from collections import defaultdict
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple
)
from uuid import UUID

from .base import MetadataFilterBase
from ..metadatatypes.metadata import (
    JSONType,
    MetadataRecord,
)


__docformat__ = "restructuredtext"


DEMOFILTER_FORMAT_NAME = 'metalad_demofilter'

logger = logging.getLogger("datalad.metadata.filter.demofilter")


def _name_tuple_2_str(name_tuple: Tuple) -> str:
    result = ""
    for name in name_tuple:
        if isinstance(name, str):
            result += f".{name}" if result != "" else name
        elif isinstance(name, int):
            result += f"[{name}]"

    return result


def _flatten_structure(structure: JSONType,
                       name: Tuple = tuple()) -> List[Tuple[Tuple, Any]]:

    if isinstance(structure, (float, int, str, type(None))):
        return [(name, structure)]

    elif isinstance(structure, dict):
        result = []
        for key, value in structure.items():
            result.extend(_flatten_structure(value, name + (key,)))
        return result

    elif isinstance(structure, list):
        result = []
        for index, value in enumerate(structure):
            result.extend(_flatten_structure(value, name + (index,)))
        return result

    else:
        raise ValueError("don't know what to do")


class DemoFilter(MetadataFilterBase):
    """
    Create a "histogram"-like summary of the key values of all specified name_tuple
    across all metadata that is yielded by the metadata iterables.

    Histograms bins are determined by the metadata format and "name" within the
    format. The "name" is a flattened JSON key hierarchy.

    The set of metadata yielded by the iterables is determined by the
    metadata urls and the recursion flag that are passed to
    "datalad meta-filter".
    """

    version = "1.0"

    uuid = UUID("46a744da-1558-4532-bf32-51d26be6c27c")

    def __init__(self,
                 filter_format_name: Optional[str] = None):
        """
        :param filter_format_name: name of the filter format
        """
        MetadataFilterBase.__init__(self, filter_format_name)
        if filter_format_name and filter_format_name != DEMOFILTER_FORMAT_NAME:
            raise ValueError(
                f"Filter: {type(self).__name__} does not support "
                f"filter format: {filter_format_name}")
        self.format_name = filter_format_name or str(self.uuid)

    def get_version(self) -> str:
        return DemoFilter.version

    def get_id(self) -> UUID:
        return DemoFilter.uuid

    def filter(self,
               metadata_iterables: List[Iterable[MetadataRecord]],
               *args,
               **kwargs
               ) -> Iterable[MetadataRecord]:

        logger.debug(
            f"{repr(self)}: called with args: {repr(args)}, "
            f"kwargs: {repr(kwargs)}")

        histograms = defaultdict(list)
        for metadata_iterable in metadata_iterables:
            for metadata_record in metadata_iterable:
                self.add_metadata_to_histograms(metadata_record, histograms)

        # TODO: what is the best value for the dataset id and the dataset
        #  version? The extracted metadata might stem from a number of
        #  different datasets. For now we set it to unknown, i.e.
        #  UUID '00000000-0000-0000-0000-000000000000', and version '0'.
        yield MetadataRecord(
            type="dataset",
            extractor_version=self.version,
            extraction_parameter={
                **dict((str(index), value) for index, value in enumerate(args)),
                **kwargs
            },
            extractor_name=f"{self.get_id()}-{self.format_name}",
            extraction_time=time.time(),
            agent_name="Metalad Demo Filter",
            agent_email="metalad-demo-filter@example.com",
            dataset_id=UUID(int=0),
            dataset_version="0",
            extracted_metadata=dict(histograms))

    def add_metadata_to_histograms(self,
                                   metadata: MetadataRecord,
                                   histogram: Dict):

        flattened_metadata = _flatten_structure(metadata.extracted_metadata)
        for name_tuple, value in flattened_metadata:
            key = metadata.extractor_name + "." + _name_tuple_2_str(name_tuple)
            histogram[key].append(value)
