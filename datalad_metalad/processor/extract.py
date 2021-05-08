"""
Extract metadata and add it to a conduct-element.
"""
import enum
import logging
from pathlib import Path
from typing import Iterable, Union

from datalad.api import meta_extract

from .base import Processor


logger = logging.getLogger("datalad.metadata.processor.extract")


class ExtractorType(enum.Enum):
    DATASET = "Dataset"
    FILE = "File"


class MetadataExtractor(Processor):
    def __init__(self,
                 metadata_repository: Union[str, Path],
                 extractor_type: str,
                 extractor_name: str
                ):
        super().__init__()
        self.metadata_repository = Path(metadata_repository)
        self.extractor_type = extractor_type
        self.extractor_name = extractor_name

    def process(self, element: dict) -> Iterable:
        # TODO: use dataset-entry from element to determine the dataset
        #  that contains the element and to enable aggregation or saving
        if element["type"] != self.extractor_type:
            return [element]

        if element["type"] == "File":
            kwargs = dict(
                extractorname=self.extractor_name,
                dataset=self.metadata_repository,
                path=element["path"].relative_to(self.metadata_repository))
        elif element["type"] == "Dataset":
            kwargs = dict(
                extractorname=self.extractor_name,
                dataset=self.metadata_repository)
        else:
            logger.warning(
                f"ignoring element {element['path']} "
                f"with unknown type {element['type']}")
            return [element]

        result = []
        for extract_result in meta_extract(**kwargs):
            if extract_result["status"] == "ok":
                element["metadata_record"] = extract_result["metadata_record"]
            result.append(extract_result)
        return result

    @staticmethod
    def input_type() -> str:
        return "dataset-traversal-entity"

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
