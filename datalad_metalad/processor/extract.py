"""
Extract metadata and add it to a conduct-element.
"""
import enum
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Optional

from datalad.api import meta_extract

from .base import Processor
from ..pipelineelement import (
    PipelineElement,
    PipelineResult
)
from ..provider.datasettraverse import DatasetTraverseResult


logger = logging.getLogger("datalad.metadata.processor.extract")


class ExtractorType(enum.Enum):
    DATASET = "Dataset"
    FILE = "File"


@dataclass
class MetadataExtractorResult(PipelineResult):
    path: str
    context: Optional[Dict] = None
    metadata_record: Optional[Dict] = field(init=False)


class MetadataExtractor(Processor):
    def __init__(self,
                 extractor_type: str,
                 extractor_name: str
                 ):
        super().__init__()
        self.extractor_type = extractor_type
        self.extractor_name = extractor_name

    def process(self, pipeline_element: PipelineElement) -> Iterable:
        # TODO: use dataset-entry from element to determine the dataset
        #  that contains the element and to enable aggregation or saving

        # TODO: make dataset traversal entitiy a PipelineResult
        dataset_traverse_result: DatasetTraverseResult = pipeline_element.get_input()

        if dataset_traverse_result.type != self.extractor_type:
            logger.debug(
                f"ignoring un-configured type "
                f"{dataset_traverse_result.type}")
            return []

        dataset_path = Path(dataset_traverse_result.dataset)
        object_path = Path(dataset_traverse_result.path)
        object_type = dataset_traverse_result.type

        if object_type == "File":
            kwargs = dict(
                extractorname=self.extractor_name,
                dataset=dataset_path,
                path=object_path.relative_to(dataset_path))
        elif object_type == "Dataset":
            kwargs = dict(
                extractorname=self.extractor_name,
                dataset=dataset_path)
        else:
            logger.warning(
                f"ignoring element {object_path} "
                f"with unknown type {object_type}")
            return []

        result = []
        for extract_result in meta_extract(**kwargs):
            if extract_result["status"] == "ok":
                md_extractor_result = MetadataExtractorResult(
                    True, extract_result["path"])
                md_extractor_result.metadata_record = extract_result["metadata_record"]
                md_extractor_result.context = None
            else:
                md_extractor_result = MetadataExtractorResult(
                    False, extract_result["path"])
                md_extractor_result.base_error = extract_result
            result.append(md_extractor_result)
        return result

    @staticmethod
    def input_type() -> str:
        return "dataset-traversal-entity"

    @staticmethod
    def output_type() -> str:
        return "metadata-record"
