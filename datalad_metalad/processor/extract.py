"""
Extract metadata and add it to a conduct-element.
"""
import enum
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

from datalad.api import meta_extract

from .base import Processor
from ..pipelineelement import (
    PipelineElement,
    PipelineResult,
    ResultState
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

    def process(self, pipeline_element: PipelineElement) -> PipelineElement:

        dataset_traverse_record: DatasetTraverseResult = pipeline_element.get_result("dataset-traversal-record")[0]
        logger.debug(f"MetadataExtractor called with: {dataset_traverse_record}")

        if dataset_traverse_record.type != self.extractor_type:
            logger.debug(
                f"ignoring un-configured type "
                f"{dataset_traverse_record.type}")
            return pipeline_element

        dataset_path = dataset_traverse_record.fs_base_path / dataset_traverse_record.dataset_path
        object_type = dataset_traverse_record.type

        if object_type == "File":
            object_path = Path(dataset_traverse_record.path)
            kwargs = dict(
                extractorname=self.extractor_name,
                dataset=dataset_path,
                path=object_path.relative_to(dataset_path))
        elif object_type == "Dataset":
            kwargs = dict(
                extractorname=self.extractor_name,
                dataset=dataset_path)
        else:
            logger.warning(f"ignoring unknown type {object_type}")
            return pipeline_element

        result = []
        for extract_result in meta_extract(**kwargs):
            if extract_result["status"] == "ok":
                md_extractor_result = MetadataExtractorResult(
                    ResultState.SUCCESS, (
                        extract_result["path"]
                        if extract_result["type"] == "Dataset"
                        else str(dataset_path)))
                md_extractor_result.metadata_record = extract_result["metadata_record"]
                md_extractor_result.context = None
            else:
                md_extractor_result = MetadataExtractorResult(
                    ResultState.FAILURE, (
                        extract_result["path"]
                        if extract_result["type"] == "Dataset"
                        else str(dataset_path)))
                md_extractor_result.base_error = extract_result
            result.append(md_extractor_result)

        pipeline_element.set_result("metadata", result)
        return pipeline_element

    @staticmethod
    def input_type() -> str:
        return "dataset-traversal-entity"

    @staticmethod
    def output_type() -> str:
        return "metadata-record"
