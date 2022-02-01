"""
Extract metadata and add it to a conduct-element.
"""
import enum
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    cast,
    Dict,
    Optional,
)

from datalad.api import meta_extract

from .base import Processor
from ..pipelineelement import (
    PipelineElement,
    PipelineResult,
    ResultState,
)
from ..provider.datasettraverse import DatasetTraverseResult


logger = logging.getLogger("datalad.metadata.processor.extract")


class ExtractorType(enum.Enum):
    DATASET = "dataset"
    FILE = "file"


@dataclass
class MetadataExtractorResult(PipelineResult):
    path: str
    context: Optional[Dict] = None
    metadata_record: Optional[Dict] = field(init=False)

    def to_json(self) -> Dict:
        return {
            **super().to_json(),
            "path": str(self.path),
            "metadata_record": self.metadata_record
        }


class MetadataExtractor(Processor):
    def __init__(self,
                 extractor_type: str,
                 extractor_name: str
                 ):
        super().__init__()
        self.extractor_type = extractor_type.lower()
        self.extractor_name = extractor_name

    def process(self, pipeline_element: PipelineElement) -> PipelineElement:

        dataset_traverse_record = cast(
            DatasetTraverseResult,
            pipeline_element.get_result("dataset-traversal-record")[0])
        logger.debug(f"MetadataExtractor process: {dataset_traverse_record}")

        if dataset_traverse_record.type != self.extractor_type:
            logger.debug(
                f"ignoring un-configured type "
                f"{dataset_traverse_record.type}")
            return pipeline_element

        dataset_path = (
            dataset_traverse_record.fs_base_path
            / dataset_traverse_record.dataset_path)

        object_type = dataset_traverse_record.type

        if object_type == "file":
            object_path = Path(dataset_traverse_record.path)
            kwargs = dict(
                extractorname=self.extractor_name,
                dataset=dataset_path,
                path=dataset_path / object_path.relative_to(dataset_path),
                result_renderer="disabled")
        elif object_type == "dataset":
            kwargs = dict(
                extractorname=self.extractor_name,
                dataset=dataset_path,
                result_renderer="disabled")
        else:
            logger.warning(f"ignoring unknown type {object_type}")
            return pipeline_element

        results = []
        for extract_result in meta_extract(**kwargs):

            path = str(dataset_path / extract_result.get("path", ""))

            if extract_result["status"] == "ok":
                md_extractor_result = MetadataExtractorResult(ResultState.SUCCESS, path)
                md_extractor_result.metadata_record = extract_result["metadata_record"]
                md_extractor_result.context = None

            else:
                md_extractor_result = MetadataExtractorResult(ResultState.FAILURE, path)
                md_extractor_result.base_error = extract_result
            results.append(md_extractor_result)

        pipeline_element.add_result_list("metadata", results)
        return pipeline_element
