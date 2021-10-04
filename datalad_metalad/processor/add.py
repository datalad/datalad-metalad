"""
Add a metadata record to a dataset.
"""
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from datalad.api import meta_add

from .base import Processor
from .extract import MetadataExtractorResult
from ..provider.datasettraverse import DatasetTraverseResult
from ..pipelineelement import (
    PipelineElement,
    PipelineResult,
    ResultState
)


logger = logging.getLogger("datalad.metadata.processor.add")


@dataclass
class MetadataAddResult(PipelineResult):
    path: str


class MetadataAdder(Processor):
    def __init__(self,
                 aggregate: bool = False
                 ):

        super().__init__()
        self.aggregate = aggregate

    def add_metadata_result(self,
                            pipeline_element: PipelineElement,
                            metadata_repository: Path,
                            metadata_record: MetadataExtractorResult,
                            additional_values: Optional[str]):

        metadata_record["dataset_id"] = str(metadata_record["dataset_id"])
        if "path" in metadata_record:
            metadata_record["path"] = str(metadata_record["path"])

        logger.debug(
            "processor.add: running meta-add with:\n"
            f"metadata:\n"
            f"{json.dumps(metadata_record)}\n"
            f"dataset: {metadata_repository}\n"
            f"additional_values:\n"
            f"{json.dumps(additional_values)}\n")

        result = []
        for add_result in meta_add(metadata=metadata_record,
                                   dataset=str(metadata_repository),
                                   additionalvalues=additional_values,
                                   result_renderer="disabled"):

            path = add_result["path"]
            if add_result["status"] == "ok":
                md_add_result = MetadataAddResult(ResultState.SUCCESS, path)
                pipeline_element.set_result("path", path)
            else:
                md_add_result = MetadataAddResult(ResultState.FAILURE, path)
                md_add_result.base_error = add_result
            result.append(md_add_result)
        return result

    def process(self, pipeline_element: PipelineElement) -> PipelineElement:

        metadata_result_list = pipeline_element.get_result("metadata")
        if not metadata_result_list:
            logger.debug(
                f"Ignoring pipeline element without metadata: "
                f"{pipeline_element}")
            return pipeline_element

        logger.debug(f"Adding metadata from pipeline element: {pipeline_element}")

        # Determine the destination metadata store. This is either the root
        # level dataset (if aggregate is True), or the containing dataset (if
        # aggregate is False).
        dataset_traversal_record: DatasetTraverseResult = pipeline_element.get_result("dataset-traversal-record")[0]
        if dataset_traversal_record.dataset_path == Path("."):
            metadata_repository = dataset_traversal_record.fs_base_path
            additional_values = None
        else:
            if self.aggregate:
                metadata_repository = dataset_traversal_record.fs_base_path
                additional_values = json.dumps({
                    "dataset_path": str(dataset_traversal_record.dataset_path),
                    "root_dataset_id": str(dataset_traversal_record.root_dataset_id),
                    "root_dataset_version": str(dataset_traversal_record.root_dataset_version)
                })
            else:
                metadata_repository = dataset_traversal_record.fs_base_path / dataset_traversal_record.dataset_path
                additional_values = None

        # Add all metadata records
        for metadata_result in metadata_result_list:
            result = self.add_metadata_result(pipeline_element,
                                              metadata_repository,
                                              metadata_result.metadata_record,
                                              additional_values)

            pipeline_element.add_result("add", result)

        return pipeline_element
