"""
Add a metadata record to a dataset.
"""
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import (
    cast,
    Dict,
)

from datalad.api import meta_add
from datalad.support.constraints import EnsureBool

from .base import Processor
from ..documentedinterface import (
    DocumentedInterface,
    ParameterEntry,
)
from .extract import MetadataExtractorResult
from ..provider.datasettraverse import DatasetTraverseResult
from ..pipelinedata import (
    PipelineData,
    PipelineResult,
    ResultState,
)


logger = logging.getLogger("datalad.metadata.processor.add")


@dataclass
class MetadataAddResult(PipelineResult):
    path: str

    def to_json(self) -> Dict:
        return {
            **super().to_json(),
            "path": str(self.path)
        }


class MetadataAdder(Processor):

    interface_documentation = DocumentedInterface(
        "A component that adds metadata to a dataset, i.e. a metadata-store",
        [
            ParameterEntry(
                keyword="aggregate",
                help="""A boolean that indicates whether sub-dataset metadata
                        should be added into the root-dataset, i.e. aggregated
                        (aggregate=True), or whether sub-dataset metadata should
                        be added into the sub-dataset (aggregate=False). The
                        sub-dataset path must exist and contain a git-repo.""",
                optional=True,
                default=False,
                constraints=EnsureBool())
        ]
    )

    def __init__(self,
                 *,
                 aggregate: bool = False
                 ):

        super().__init__()
        self.aggregate = aggregate

    def process(self, pipeline_data: PipelineData) -> PipelineData:

        metadata_result_list = pipeline_data.get_result("metadata")
        if not metadata_result_list:
            logger.debug(
                f"Ignoring pipeline data without metadata: "
                f"{pipeline_data}")
            return pipeline_data

        # Determine the destination metadata store. This is either the root
        # level dataset (if aggregate is True), or the containing dataset (if
        # aggregate is False).
        dataset_traversal_record = cast(
            DatasetTraverseResult,
            pipeline_data.get_result("dataset-traversal-record")[0])

        if dataset_traversal_record.dataset_path == Path(""):
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
                metadata_repository = (
                    dataset_traversal_record.fs_base_path
                    / dataset_traversal_record.dataset_path
                )
                additional_values = None

        for metadata_extractor_result in metadata_result_list:

            metadata_record = cast(
                MetadataExtractorResult,
                metadata_extractor_result).metadata_record

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
                    pipeline_data.set_result("path", path)
                else:
                    md_add_result = MetadataAddResult(ResultState.FAILURE, path)
                    md_add_result.base_error = add_result
                result.append(md_add_result)

            pipeline_data.add_result_list("add", result)
        return pipeline_data
