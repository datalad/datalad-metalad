import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Dict,
    Optional,
    cast,
)

from datalad.cmd import BatchedCommand
from datalad.support.constraints import EnsureBool

from .base import Consumer
from ..documentedinterface import (
    DocumentedInterface,
    ParameterEntry,
)
from ..pipelinedata import (
    PipelineData,
    PipelineResult,
    ResultState,
)
from ..processor.extract import MetadataExtractorResult
from ..provider.datasettraverse import DatasetTraverseResult
from ..provider.metadatatraverse import MetadataTraverseResult


logger = logging.getLogger("datalad.meta-conduct.consumer.add")


@dataclass
class MetadataBatchAddResult(PipelineResult):
    path: str

    def to_json(self) -> Dict:
        return {
            **super().to_json(),
            "path": str(self.path)
        }


class BatchAdder(Consumer):

    interface_documentation = DocumentedInterface(
        "A component that adds metadata to a dataset in batch mode",
        [
            ParameterEntry(
                keyword="dataset",
                help="""A path to the dataset in which the metadata should be
                        stored.""",
                optional=True,
                default="."),
            ParameterEntry(
                keyword="aggregate",
                help="""A boolean that indicates whether sub-dataset metadata
                        should be added into the root-dataset, i.e. aggregated
                        (aggregate=True), or whether sub-dataset metadata should
                        be ignored (aggregate=False).""",
                optional=True,
                default=True,
                constraints=EnsureBool())
        ]
    )

    def __init__(self,
                 *,
                 dataset: str = ".",
                 aggregate: Optional[bool] = True):

        self.aggregate = aggregate
        self.batched_add = BatchedCommand(
            ["datalad", "meta-add", "-d", dataset, "--batch-mode", "-"])

    def __del__(self):
        self.batched_add("")
        self.batched_add.close()

    def consume(self, pipeline_data: PipelineData) -> PipelineData:

        metadata_result_list = pipeline_data.get_result("metadata")
        if not metadata_result_list:
            logger.debug(
                f"Ignoring pipeline data without metadata: "
                f"{pipeline_data}")
            return pipeline_data

        # If aggregate is specified, we aggregate sub-dataset metadata into this
        # metadata store.
        additional_values = get_dataset_traverse_root(pipeline_data)
        if additional_values:
            if not self.aggregate:
                logger.debug(
                    "ignoring non-root metadata because aggregate is not set")
                return pipeline_data
        else:
            additional_values = get_metadata_traverse_root(pipeline_data)

        for metadata_extractor_result in metadata_result_list:

            metadata_record = cast(
                MetadataExtractorResult,
                metadata_extractor_result).metadata_record

            metadata_record["dataset_id"] = str(metadata_record["dataset_id"])
            if "path" in metadata_record:
                metadata_record["path"] = str(metadata_record["path"])
                path = metadata_record["path"]
            else:
                path = ""

            metadata_record_json = json.dumps({
                **metadata_record,
                **(additional_values or {})
            })

            logger.debug(f"adding {repr(metadata_record_json)}")
            response = json.loads(self.batched_add(metadata_record_json))

            if response["status"] == "ok":
                add_result = MetadataBatchAddResult(ResultState.SUCCESS, path)
                pipeline_data.set_result("path", path)
            else:
                add_result = MetadataBatchAddResult(ResultState.FAILURE, path)
                add_result.base_error = response
            pipeline_data.add_result_list("batch_add", [add_result])

        return pipeline_data


def get_dataset_traverse_root(pipeline_data: PipelineData) -> Optional[Dict]:

    dataset_traversal_record = pipeline_data.get_result("dataset-traversal-record")
    if dataset_traversal_record is None:
        return None

    dataset_traversal_record = cast(
        DatasetTraverseResult,
        dataset_traversal_record[0])

    if dataset_traversal_record.dataset_path == Path(""):
        return {}
    return {
        "dataset_path": str(dataset_traversal_record.dataset_path),
        "root_dataset_id": str(dataset_traversal_record.root_dataset_id),
        "root_dataset_version": str(dataset_traversal_record.root_dataset_version)
    }


def get_metadata_traverse_root(pipeline_data: PipelineData) -> Optional[Dict]:

    # MetadataRecord traverse only yields metadata of one metadata-store. There
    # is no need and no possibility for further aggregation, but some records
    # might already be aggregated. They have to be added as aggregated records.

    metadata_traversal_record = pipeline_data.get_result("metadata-traversal-record")
    if metadata_traversal_record is None:
        return None

    metadata_traversal_record = cast(
        MetadataTraverseResult,
        metadata_traversal_record[0])

    metadata_record = metadata_traversal_record.metadata_record
    if all((metadata_record["dataset_path"],
            metadata_record["root_dataset_id"],
            metadata_record["root_dataset_version"])):
        return {
            "dataset_path": str(metadata_record["dataset_path"]),
            "root_dataset_id": str(metadata_record["root_dataset_id"]),
            "root_dataset_version": str(metadata_record["root_dataset_version"])
        }
    return {}
