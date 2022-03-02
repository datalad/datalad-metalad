import json
import logging
from pathlib import Path
from typing import (
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
from ..pipelinedata import PipelineData
from ..processor.extract import MetadataExtractorResult
from ..provider.datasettraverse import DatasetTraverseResult


logger = logging.getLogger("datalad.meta-conduct.consumer.add")


class BatchAdder(Consumer):

    interface_documentation = DocumentedInterface(
        "A component that adds metadata to a dataset in batch mode",
        [
            ParameterEntry(
                keyword="dataset",
                help="""A path to the dataset in which the metadata should be
                        stored.""",
                optional=True),
            ParameterEntry(
                keyword="aggregate",
                help="""A boolean that indicates whether sub-dataset metadata
                        should be added into the root-dataset, i.e. aggregated
                        (aggregate=True), or whether sub-dataset metadata should
                        be added into the sub-dataset (aggregate=False). In the
                        latter case it will be ignored, since it has to be sent
                        to a different instance of a batched add command.
                        Default: True""",
                optional=True,
                constraints=EnsureBool())
        ]
    )

    def __init__(self,
                 *,
                 dataset: str,
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

        # Determine the destination metadata store. This is either the root
        # level dataset (if aggregate is True), or the containing dataset (if
        # aggregate is False).
        dataset_traversal_record = cast(
            DatasetTraverseResult,
            pipeline_data.get_result("dataset-traversal-record")[0])

        if dataset_traversal_record.dataset_path == Path(""):
            additional_values = {}
        else:
            if self.aggregate:
                additional_values = {
                    "dataset_path": str(dataset_traversal_record.dataset_path),
                    "root_dataset_id": str(dataset_traversal_record.root_dataset_id),
                    "root_dataset_version": str(dataset_traversal_record.root_dataset_version)
                }
            else:
                logger.debug("ignoring non-root metadata because aggregate is not set")
                return pipeline_data

        for metadata_extractor_result in metadata_result_list:

            metadata_record = cast(
                MetadataExtractorResult,
                metadata_extractor_result).metadata_record

            metadata_record["dataset_id"] = str(metadata_record["dataset_id"])
            if "path" in metadata_record:
                metadata_record["path"] = str(metadata_record["path"])

            metadata_record_json = json.dumps({
                **metadata_record,
                **additional_values
            })

            logger.debug(f"adding {repr(metadata_record_json)}")
            self.batched_add(metadata_record_json)

        return pipeline_data
