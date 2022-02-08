import json
import logging
from typing import Optional

from datalad.cmd import BatchedCommand
from datalad.support.constraints import EnsureBool

from ..documentedinterface import (
    DocumentedInterface,
    ParameterEntry,
)
from ..pipelinedata import PipelineData
from .base import Consumer


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
                keyword="aggregate (NOT SUPPORTED YET)",
                help="""A boolean that indicates whether sub-dataset metadata
                        should be added into the root-dataset, i.e. aggregated
                        (aggregate=True), or whether sub-dataset metadata should
                        be added into the sub-dataset (aggregate=False). The
                        sub-dataset path must exist and contain a git-repo.""",
                optional=True,
                constraints=[EnsureBool()])
        ]
    )

    def __init__(self,
                 *,
                 dataset: str,
                 aggregate: Optional[bool] = False):

        self.batched_add = BatchedCommand(
            ["datalad", "meta-add", "-d", dataset, "--batch-mode", "-"])

    def __del__(self):
        self.batched_add.close()

    def consume(self, pipeline_data: PipelineData) -> PipelineData:
        for metadata in pipeline_data.get_result("metadata"):
            metadata_json = metadata.to_json()
            metadata.metadata_record["dataset_id"] = str(metadata.metadata_record["dataset_id"])
            metadata.metadata_record["path"] = str(metadata.metadata_record["path"])
            metadata_json_string = json.dumps(metadata_json)
            logger.info(f"adding: {metadata_json_string}")
            self.batched_add(metadata_json_string)
        return pipeline_data
