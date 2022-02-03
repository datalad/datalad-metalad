import json
import logging
from typing import Optional

from datalad.cmd import BatchedCommand

from ..pipelinedata import PipelineData
from .base import Consumer


logger = logging.getLogger("datalad.meta-conduct.consumer.add")


class BatchAdder(Consumer):
    def __init__(self,
                 dataset: str):

        self.dataset = dataset
        self.batched_add = BatchedCommand(
            ["datalad", "meta-add", "-d", dataset, "--batch-mode", "-"])

    def __del__(self):
        if hasattr(self, "batched_add"):
            self.batched_add.close()

    def consume(self, pipeline_data: PipelineData) -> PipelineData:

        if pipeline_data.get_result("metadata") is None:
            return pipeline_data

        for metadata in pipeline_data.get_result("metadata"):
            metadata_record_json = metadata.to_json()["metadata_record"]
            metadata_record_json_string = json.dumps(metadata_record_json, default=str)
            logger.debug(f"adding: {metadata_record_json_string}")
            self.batched_add(metadata_record_json_string)
        return pipeline_data
