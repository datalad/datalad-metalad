import json
import logging
from queue import Queue
from typing import Optional

from datalad.cmd import BatchedCommand

from ..pipelineelement import PipelineElement
from .base import Consumer


logger = logging.getLogger("datalad.meta-conduct.consumer.add")


class BatchAdder(Consumer):
    def __init__(self,
                 dataset: str):
        self.batched_add = BatchedCommand(
            ["datalad", "meta-add", "-d", dataset, "--batch-mode", "-"])

    def __del__(self):
        self.batched_add.close()

    def consume(self, pipeline_element: PipelineElement) -> PipelineElement:
        for metadata in pipeline_element.get_result("metadata"):
            metadata_json = metadata.to_json()
            metadata.metadata_record["dataset_id"] = str(metadata.metadata_record["dataset_id"])
            metadata.metadata_record["path"] = str(metadata.metadata_record["path"])
            metadata_json_string = json.dumps(metadata_json)
            logger.info(f"adding: {metadata_json_string}")
            self.batched_add(metadata_json_string)
        return pipeline_element
