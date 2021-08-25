from typing import Iterable

from .base import Processor
from ..pipelineelement import PipelineElement


class AutoDrop(Processor):
    def __init__(self):
        super().__init__()

    def process(self, pipeline_element: PipelineElement) -> Iterable:
        traversal_record = pipeline_element.get_input()
        if pipeline_element.get_dynamic_data("auto_get") is True:
            dataset = traversal_record["dataset"]
            dataset.drop(str(traversal_record["path"]), jobs=1)
        return [traversal_record]

    @staticmethod
    def input_type() -> str:
        return "dataset-traversal-entity"

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
