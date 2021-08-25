from typing import Iterable

from ..pipelineelement import PipelineElement
from ..provider.datasettraverse import DatasetTraverseResult
from ..utils import check_dataset
from .base import Processor


class AutoGet(Processor):
    """
    This processor "gets" a file that is annexed and not locally available.
    It sets a flag in the element that will allow the AutoDrop-processor
    to automatically drop the file again.
    """
    def __init__(self):
        super().__init__()

    def process(self, pipeline_element: PipelineElement) -> Iterable:
        traverse_result: DatasetTraverseResult = pipeline_element.get_input()
        if traverse_result.type == "File":
            path = traverse_result.path
            if path.is_symlink():
                if path.exists() is False:
                    dataset = check_dataset(traverse_result.dataset, "auto_get")
                    dataset.get(str(traverse_result.path), jobs=1)
                    pipeline_element.set_dynamic_data("auto_get", True)
        return [traverse_result]

    @staticmethod
    def input_type() -> str:
        return "dataset-traversal-entity"

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
