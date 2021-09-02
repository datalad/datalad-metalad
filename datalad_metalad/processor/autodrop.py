import logging

from .base import Processor
from ..pipelineelement import PipelineElement
from ..provider.datasettraverse import DatasetTraverseResult
from ..utils import check_dataset


logger = logging.getLogger("datalad.metadata.processor.autodrop")


class AutoDrop(Processor):
    def __init__(self):
        super().__init__()

    def process(self, pipeline_element: PipelineElement) -> PipelineElement:
        if pipeline_element.get_result("auto_get") is not None:
            for traverse_result in pipeline_element.get_result("dataset-traversal-record"):
                fs_dataset_path = traverse_result.fs_base_path / traverse_result.dataset_path
                dataset = check_dataset(str(fs_dataset_path), "auto_get")
                path = traverse_result.path
                logger.debug(f"AutoDrop: automatically dropping {path} in dataset {dataset.path}")
                dataset.drop(str(path))
        return pipeline_element

    @staticmethod
    def input_type() -> str:
        return "dataset-traversal-entity"

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
