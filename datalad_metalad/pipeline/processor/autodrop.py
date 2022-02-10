import logging

from .base import Processor
from ..pipelinedata import PipelineData
from ...utils import check_dataset


logger = logging.getLogger("datalad.metadata.processor.autodrop")


class AutoDrop(Processor):

    def process(self, pipeline_data: PipelineData) -> PipelineData:
        if pipeline_data.get_result("auto_get") is not None:
            for traverse_result in pipeline_data.get_result("dataset-traversal-record"):
                fs_dataset_path = (
                    traverse_result.fs_base_path
                    / traverse_result.dataset_path
                )
                dataset = check_dataset(str(fs_dataset_path), "auto_get")
                path = traverse_result.path
                logger.debug(
                    f"AutoDrop: automatically dropping {path} "
                    f"in dataset {dataset.path}")
                dataset.drop(str(path))
        return pipeline_data

    @staticmethod
    def input_type() -> str:
        return "dataset-traversal-entity"

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
