import logging

from .base import Processor
from ..pipelinedata import (
    PipelineResult,
    ResultState,
    PipelineData,
)
from ...utils import check_dataset


logger = logging.getLogger("datalad.metadata.processor.autoget")


class AutoGet(Processor):
    """
    This processor "gets" a file that is annexed and not locally available.
    It sets a flag in the element that will allow the AutoDrop-processor
    to automatically drop the file again.
    """

    def process(self, pipeline_data: PipelineData) -> PipelineData:
        for traverse_result in pipeline_data.get_result("dataset-traversal-record"):
            if traverse_result.type == "File":
                path = traverse_result.path
                if path.is_symlink():
                    if path.exists() is False:
                        fs_dataset_path = (
                            traverse_result.fs_base_path
                            / traverse_result.dataset_path
                        )
                        dataset = check_dataset(str(fs_dataset_path), "auto_get")
                        logger.debug(
                            f"AutoGet: automatically getting {path} "
                            f"in dataset {dataset.path}")
                        dataset.get(str(traverse_result.path), jobs=1)
                        pipeline_data.set_result(
                            "auto_get",
                            [PipelineResult(ResultState.SUCCESS)])
        return pipeline_data
