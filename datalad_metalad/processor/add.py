"""
Add a metadata record to a dataset.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Union

from datalad.api import meta_add

from .base import Processor
from ..pipelineelement import (
    PipelineElement,
    PipelineResult,
    ResultState
)


@dataclass
class MetadataAddResult(PipelineResult):
    path: str


class MetadataAdder(Processor):
    def __init__(self,
                 metadata_repository: Union[str, Path]
                 ):

        super().__init__()
        self.metadata_repository = Path(metadata_repository)

    def process(self, pipeline_element: PipelineElement) -> Iterable:
        metadata_record = pipeline_element.get_input().metadata_record
        metadata_record["dataset_id"] = str(metadata_record["dataset_id"])
        if "path" in metadata_record:
            metadata_record["path"] = str(metadata_record["path"])

        if True:
            print(f"[DRY] meta-add: {metadata_record} to {self.metadata_repository}")
            return [MetadataAddResult(ResultState.SUCCESS, str(self.metadata_repository))]

        result = []
        for add_result in meta_add(metadata=metadata_record, dataset=str(self.metadata_repository)):
            if add_result["status"] == "ok":
                md_add_result = MetadataAddResult(ResultState.SUCCESS, add_result["path"])
            else:
                md_add_result = MetadataAddResult(ResultState.FAILURE, add_result["path"])
                md_add_result.base_error = add_result
            result.append(md_add_result)
        return result

    @staticmethod
    def input_type() -> str:
        return "metadata-record"

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
