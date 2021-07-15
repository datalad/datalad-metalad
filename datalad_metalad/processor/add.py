"""
Add a metadata record to a dataset.
"""
from pathlib import Path
from typing import Iterable, Union

from .base import Processor


class MetadataAdder(Processor):
    def __init__(self,
                 metadata_repository: Union[str, Path]
                 ):

        super().__init__()
        self.metadata_repository = Path(metadata_repository)

    def process(self, element: dict) -> Iterable:
        if element.get("auto_get", False) is True:
            dataset = element["dataset"]
            dataset.drop(str(element["path"]), jobs=1)
        return [element]

    @staticmethod
    def input_type() -> str:
        return "dataset-traversal-entity"

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
