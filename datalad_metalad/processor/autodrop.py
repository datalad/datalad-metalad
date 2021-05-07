from typing import Iterable

from .base import Processor


class AutoDrop(Processor):
    def __init__(self):
        super().__init__()

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
