from typing import Iterable

from .base import Processor


class AutoGet(Processor):
    """
    This processor get a file that is annexed and not locally available.
    It sets a flag in the element that will allow the AutoDrop-processor
    to automatically drop the file again.
    """
    def __init__(self):
        super().__init__()

    def process(self, element: dict) -> Iterable:
        if element["type"] == "File":
            path = element["path"]
            if path.is_symlink():
                if path.exists() is False:
                    dataset = element["dataset"]
                    dataset.get(str(element["path"]), jobs=1)
                    element["auto_get"] = True
        return [element]

    @staticmethod
    def input_type() -> str:
        return "dataset-traversal-entity"

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
