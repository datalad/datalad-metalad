"""
Traversal of datasets.

Relates to datalad_metalad issue #68
"""
import logging
import re
from dataclasses import dataclass
from os.path import isdir
from pathlib import Path
from typing import Iterable, Optional, Union

from datalad.distribution.dataset import (
    Dataset,
    require_dataset,
    resolve_path
)

from .base import Provider
from ..pipelineelement import (
    PipelineResult,
    ResultState
)


lgr = logging.getLogger('datalad.metadata.provider.datasettraverse')


_standard_exclude = [".git*", ".datalad", ".noannex"]


@dataclass
class DatasetTraverseResult(PipelineResult):
    path: Path
    type: str
    dataset: str
    root_dataset: str
    dataset_path: str
    message: Optional[str] = ""


class DatasetTraverser(Provider):
    def __init__(self,
                 top_level_dir: Union[str, Path],
                 traverse_sub_datasets: bool = False):

        super().__init__()
        self.top_level_dir = Path(top_level_dir)
        self.traverse_subdatasets = traverse_sub_datasets
        self.root_dataset = require_dataset(self.top_level_dir, purpose="dataset_traversal")
        self.root_dataset_path = Path(resolve_path(self.top_level_dir, self.root_dataset))

    def _traverse_dataset(self, dataset_path: Path) -> Iterable:
        dataset = require_dataset(dataset_path, purpose="dataset_traversal")
        yield DatasetTraverseResult(
                ResultState.SUCCESS,
                path=dataset.path,
                type="Dataset",
                dataset=dataset.path,
                root_dataset=str(self.root_dataset_path),
                dataset_path=str(dataset.pathobj.relative_to(self.root_dataset_path)))

        repo = dataset.repo
        for element_path in repo.get_files():
            if any(map(lambda pattern: re.match(pattern, element_path), _standard_exclude)):
                lgr.debug(f"Ignoring excluded element {element_path}")
                continue
            element_path = resolve_path(element_path, dataset)
            if not isdir(element_path):
                yield DatasetTraverseResult(
                    ResultState.SUCCESS,
                    path=resolve_path(element_path, dataset),
                    type="File",
                    dataset=dataset.path,
                    root_dataset=str(self.root_dataset_path),
                    dataset_path=str(dataset.pathobj.relative_to(self.root_dataset_path)))

        if self.traverse_subdatasets:
            for submodule_info in repo.get_submodules():
                submodule_path = submodule_info["path"]
                sub_dataset = Dataset(submodule_path)
                if sub_dataset.is_installed():
                    yield from self._traverse_dataset(submodule_info["path"])
                else:
                    lgr.debug(f"ignoring un-installed dataset at {submodule_path}")
        return

    def next_object(self):
        yield from self._traverse_dataset(self.root_dataset_path)

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
