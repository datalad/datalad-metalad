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
    PipelineElement,
    PipelineResult,
    ResultState
)


lgr = logging.getLogger('datalad.metadata.provider.datasettraverse')


_standard_exclude = [".git*", ".datalad", ".noannex"]


@dataclass
class DatasetTraverseResult(PipelineResult):
    fs_base_path: Path
    type: str
    dataset_path: Path
    dataset_id: str
    dataset_version: str
    path: Optional[Path] = None
    root_dataset_id: Optional[str] = None
    root_dataset_version: Optional[str] = None

    message: Optional[str] = ""


class DatasetTraverser(Provider):
    def __init__(self,
                 top_level_dir: Union[str, Path],
                 traverse_sub_datasets: bool = False):

        super().__init__()
        self.top_level_dir = Path(top_level_dir)
        self.traverse_sub_datasets = traverse_sub_datasets
        self.root_dataset = require_dataset(self.top_level_dir, purpose="dataset_traversal")
        self.fs_base_path = Path(resolve_path(self.top_level_dir, self.root_dataset))

    def _get_base_dataset_result(self,
                                 dataset: Dataset,
                                 id_key: str,
                                 version_key: str):

        return {
            id_key: str(dataset.id),
            version_key: str(dataset.repo.get_hexsha())
        }

    def _get_dataset_result_part(self, dataset: Dataset):
        if dataset.pathobj == self.fs_base_path:
            return {
                "dataset_path": Path("."),
                **self._get_base_dataset_result(dataset, "dataset_id", "dataset_version")
            }
        else:
            return {
                "dataset_path": dataset.pathobj.relative_to(self.fs_base_path),
                **self._get_base_dataset_result(dataset, "dataset_id", "dataset_version"),
                **self._get_base_dataset_result(self.root_dataset, "root_dataset_id", "root_dataset_version"),
            }

    def _traverse_dataset(self, dataset_path: Path) -> Iterable:
        dataset = require_dataset(dataset_path, purpose="dataset_traversal")
        yield PipelineElement(((
            "dataset-traversal-record",
            [
                DatasetTraverseResult(**{
                    "state": ResultState.SUCCESS,
                    "fs_base_path": self.fs_base_path,
                    "type": "Dataset",
                    **self._get_dataset_result_part(dataset)
                })
            ]
        ),))

        repo = dataset.repo
        for element_path in repo.get_files():
            if any(map(lambda pattern: re.match(pattern, element_path), _standard_exclude)):
                lgr.debug(f"Ignoring excluded element {element_path}")
                continue
            element_path = resolve_path(element_path, dataset)
            if not isdir(element_path):
                yield PipelineElement(((
                    "dataset-traversal-record",
                    [
                        DatasetTraverseResult(**{
                            "state": ResultState.SUCCESS,
                            "fs_base_path": self.fs_base_path,
                            "type": "File",
                            "path": resolve_path(element_path, dataset),
                            **self._get_dataset_result_part(dataset)
                        })
                    ]
                ),))

        if self.traverse_sub_datasets:
            for submodule_info in repo.get_submodules():
                submodule_path = submodule_info["path"]
                sub_dataset = Dataset(submodule_path)
                if sub_dataset.is_installed():
                    yield from self._traverse_dataset(submodule_info["path"])
                else:
                    lgr.debug(f"ignoring un-installed dataset at {submodule_path}")
        return

    def next_object(self) -> Iterable:
        yield from self._traverse_dataset(self.fs_base_path)
