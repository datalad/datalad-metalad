"""
Traversal of datasets.

Relates to datalad_metalad issue #68


TODO: this is a naive implementation, replace with the proper thing,
 once the conduct mechanics is fleshed out.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set, Union

from datalad.utils import get_dataset_root

from .base import Provider
from ..pipelineelement import PipelineResult


standard_exclude = [".git*", ".datalad", ".noannex"]


@dataclass
class DatasetTraverseResult(PipelineResult):
    path: Path
    type: str
    dataset: str


class DatasetTraverser(Provider):
    def __init__(self,
                 top_level_dir: Union[str, Path],
                 recursive: bool = True,
                 traverse_subdatasets: bool = False,
                 traverse_subdatasets_limit: Optional[int] = None,
                 exclude_paths: Optional[Set[Path]] = None):

        super().__init__()
        self.top_level_dir = Path(top_level_dir)
        self.root_dataset_dir = get_dataset_root(self.top_level_dir)
        self.recursive = recursive
        self.traverse_subdatasets = traverse_subdatasets
        self.traverse_subdatasets_limit = traverse_subdatasets_limit
        self.exclude_paths = (
            standard_exclude
            if exclude_paths is None
            else exclude_paths)

        assert self.root_dataset_dir is not None, "No dataset found"
        assert str(self.top_level_dir) == str(self.root_dataset_dir), "Not a dataset root directory"

        self.current_dataset = top_level_dir
        self.subdataset_level = 0

    def _is_git_or_dataset_root(self, path, require_datalad_dir: bool = True) -> bool:
        if path.is_dir():
            git_dir_path = (tuple(path.glob(".git")) + (None,))[0]
            if git_dir_path is not None and git_dir_path.is_dir():
                if require_datalad_dir is True:
                    datalad_dir_path = (tuple(path.glob(".git")) + (None,))[0]
                    if datalad_dir_path is not None and datalad_dir_path.is_dir():
                        return True
                    else:
                        return False
                return True
        return False

    def _create_result(self, path: Path, path_type: str) -> DatasetTraverseResult:
        return DatasetTraverseResult(
            True,
            path=path,
            type=path_type,
            dataset=str(self.current_dataset))

    def _traverse_recursive(self, current_element: Path):
        # Report the current element
        if any([current_element.match(pattern) for pattern in self.exclude_paths]):
            return
        if current_element.is_dir():
            if self._is_git_or_dataset_root(current_element):
                path_type = "Dataset"
            else:
                path_type = "Directory"
            if path_type == "Directory" and self.recursive is True:
                for element in current_element.iterdir():
                    yield from self._traverse_recursive(element)
            elif path_type == "Dataset":
                yield self._create_result(current_element, path_type)
                if current_element == self.top_level_dir:
                    # If this is the root-dataset, show its content
                    for element in current_element.iterdir():
                        yield from self._traverse_recursive(element)
                else:
                    if self.traverse_subdatasets:
                        if self.traverse_subdatasets_limit is not None:
                            if self.subdataset_level < self.traverse_subdatasets_limit:
                                self.subdataset_level += 1
                                saved_current_dataset = self.current_dataset
                                self.current_dataset = current_element
                                for element in current_element.iterdir():
                                    yield from self._traverse_recursive(element)
                                self.current_dataset = saved_current_dataset
                        else:
                            saved_current_dataset = self.current_dataset
                            self.current_dataset = current_element
                            for element in current_element.iterdir():
                                yield from self._traverse_recursive(element)
                            self.current_dataset = saved_current_dataset
        else:
            path_type = "File"
            yield self._create_result(current_element, path_type)

    def next_object(self):
        yield from self._traverse_recursive(self.top_level_dir)

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
