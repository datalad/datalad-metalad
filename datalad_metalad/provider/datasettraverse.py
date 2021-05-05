"""
Traversal of datasets.

Relates to datalad_metalad issues #68
"""

from pathlib import Path
from typing import Optional, Set, Union

from datalad.utils import get_dataset_root
from .base import Provider


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
        self.current_dataset = None
        self.traverse_subdatasets = traverse_subdatasets
        self.exclude_paths = exclude_paths

        assert self.root_dataset_dir is not None, "No dataset found"
        assert str(self.top_level_dir) == str(self.root_dataset_dir), "Not a dataset root directory"

    def next_object(self):
        return

    @staticmethod
    def output_type() -> str:
        return "dataset-traversal-entity"
