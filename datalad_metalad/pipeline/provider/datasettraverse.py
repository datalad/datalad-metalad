"""
Traversal of datasets.

Relates to datalad_metalad issue #68
"""
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Iterable,
    Optional,
    Union,
)

from datalad.distribution.dataset import (
    Dataset,
    require_dataset,
    resolve_path,
)
from datalad.support.constraints import (
    EnsureBool,
    EnsureChoice,
)

from .base import Provider
from ..documentedinterface import (
    DocumentedInterface,
    ParameterEntry,
)
from ..pipelinedata import (
    PipelineData,
    PipelineResult,
    ResultState,
)


lgr = logging.getLogger('datalad.metadata.pipeline.provider.datasettraverse')

# By default, we exclude all paths that start with "."
_standard_exclude = ["^\\..*"]


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

    file_mask = 0x01
    dataset_mask = 0x02
    name_to_item_set = {
        "file": {file_mask},
        "dataset": {dataset_mask},
        "both": {file_mask, dataset_mask}
    }

    interface_documentation = DocumentedInterface(
        """A component that traverses a dataset and generates file- and/or
           dataset-data for each file and/or dataset object in the dataset and
           optionally in its sub-datasets.""",
        [
            ParameterEntry(
                keyword="top_level_dir",
                help="""A path to the dataset that should be traversed.""",
                optional=False),
            ParameterEntry(
                keyword="item_type",
                help="""Indicate which elements should be reported. Either
                        files ("file") or datasets ("dataset") or files and
                        datasets ("both").""",
                optional=False,
                constraints=EnsureChoice("file", "dataset", "both")),
            ParameterEntry(
                keyword="traverse_sub_datasets",
                help="""Indicate whether sub-datasets should be traversed as
                        well.""",
                optional=True,
                default=False,
                constraints=EnsureBool())
        ]
    )

    def __init__(self,
                 *,
                 top_level_dir: Union[str, Path],
                 item_type: str,
                 traverse_sub_datasets: bool = False
                 ):

        known_types = tuple(DatasetTraverser.name_to_item_set.keys())
        if item_type.lower() not in known_types:
            raise ValueError(f"{item_type.lower()} is not a known item_type. "
                             f"Known types are: {', '.join(known_types)}")

        self.top_level_dir = Path(top_level_dir).absolute().resolve()
        self.item_set = self.name_to_item_set[item_type.lower()]
        self.traverse_sub_datasets = traverse_sub_datasets
        self.root_dataset = require_dataset(self.top_level_dir,
                                            purpose="dataset_traversal")
        self.fs_base_path = Path(resolve_path(self.top_level_dir,
                                              self.root_dataset))
        self.seen = dict()

    def _already_visited(self, dataset: Dataset, relative_element_path: Path):
        if dataset.id not in self.seen:
            self.seen[dataset.id] = set()
        if relative_element_path in self.seen[dataset.id]:
            lgr.info(f"ignoring already visited element: "
                     f"{dataset.id}:{relative_element_path}\t"
                     f"({dataset.repo.pathobj / relative_element_path})")
            return True
        self.seen[dataset.id].add(relative_element_path)
        return False

    def _get_base_dataset_result(self,
                                 dataset: Dataset,
                                 id_key: str,
                                 version_key: str):
        return {
            id_key: str(dataset.id),
            version_key: str(dataset.repo.get_hexsha())}

    def _get_dataset_result_part(self, dataset: Dataset):
        if dataset.pathobj == self.fs_base_path:
            return {
                "dataset_path": Path(""),
                **self._get_base_dataset_result(dataset,
                                                "dataset_id",
                                                "dataset_version")}
        else:
            return {
                "dataset_path": dataset.pathobj.relative_to(self.fs_base_path),
                **self._get_base_dataset_result(dataset,
                                                "dataset_id",
                                                "dataset_version"),
                **self._get_base_dataset_result(self.root_dataset,
                                                "root_dataset_id",
                                                "root_dataset_version")}

    def _traverse_dataset(self, dataset_path: Path) -> Iterable:
        dataset = require_dataset(dataset_path, purpose="dataset_traversal")
        element_path = resolve_path("", dataset)

        if self.dataset_mask in self.item_set:

            if self._already_visited(dataset, Path("")):
                return

            yield PipelineData((
                ("path", element_path),
                (
                    "dataset-traversal-record",
                    [
                        DatasetTraverseResult(**{
                            "state": ResultState.SUCCESS,
                            "fs_base_path": self.fs_base_path,
                            "type": "dataset",
                            "path": element_path,
                            **self._get_dataset_result_part(dataset)
                        })
                    ]
                )))

        if self.file_mask in self.item_set:
            repo = dataset.repo
            for relative_element_path in repo.get_files():

                element_path = resolve_path(relative_element_path, dataset)
                if any([
                        re.match(pattern, path_part)
                        for path_part in element_path.parts
                        for pattern in _standard_exclude]):
                    lgr.debug(f"Ignoring excluded element {element_path}")
                    continue

                if not element_path.is_dir():

                    if self._already_visited(dataset, relative_element_path):
                        continue

                    yield PipelineData((
                        ("path", element_path),
                        (
                            "dataset-traversal-record",
                            [
                                DatasetTraverseResult(**{
                                    "state": ResultState.SUCCESS,
                                    "fs_base_path": self.fs_base_path,
                                    "type": "file",
                                    "path": element_path,
                                    **self._get_dataset_result_part(dataset)
                                })
                            ]
                        )
                    ))

        if self.traverse_sub_datasets:
            repo = dataset.repo
            for submodule_info in repo.get_submodules():
                submodule_path = submodule_info["path"]
                sub_dataset = Dataset(submodule_path)
                if sub_dataset.is_installed():
                    yield from self._traverse_dataset(submodule_info["path"])
                else:
                    lgr.debug(
                        f"ignoring un-installed dataset at {submodule_path}")
        return

    def next_object(self) -> Iterable:
        yield from self._traverse_dataset(self.fs_base_path)
