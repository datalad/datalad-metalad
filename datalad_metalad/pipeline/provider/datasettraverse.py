"""
Traversal of datasets.

Relates to datalad_metalad issue #68
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Generator,
    Iterable,
    Optional,
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
from datalad.support.annexrepo import AnnexRepo
from datalad.support.exceptions import NoDatasetFound
from datalad.tests.utils import get_annexstatus

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
from ...extractors.base import (
    AnnexedFileInfo,
    DatasetInfo,
    FileInfo,
)


__docformat__ = "restructuredtext"


lgr = logging.getLogger('datalad.metadata.pipeline.provider.datasettraverse')

# By default, we exclude all paths that start with "."
standard_excludes = ["^\\..*"]
standard_exclude_matcher = [
    re.compile(standard_exclude)
    for standard_exclude in standard_excludes
]


std_args = dict(
    result_renderer="disabled"
)


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
    element_info: Optional[AnnexedFileInfo | DatasetInfo | FileInfo] = None

    def to_dict(self) -> dict:

        def optional_dict(name, attribute):
            return {name: str(attribute)} if attribute else {}

        return dict(
            fs_base_path=str(self.fs_base_path),
            type=self.type,
            dataset_path=str(self.dataset_path),
            dataset_id=str(self.dataset_id),
            dataset_version=str(self.dataset_version),
            **optional_dict("path", self.path),
            **optional_dict("root_dataset_id", self.root_dataset_id),
            **optional_dict("root_dataset_version", self.root_dataset_version),
            **optional_dict("message", self.message),
            **({"element_info": self.element_info.to_dict()}
               if self.element_info is not None
               else {}
               )
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class DatasetTraverser(Provider):

    name_to_item_set = {
        "file": {"file"},
        "dataset": {"dataset"},
        "both": {"file", "dataset"}
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
                 top_level_dir: str | Path,
                 item_type: str,
                 traverse_sub_datasets: bool = False
                 ):

        known_types = tuple(DatasetTraverser.name_to_item_set.keys())
        if item_type.lower() not in known_types:
            raise ValueError(
                f"{item_type.lower()} is not a known item_type. "
                f"Known types are: {', '.join(known_types)}"
            )

        self.top_level_dir = resolve_path(top_level_dir)
        self.item_set = self.name_to_item_set[item_type.lower()]
        self.traverse_sub_datasets = traverse_sub_datasets
        self.root_dataset = require_dataset(
            self.top_level_dir,
            purpose="dataset_traversal"
        )
        self.fs_base_path = Path(
            resolve_path(self.top_level_dir, self.root_dataset)
        )
        self.seen = dict()
        self.annex_info = None

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
                                 id_key: str = "dataset_id",
                                 version_key: str = "dataset_version"):
        return {
            id_key: str(dataset.id),
            version_key: str(dataset.repo.get_hexsha())
        }

    def _get_dataset_result_part(self, dataset: Dataset):
        if dataset.pathobj == self.fs_base_path:
            return {
                "dataset_path": Path(""),
                **self._get_base_dataset_result(dataset)
            }
        else:
            return {
                "dataset_path": dataset.pathobj.relative_to(self.fs_base_path),
                **self._get_base_dataset_result(dataset),
                **self._get_base_dataset_result(
                    self.root_dataset,
                    "root_dataset_id",
                    "root_dataset_version"
                )
            }

    x = """
    def get_annex_file_info(self,
                            annex_repo: AnnexRepo,
                            dataset_path: Path
                            ) -> list:
        annex_status = get_annexstatus(annex_repo)
        return [
            AnnexedFileInfo.from_annex_status(status, path, str(Path(path).relative_to(dataset_path)))
            if len(status) == 13
            else FileInfo.from_annex_status(status, path, str(Path(path).relative_to(dataset_path)))
            for path, status in annex_status
        ]

    def _traverse_dataset(self, dataset_path: Path) -> Iterable:
        dataset = require_dataset(dataset_path, purpose="dataset_traversal")
        element_path = resolve_path("", dataset)

        if isinstance(dataset.repo, AnnexRepo):
            info = self.get_annex_file_info(dataset.repo)

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
    """

    def is_excluded(self, path: Path) -> bool:
        """Check whether any of the path parts matches an exclude-pattern."""
        return any([
            matcher.match(part)
            for part in Path(path).parts
            for matcher in standard_exclude_matcher
        ])

    def is_installed(self, dataset: Dataset) -> bool:
        try:
            require_dataset(dataset, purpose="dataset traversal")
            return True
        except NoDatasetFound:
            return False

    def _get_element_info_object(self,
                                 dataset: Dataset,
                                 dataset_path: str,
                                 element_path: Path,
                                 element_info: dict
                                 ) -> AnnexedFileInfo | DatasetInfo | FileInfo:
        dataset_keys = {
            "path": str(element_path),
            "dataset_path": dataset_path
        }
        if element_info["type"] == "dataset":
            return DatasetInfo.from_dict({**element_info, **dataset_keys})

        intra_dataset_path = element_path.relative_to(dataset.pathobj)
        file_keys = {
            **dataset_keys,
            "intra_dataset_path": str(intra_dataset_path)
        }
        if len(element_info) == 5:
            return FileInfo.from_dict({**element_info, **file_keys})
        return AnnexedFileInfo.from_dict({**element_info, **file_keys})

    def _generate_result(self,
                         dataset: Dataset,
                         dataset_path: str,
                         element_path: Path,
                         element_info: dict
                         ) -> DatasetTraverseResult:
        """Create a traverse result for an element, i.e. file or dataset."""
        return DatasetTraverseResult(**{
            "state": ResultState.SUCCESS,
            "fs_base_path": self.fs_base_path,
            "type": element_info["type"],
            "path": element_path,
            "element_info": self._get_element_info_object(
                dataset,
                dataset_path,
                element_path,
                element_info
            ),
            **self._get_dataset_result_part(dataset)
        })

    def _traverse_single_dataset(self,
                                 root: Path,
                                 dataset: Dataset
                                 ) -> Generator[DatasetTraverseResult, None, None]:
        """Traverse all elements of dataset, do not recurse into subdatasets.

        This method will traverse the dataset `dataset` and yield traversal
        results for each `file` or installed `dataset`, depending on the
        selected item types.

        :param Path root: the root of all traversals.
        :param Dataset dataset: the dataset to traverse in this method.
        :return: a generator, yielding `DatasetTraversalResult`-records
        :rtype: Generator[DatasetTraverseResult, None, None]
        """

        if not self.is_installed(dataset):
            lgr.debug(f"ignoring un-installed dataset at {dataset.path}")
            return

        dataset_path = str(dataset.pathobj.relative_to(root))
        if isinstance(dataset.repo, AnnexRepo):
            status = get_annexstatus
        else:
            status = dataset.repo.status
        for path_str, element_info in status(dataset.repo).items():
            if element_info["type"] in self.item_set:
                element_path = Path(path_str)
                if self.is_excluded(element_path):
                    lgr.debug(f"Ignoring excluded path {element_path}")
                    continue
                traverse_result = self._generate_result(
                    dataset=dataset,
                    dataset_path=dataset_path,
                    element_path=element_path,
                    element_info=element_info
                )
                yield PipelineData((
                    ("path", element_path),
                    ("dataset-traversal-record", [traverse_result])
                ))

    def _traverse_subdatasets(self,
                              root_dataset: Dataset
                              ) -> Generator[DatasetTraverseResult, None, None]:
        """Traverse all sub datasets of `root_dataset`

        Traverse all sub datasets of the root dataset and yield pipeline
        results, that can be added to pipeline data. All `dataset_path`
        instances are relative to the path of `root_dataset`.

        :param root_dataset: Dataset:
        :return: Generator
        """
        for sub_dataset in root_dataset.subdatasets(recursive=True, **std_args):
            yield from self._traverse_single_dataset(
                root=root_dataset.pathobj,
                dataset=Dataset(sub_dataset["path"])
            )

    def _traverse_datasets(self, top_level_dir: Path):
        """Traverse the dataset at top_level_dir and optionally its subdatasets

        The
        :param top_level_dir:
        :return:
        """

        # Yield annex status information from the root dataset.
        dataset = Dataset(top_level_dir)
        yield from self._traverse_single_dataset(
            root=top_level_dir,
            dataset=dataset
        )

        # If subdataset recursion is requested, yield annex status information
        # from all subdatasets.
        if self.traverse_sub_datasets:
            yield from self._traverse_subdatasets(dataset)

    def next_object(self) -> Iterable:
        yield from self._traverse_datasets(self.fs_base_path)

    doc = """
    We need to collect information that was sent to legacy extractors.
    
    - inter-dataset path
    - intra-dataset path
    - file infos *
    """