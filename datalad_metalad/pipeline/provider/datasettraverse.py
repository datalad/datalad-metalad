"""
Traversal of datasets.

Relates to datalad_metalad issue #68
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import (
    Generator,
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
from datalad.support.exceptions import NoDatasetFound

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
    DatasetInfo,
    FileInfo,
)
from ...utils import ls_struct


__docformat__ = "restructuredtext"


lgr = logging.getLogger('datalad.metadata.pipeline.provider.datasettraverse')


std_args = dict(
    result_renderer="disabled"
)


class TraversalType(Enum):
    DATASET = "dataset"
    FILE = "file"


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
    element_info: Optional[DatasetInfo | FileInfo] = None

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


class DatasetTraverser(Provider):

    name_to_item_set = {
        "file": {TraversalType.FILE},
        "dataset": {TraversalType.DATASET},
        "both": {TraversalType.FILE, TraversalType.DATASET}
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
                 ) -> None:

        known_types = tuple(DatasetTraverser.name_to_item_set.keys())
        if item_type.lower() not in known_types:
            raise ValueError(
                f"{item_type.lower()} is not a known item_type. "
                f"Known types are: {', '.join(known_types)}"
            )

        self.top_level_dir = Path(top_level_dir).absolute().resolve()
        self.item_set = self.name_to_item_set[item_type.lower()]
        self.traverse_sub_datasets = traverse_sub_datasets
        self.root_dataset = require_dataset(
            self.top_level_dir,
            check_installed=True,
            purpose="dataset_traversal"
        )
        self.root_dataset_version = self.root_dataset.repo.get_hexsha()
        self.fs_base_path = Path(
            resolve_path(self.top_level_dir, self.root_dataset)
        )
        self.seen = dict()
        self.annex_info = None

    def _already_visited(self,
                         dataset: Dataset,
                         relative_element_path: Path
                         ) -> bool:
        if dataset.id not in self.seen:
            self.seen[dataset.id] = set()
        if relative_element_path in self.seen[dataset.id]:
            lgr.debug(
                "ignoring already visited element: %s:%s\t%s",
                dataset.id, relative_element_path,
                dataset.repo.pathobj / relative_element_path
            )
            return True
        self.seen[dataset.id].add(relative_element_path)
        return False

    def _get_base_dataset_result(self,
                                 dataset: Dataset,
                                 dataset_hexsha: str,
                                 id_key: str = "dataset_id",
                                 version_key: str = "dataset_version"
                                 ) -> dict[str, str]:
        return {
            id_key: str(dataset.id),
            version_key: dataset_hexsha
        }

    def _get_dataset_result_part(self,
                                 dataset: Dataset,
                                 dataset_hexsha: str
                                 ) -> dict[str, str | Path]:
        if dataset.pathobj == self.fs_base_path:
            return {
                "dataset_path": Path(""),
                **self._get_base_dataset_result(dataset, dataset_hexsha)
            }
        else:
            return {
                "dataset_path": dataset.pathobj.relative_to(self.fs_base_path),
                **self._get_base_dataset_result(dataset, dataset_hexsha),
                **self._get_base_dataset_result(
                    self.root_dataset,
                    self.root_dataset_version,
                    "root_dataset_id",
                    "root_dataset_version"
                )
            }

    def _traverse_dataset(self,
                          dataset_path: Path,
                          is_root: bool = False
                          ) -> Generator:
        """Traverse all elements of dataset, and potentially its subdatasets.

        This method will traverse the dataset `dataset` and yield traversal
        results for each `file` or installed `dataset`, depending on the
        selected item types.

        :param Path dataset_path: the root of all traversals
        :return: a generator, yielding `DatasetTraversalResult`-records
        :rtype: Generator[PipelineData]
        """

        if is_root:
            dataset = self.root_dataset
            dataset_hexsha = self.root_dataset_version
        else:
            try:
                dataset = require_dataset(
                    dataset=dataset_path,
                    check_installed=True,
                    purpose="dataset_traversal"
                )
            except NoDatasetFound:
                lgr.info(
                    "ignoring not installed sub-dataset at %s",
                    dataset_path
                )
                return

            dataset_hexsha = dataset.repo.get_hexsha()

        element_path = resolve_path("", dataset)

        if TraversalType.DATASET in self.item_set:
            if self._already_visited(dataset, Path("")):
                return
            traverse_result = self._generate_result(
                dataset=dataset,
                dataset_hexsha=dataset_hexsha,
                dataset_path=str(
                    dataset.pathobj.relative_to(self.fs_base_path)
                ),
                element_path=element_path,
                element_info={
                    "type": TraversalType.DATASET.value,
                    "state": "",
                    "gitshasum": "",
                    "prev_gitshasum": ""
                }
            )
            yield PipelineData((
                ("path", element_path),
                ("dataset-traversal-record", [traverse_result])
            ))

        if TraversalType.FILE in self.item_set:
            for element_path, element_info in ls_struct(dataset).items():
                if element_info["type"] == "file":
                    traverse_result = self._generate_result(
                        dataset=dataset,
                        dataset_hexsha=dataset_hexsha,
                        dataset_path=str(
                            dataset.pathobj.relative_to(self.fs_base_path)
                        ),
                        element_path=element_path,
                        element_info=element_info
                    )
                    yield PipelineData((
                        ("path", element_path),
                        ("dataset-traversal-record", [traverse_result])
                    ))

        if self.traverse_sub_datasets:
            for submodule_info in dataset.repo.get_submodules():
                yield from self._traverse_dataset(submodule_info["path"])
        return

    def _get_element_info_object(self,
                                 dataset: Dataset,
                                 dataset_path: str,
                                 element_path: Path,
                                 element_info: dict
                                 ) -> DatasetInfo | FileInfo:
        dataset_keys = {
            "path": str(element_path),
            "dataset_path": dataset_path
        }
        if element_info["type"] == "dataset":
            return DatasetInfo.from_dict({**element_info, **dataset_keys})

        intra_dataset_path = element_path.relative_to(dataset.pathobj)
        file_keys = {
            **dataset_keys,
            "intra_dataset_path": str(intra_dataset_path),
            "fs_base_path": str(self.fs_base_path)
        }
        return FileInfo.from_dict({**element_info, **file_keys})

    def _generate_result(self,
                         dataset: Dataset,
                         dataset_hexsha: str,
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
            **self._get_dataset_result_part(dataset, dataset_hexsha)
        })

    def next_object(self) -> Generator:
        yield from self._traverse_dataset(self.fs_base_path, True)
