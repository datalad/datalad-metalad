# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Dump metadata of a dataset
"""


__docformat__ = 'restructuredtext'


import json
import logging
from pathlib import Path
from typing import (
    cast,
    Any,
    Generator,
    Union,
)
from uuid import UUID

from datalad.distribution.dataset import datasetmethod
from datalad.interface.base import build_doc
from datalad.interface.base import Interface
from datalad.interface.utils import eval_results
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
)
from datalad.support.param import Parameter
from datalad.ui import ui
from dataladmetadatamodel.datasettree import datalad_root_record_name
from dataladmetadatamodel.mapper.reference import Reference
from dataladmetadatamodel.mappableobject import ensure_mapped
from dataladmetadatamodel.metadata import (
    Metadata,
    MetadataInstance,
)
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.metadatarootrecord import MetadataRootRecord
from dataladmetadatamodel.mtreenode import MTreeNode
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import TreeVersionList

from .pathutils.metadataurlparser import (
    MetadataURLParser,
    TreeMetadataURL,
    UUIDMetadataURL,
)

from .metadatautils import get_metadata_objects
from .metadatatypes import JSONType
from .pathutils.mtreesearch import MTreeSearch


default_mapper_family = "git"

lgr = logging.getLogger('datalad.metadata.dump')


def _dataset_report_matcher(node: Any) -> bool:
    return isinstance(node, MetadataRootRecord)


def _file_report_matcher(node: Any) -> bool:
    return isinstance(node, Metadata)


def _create_result_record(mapper: str,
                          metadata_store: Union[Path, str],
                          metadata_record: JSONType,
                          element_path: MetadataPath,
                          report_type: str):

    # Display remote metadata stores properly
    if isinstance(metadata_store, str):
        if Reference.is_remote(metadata_store):
            path = metadata_store + ":/" + str(element_path)
        else:
            path = (Path(metadata_store) / element_path).absolute()
    else:
        path = (metadata_store / element_path).absolute()

    return {
        "status": "ok",
        "action": "meta_dump",
        "backend": mapper,
        "metadata_source": metadata_store,
        "type": report_type,
        "metadata": metadata_record,
        "path": path,
    }


def _get_common_properties(root_dataset_identifier: UUID,
                           root_dataset_version: str,
                           prefix_path: MetadataPath,
                           metadata_root_record: MetadataRootRecord,
                           dataset_path: MetadataPath) -> dict:

    if prefix_path != MetadataPath(""):
        root_info = {
            "dataset_path": str(prefix_path)}
    elif dataset_path != MetadataPath(""):
        root_info = {
            "root_dataset_id": str(root_dataset_identifier),
            "root_dataset_version": root_dataset_version,
            "dataset_path": str(dataset_path)}
    else:
        root_info = {}

    return {
        **root_info,
        "dataset_id": str(metadata_root_record.dataset_identifier),
        "dataset_version": metadata_root_record.dataset_version
    }


def _get_instance_properties(extractor_name: str,
                             instance: MetadataInstance) -> dict:
    return {
        "extraction_time": instance.time_stamp,
        "agent_name": instance.author_name,
        "agent_email": instance.author_email,
        "extractor_name": extractor_name,
        "extractor_version": instance.configuration.version,
        "extraction_parameter": instance.configuration.parameter,
        "extracted_metadata": instance.metadata_content
    }


def show_dataset_metadata(mapper: str,
                          metadata_store: Path,
                          root_dataset_identifier: UUID,
                          root_dataset_version: str,
                          prefix_path: MetadataPath,
                          dataset_path: MetadataPath,
                          metadata_root_record: MetadataRootRecord
                          ) -> Generator[dict, None, None]:

    with ensure_mapped(metadata_root_record):
        dataset_level_metadata = metadata_root_record.dataset_level_metadata.read_in()

        if dataset_level_metadata is None:
            lgr.warning(
                f"no dataset level metadata for dataset "
                f"uuid:{root_dataset_identifier}@{root_dataset_version}")
            return

        common_properties = _get_common_properties(
            root_dataset_identifier,
            root_dataset_version,
            prefix_path,
            metadata_root_record,
            dataset_path)

        dataset_level_metadata = cast(Metadata, dataset_level_metadata)

        for extractor_name, extractor_runs in dataset_level_metadata.extractor_runs:
            for instance in extractor_runs:

                instance_properties = _get_instance_properties(
                    extractor_name,
                    instance)

                yield _create_result_record(
                    mapper=mapper,
                    metadata_store=metadata_store,
                    metadata_record={
                        "type": "dataset",
                        **common_properties,
                        **instance_properties
                    },
                    element_path=dataset_path,
                    report_type="dataset")


def show_file_tree_metadata(mapper: str,
                            metadata_store: Path,
                            root_dataset_identifier: UUID,
                            root_dataset_version: str,
                            prefix_path: MetadataPath,
                            dataset_path: MetadataPath,
                            metadata_root_record: MetadataRootRecord,
                            search_pattern: MetadataPath,
                            recursive: bool
                            ) -> Generator[dict, None, None]:

    with ensure_mapped(metadata_root_record):

        dataset_level_metadata = metadata_root_record.dataset_level_metadata
        file_tree = metadata_root_record.file_tree

        with ensure_mapped(dataset_level_metadata):
            with ensure_mapped(file_tree):

                # Do not try to search anything if the file tree is empty
                if not file_tree or not file_tree.mtree.child_nodes:
                    return

                # Determine matching file paths
                tree_search = MTreeSearch(file_tree.mtree)
                result_count = 0
                for path, metadata, _ in tree_search.search_pattern(pattern=search_pattern,
                                                                    recursive=recursive):
                    result_count += 1

                    # Ignore empty datasets and ignore paths that do not
                    # describe metadata, but a directory.
                    if metadata is None or isinstance(metadata, MTreeNode):
                        continue

                    metadata = cast(Metadata, metadata)

                    common_properties = _get_common_properties(
                        root_dataset_identifier,
                        root_dataset_version,
                        prefix_path,
                        metadata_root_record,
                        dataset_path)

                    with ensure_mapped(metadata):
                        for extractor_name, extractor_runs in metadata.extractor_runs:
                            for instance in extractor_runs:

                                instance_properties = _get_instance_properties(
                                    extractor_name,
                                    instance)

                                yield _create_result_record(
                                    mapper=mapper,
                                    metadata_store=metadata_store,
                                    metadata_record={
                                        "type": "file",
                                        "path": str(path),
                                        **common_properties,
                                        **instance_properties
                                    },
                                    element_path=dataset_path / path,
                                    report_type="dataset")

                if result_count == 0:
                    lgr.warning(
                        f"pattern '{str(search_pattern)}' does not match any element "
                        f"in file-tree of dataset {metadata_root_record.dataset_identifier}"
                        f"@{metadata_root_record.dataset_version} (stored on "
                        f"{mapper}:{metadata_store})")


def dump_from_dataset_tree(mapper: str,
                           metadata_store: Path,
                           tree_version_list: TreeVersionList,
                           metadata_url: TreeMetadataURL,
                           recursive: bool) -> Generator[dict, None, None]:
    """ Dump dataset tree elements that are referenced in path """

    # Normalize path representation
    if not metadata_url or metadata_url.dataset_path is None:
        metadata_url = TreeMetadataURL(MetadataPath(""), MetadataPath(""))

    # Get specified version, if none is specified, take all versions.
    requested_versions = ([metadata_url.version]
                          if metadata_url.version is not None
                          else list(tree_version_list.versions()))

    for version in requested_versions:

        try:
            # Fetch dataset tree for the specified version
            vpd_iterable = tree_version_list.get_dataset_trees(version)
        except KeyError:
            lgr.error(
                f"could not locate metadata for version {version} in "
                f"metadata_store {mapper}:{metadata_store}")
            continue

        for _, prefix_path, dataset_tree in vpd_iterable:
            root_mrr = dataset_tree.get_metadata_root_record(MetadataPath(""))

            if root_mrr is None:
                lgr.debug(
                    f"no root dataset record found for version "
                    f"{version} in metadata store "
                    f"{metadata_store}, cannot determine root dataset id")
                root_dataset_version = version
                root_dataset_identifier = "<unknown>"
            else:
                with ensure_mapped(root_mrr):
                    root_dataset_version = root_mrr.dataset_version
                    root_dataset_identifier = root_mrr.dataset_identifier

            # Create a tree search object to search for the specified datasets
            tree_search = MTreeSearch(dataset_tree.mtree)

            if prefix_path == MetadataPath(""):
                search_results = tree_search.search_pattern(
                    pattern=metadata_url.dataset_path,
                    recursive=recursive,
                    item_indicator=datalad_root_record_name)
            else:
                search_results = tree_search.search_pattern(
                    pattern=metadata_url.dataset_path,
                    recursive=recursive,
                    item_indicator=datalad_root_record_name)

            result_count = 0
            for path, node, _ in search_results:
                result_count += 1

                mrr = cast(
                    MetadataRootRecord,
                    node.get_child(datalad_root_record_name))

                yield from show_dataset_metadata(
                    mapper,
                    metadata_store,
                    root_dataset_identifier,
                    root_dataset_version,
                    prefix_path,
                    path,
                    mrr)

                yield from show_file_tree_metadata(
                    mapper,
                    metadata_store,
                    root_dataset_identifier,
                    root_dataset_version,
                    prefix_path,
                    path,
                    mrr,
                    metadata_url.local_path,
                    recursive)

            if result_count == 0:
                lgr.error(
                    f"search pattern '{str(metadata_url.dataset_path)}' does not "
                    f"match any dataset in dataset-tree of dataset "
                    f"{root_dataset_identifier}@{root_dataset_version} (stored on "
                    f"{mapper}:{metadata_store})")


def dump_from_uuid_set(mapper: str,
                       metadata_store: Path,
                       uuid_set: UUIDSet,
                       path: UUIDMetadataURL,
                       recursive: bool) -> Generator[dict, None, None]:

    """ Dump UUID-identified dataset elements that are referenced in path """

    try:
        version_list = uuid_set.get_version_list(path.uuid)
    except KeyError:
        lgr.error(
            f"could not locate metadata for dataset with UUID {path.uuid} in "
            f"metadata_store {mapper}:{metadata_store}")
        return

    # Get specified version, if none is specified, take all versions.
    requested_dataset_version = ([path.version]
                                 if path.version is not None
                                 else list(version_list.versions()))

    for dataset_version, prefix_path in version_list.versions_and_prefix_paths():
        if dataset_version not in requested_dataset_version:
            continue
        try:
            _, dataset_path, metadata_root_record = \
                version_list.get_versioned_element(dataset_version, prefix_path)
        except KeyError:
            lgr.error(
                f"could not locate metadata for version {dataset_version} for "
                f"dataset with UUID {path.uuid} in metadata_store "
                f"{mapper}:{metadata_store}")
            continue

        metadata_root_record = cast(MetadataRootRecord, metadata_root_record)

        # Show dataset-level metadata
        yield from show_dataset_metadata(
            mapper,
            metadata_store,
            path.uuid,
            dataset_version,
            prefix_path,
            dataset_path,
            metadata_root_record)

        # Show file-level metadata
        yield from show_file_tree_metadata(
            mapper,
            metadata_store,
            path.uuid,
            dataset_version,
            prefix_path,
            dataset_path,
            metadata_root_record,
            path.local_path,
            recursive)

    return


@build_doc
class Dump(Interface):
    """Dump a dataset's aggregated metadata for dataset and file metadata

    Two types of metadata are supported:

    1. metadata describing a dataset as a whole (dataset-global metadata), and

    2. metadata for files in a dataset (content metadata).

    The DATASET_FILE_PATH_PATTERN argument specifies dataset and file patterns
    that are matched against the dataset and file information in the metadata.
    There are two format, UUID-based and dataset-tree based. The formats are:

        TREE:   ["tree:"] [DATASET_PATH] ["@" VERSION-DIGITS] [":" [LOCAL_PATH]]
        UUID:   "uuid:" UUID-DIGITS ["@" VERSION-DIGITS] [":" [LOCAL_PATH]]

    (The tree-format is the default format and does not require a prefix).
    """

    # Use a custom renderer to emit a self-contained metadata record. The
    # emitted record can be fed into meta-add for example.
    result_renderer = 'tailored'

    _examples_ = [
        dict(
            text='Dump the metadata of the file "dataset_description.json" in '
                 'the dataset "simon". (The queried dataset is determined '
                 'based on the current working directory)',
            code_cmd="datalad meta-dump simon:dataset_description.json"),
        dict(
            text="Sometimes it is helpful to get metadata records formatted "
                 "in a more accessible form, here as pretty-printed JSON",
            code_cmd="datalad -f json_pp meta-dump "
                     "simon:dataset_description.json"),
        dict(
            text="Same query as above, but specify that all datasets should "
                 "be queried for the given path",
            code_cmd="datalad meta-dump :somedir/subdir/thisfile.dat"),
        dict(
            text="Dump any metadata record of any dataset known to the "
                 "queried dataset",
            code_cmd="datalad meta-dump -r"),
        dict(
            text="Dump any metadata record of any dataset known to the "
                 "queried dataset and output pretty-printed JSON",
            code_cmd="datalad -f json_pp meta-dump -r"),
        dict(
            text="Show metadata for all files ending in `.json´ in the root "
                 "directories of all datasets",
            code_cmd="datalad meta-dump *:*.json -r"),
        dict(
            text="Show metadata for all files ending in `.json´ in all "
                 "datasets by not specifying a dataset at all. This will "
                 "start dumping at the top-level dataset.",
            code_cmd="datalad meta-dump :*.json -r")
    ]

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            metavar="DATASET",
            doc="""Dataset for which metadata should be dumped. If no 
                   directory name is provided, the current working directory is 
                   used."""),
        path=Parameter(
            args=("path",),
            metavar="DATASET_FILE_PATH_PATTERN",
            doc="path to query metadata for",
            constraints=EnsureStr() | EnsureNone(),
            nargs="?"),
        recursive=Parameter(
            args=("-r", "--recursive",),
            action="store_true",
            doc="""If set, recursively report on any matching metadata based
                   on given paths or reference dataset. Note, setting this
                   option does not cause any recursion into potential
                   sub-datasets on the filesystem. It merely determines what
                   metadata is being reported from the given/discovered
                   reference dataset."""))

    @staticmethod
    @datasetmethod(name='meta_dump')
    @eval_results
    def __call__(
            dataset=None,
            path="",
            recursive=False):

        metadata_store_path, tree_version_list, uuid_set = get_metadata_objects(
            dataset,
            default_mapper_family)

        parser = MetadataURLParser(path)
        metadata_url = parser.parse()

        if isinstance(metadata_url, TreeMetadataURL):
            yield from dump_from_dataset_tree(
                default_mapper_family,
                metadata_store_path,
                tree_version_list,
                metadata_url,
                recursive)

        elif isinstance(metadata_url, UUIDMetadataURL):
            yield from dump_from_uuid_set(
                default_mapper_family,
                metadata_store_path,
                uuid_set,
                metadata_url,
                recursive)

        return

    @staticmethod
    def custom_result_renderer(res, **_):

        if res["status"] != "ok" or res.get("action", "") != 'meta_dump':
            # logging complained about this already
            return

        ui.message(json.dumps(res["metadata"]))
