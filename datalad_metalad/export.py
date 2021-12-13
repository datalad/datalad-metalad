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


import binascii
import json
import hashlib
import logging
import os
from pathlib import Path
from typing import (
    cast,
    Any,
    Dict,
    Generator,
    Iterable,
    Optional,
    Tuple,
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
from dataladmetadatamodel import JSONObject
from dataladmetadatamodel.common import get_top_level_metadata_objects
from dataladmetadatamodel.datasettree import datalad_root_record_name
from dataladmetadatamodel.metadata import (
    Metadata,
    MetadataInstance
)
from dataladmetadatamodel.filetree import FileTree
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.metadatarootrecord import MetadataRootRecord
from dataladmetadatamodel.mtreenode import MTreeNode
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import TreeVersionList

from .exceptions import NoMetadataStoreFound
from .pathutils.metadataurlparser import (
    MetadataURLParser,
    TreeMetadataURL,
    UUIDMetadataURL
)
from .pathutils.mtreesearch import MTreeSearch
from .utils import ensure_mapped


metadata_export_layout_version = "1.0"
default_mapper_family = "git"

lgr = logging.getLogger('datalad.metadata.export')


x = """
def _dataset_report_matcher(node: Any) -> bool:
    return isinstance(node, MetadataRootRecord)


def _file_report_matcher(node: Any) -> bool:
    return isinstance(node, Metadata)


def _create_result_record(mapper: str,
                          metadata_store: Path,
                          metadata_record: JSONObject,
                          element_path: MetadataPath,
                          report_type: str):
    return {
        "status": "ok",
        "action": "meta_dump",
        "backend": mapper,
        "metadata_source": metadata_store,
        "type": report_type,
        "metadata": metadata_record,
        "path": str((metadata_store / element_path).absolute())
    }


def _get_common_properties(root_dataset_identifier: UUID,
                           root_dataset_version: str,
                           metadata_root_record: MetadataRootRecord,
                           dataset_path: MetadataPath) -> dict:

    if dataset_path != MetadataPath(""):
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
                          dataset_path: MetadataPath,
                          metadata_root_record: MetadataRootRecord
                          ) -> Generator[dict, None, None]:

    purge_metadata_root_record = metadata_root_record.ensure_mapped()
    dataset_level_metadata = \
        metadata_root_record.dataset_level_metadata.read_in()

    if dataset_level_metadata is None:
        lgr.warning(
            f"no dataset level metadata for dataset "
            f"uuid:{root_dataset_identifier}@{root_dataset_version}")
        if purge_metadata_root_record:
            metadata_root_record.purge()
        return

    common_properties = _get_common_properties(
        root_dataset_identifier,
        root_dataset_version,
        metadata_root_record,
        dataset_path)

    assert isinstance(dataset_level_metadata, Metadata)

    for extractor_name, extractor_runs in dataset_level_metadata.extractor_runs():
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

    if purge_metadata_root_record:
        metadata_root_record.purge()


def show_file_tree_metadata(mapper: str,
                            metadata_store: Path,
                            root_dataset_identifier: UUID,
                            root_dataset_version: str,
                            dataset_path: MetadataPath,
                            metadata_root_record: MetadataRootRecord,
                            search_pattern: MetadataPath,
                            recursive: bool
                            ) -> Generator[dict, None, None]:

    purge_mrr = metadata_root_record.ensure_mapped()

    dataset_level_metadata = metadata_root_record.dataset_level_metadata
    file_tree = metadata_root_record.file_tree

    if dataset_level_metadata is not None:
        purge_dataset_level_metadata = dataset_level_metadata.ensure_mapped()
    else:
        purge_dataset_level_metadata = False

    if file_tree is not None:
        purge_file_tree = file_tree.ensure_mapped()
    else:
        purge_file_tree = False

    # Do not try to search anything if the file tree is empty
    if not file_tree or not file_tree.mtree.child_nodes:
        if purge_file_tree:
            file_tree.purge()
        if purge_dataset_level_metadata:
            dataset_level_metadata.purge()
        if purge_mrr:
            metadata_root_record.purge()
        return

    # Determine matching file paths
    tree_search = MTreeSearch(file_tree.mtree)
    result_count = 0
    for path, metadata, _ in tree_search.search_pattern(pattern=search_pattern,
                                                        recursive=recursive):
        result_count += 1

        # Ignore empty datasets and ignore paths that do not
        # described metadata, but a directory
        if metadata is None or isinstance(metadata, MTreeNode):
            continue

        assert isinstance(metadata, Metadata)

        common_properties = _get_common_properties(
            root_dataset_identifier,
            root_dataset_version,
            metadata_root_record,
            dataset_path)

        purge_metadata = metadata.ensure_mapped()
        for extractor_name, extractor_runs in metadata.extractor_runs():
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

        if purge_metadata:
            metadata.purge()

    if result_count == 0:
        lgr.warning(
            f"pattern '{str(search_pattern)}' does not match any element "
            f"in file-tree of dataset {metadata_root_record.dataset_identifier}"
            f"@{metadata_root_record.dataset_version} (stored on "
            f"{mapper}:{metadata_store})")

    if purge_file_tree:
        file_tree.purge()

    if purge_dataset_level_metadata:
        dataset_level_metadata.purge()

    if purge_mrr:
        metadata_root_record.purge()


def dump_from_dataset_tree(mapper: str,
                           metadata_store: Path,
                           tree_version_list: TreeVersionList,
                           metadata_url: TreeMetadataURL,
                           recursive: bool) -> Generator[dict, None, None]:
    "-"" Dump dataset tree elements that are referenced in path "-""

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
            time_stamp, dataset_tree = tree_version_list.get_dataset_tree(
                version)
        except KeyError:
            lgr.error(
                f"could not locate metadata for version {version} of "
                f"{metadata_url.dataset_path} in metadata_store "
                f"{mapper}:{metadata_store}")
            continue

        root_mrr = dataset_tree.get_metadata_root_record(MetadataPath(""))
        if root_mrr is None:
            lgr.debug(
                f"no root dataset record found for version "
                f"{version} in metadata store "
                f"{metadata_store}, cannot determine root dataset id")
            purge_root_mrr = False
            root_dataset_version = version
            root_dataset_identifier = "<unknown>"
        else:
            purge_root_mrr = root_mrr.ensure_mapped()
            root_dataset_version = root_mrr.dataset_version
            root_dataset_identifier = root_mrr.dataset_identifier

        # Create a tree search object to search for the specified datasets
        tree_search = MTreeSearch(dataset_tree.mtree)
        result_count = 0
        for path, node, remaining_pattern in tree_search.search_pattern(
                                      pattern=metadata_url.dataset_path,
                                      recursive=recursive,
                                      item_indicator=datalad_root_record_name):
            result_count += 1

            mrr = cast(
                MetadataRootRecord,
                node.get_child(datalad_root_record_name))

            yield from show_dataset_metadata(
                mapper,
                metadata_store,
                root_dataset_identifier,
                root_dataset_version,
                path,
                mrr)

            yield from show_file_tree_metadata(
                mapper,
                metadata_store,
                root_dataset_identifier,
                root_dataset_version,
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

        if purge_root_mrr:
            root_mrr.purge()


def dump_from_uuid_set(mapper: str,
                       metadata_store: Path,
                       uuid_set: UUIDSet,
                       path: UUIDMetadataURL,
                       recursive: bool) -> Generator[dict, None, None]:

    "-"" Dump UUID-identified dataset elements that are referenced in path "-""

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

    for dataset_version in requested_dataset_version:
        try:
            time_stamp, dataset_path, metadata_root_record = \
                version_list.get_versioned_element(dataset_version)
        except KeyError:
            lgr.error(
                f"could not locate metadata for version {dataset_version} for "
                f"dataset with UUID {path.uuid} in metadata_store "
                f"{mapper}:{metadata_store}")
            continue

        assert isinstance(metadata_root_record, MetadataRootRecord)

        # Show dataset-level metadata
        yield from show_dataset_metadata(
            mapper,
            metadata_store,
            path.uuid,
            dataset_version,
            dataset_path,
            metadata_root_record)

        # Show file-level metadata
        yield from show_file_tree_metadata(
            mapper,
            metadata_store,
            path.uuid,
            dataset_version,
            dataset_path,
            metadata_root_record,
            path.local_path,
            recursive)

    return
"""

@build_doc
class Export(Interface):
    """Export a dataset's metadata to file-system objects
    """

    # Use a custom renderer to emit a self-contained metadata record. The
    # emitted record can be fed into meta-add for example.
    result_renderer = 'tailored'

    _examples_ = [
        dict(
            text='Write metadata of the dataset in the current directory '
                 'into the directory /tmp/metadata-export',
            code_cmd="datalad meta-export /tmp/metadata-export"),
    ]

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            metavar="DATASET",
            doc="""Dataset for which metadata should be exported. If no 
            directory name is provided, the dataset is assumed to be located
            in the current working directory."""),
        path=Parameter(
            args=("path",),
            metavar="EXPORT_DESTINATION_DIR",
            doc="""path of a directory where the exported data should be 
            stored, if the directory does not exist, it is created, it is
            an error if the directory already exists.""",
            constraints=EnsureStr()))

    @staticmethod
    @datasetmethod(name='meta_export')
    @eval_results
    def __call__(
            dataset=None,
            path=""):

        path = Path(path)
        metadata_store_path = Path(dataset or ".")

        backend = default_mapper_family
        tree_version_list, uuid_set = get_top_level_metadata_objects(
            backend,
            metadata_store_path)

        # We require both top level entry points to exist for valid metadata
        if tree_version_list is None or uuid_set is None:
            raise NoMetadataStoreFound(
                f"No valid datalad metadata found in: "
                f"{Path(metadata_store_path).resolve()}")

        destination = path.resolve()
        destination.mkdir(parents=True)
        yield from export_metadata(tree_version_list, uuid_set, destination)


def export_metadata(tree_version_list: TreeVersionList,
                    uuid_set: UUIDSet,
                    root: Path):

    write_version(root / "version.json")
    for uuid in uuid_set.uuids():
        export_uuid(root, uuid, uuid_set)
    yield {"exported": "all"}


def write_version(path: Path):
    path.write_text(json.dumps({
        "@id": "MetadataExport",
        "export_layout_version": metadata_export_layout_version
    }))


def export_uuid(root: Path, uuid: str, uuid_set: UUIDSet):

    uuid_path, uuid_file = get_dir_for(str(uuid), (2, 2))
    uuid_path = uuid_path / uuid_file

    version_list = uuid_set.get_version_list(uuid)
    for version, (time_stamp, dataset_path, mappable_object) in version_list.get_versioned_elements():

        mrr = cast(MetadataRootRecord, mappable_object)

        version_path, version_file = get_dir_for(version, (2,))
        version_path = root / uuid_path / version_path / version_file
        version_path.mkdir(parents=True, exist_ok=True)

        with ensure_mapped(mrr):
            object_store_path = version_path / "objects"

            dataset_level_path = export_metadata_instances(
                object_store_path, mrr.dataset_level_metadata)
            (version_path / "dataset-level-metadata.ref").write_text(
                str(dataset_level_path) + "\n")

            if mrr.file_tree:
                file_metadata = export_file_tree(mrr.file_tree, object_store_path)
                (version_path / "file-tree.json").write_text(json.dumps(file_metadata) + "\n")


def export_file_tree(file_tree: FileTree, object_store_path: Path) -> Dict:
    assert file_tree is not None
    with ensure_mapped(file_tree):
        return {
            str(metadata_path): str(export_metadata_instances(object_store_path, metadata))
            for metadata_path, metadata in file_tree.get_paths_recursive()
        }


def export_metadata_instances(object_store: Path, metadata: Metadata) -> Path:
    """Write metadata content to an object store.

    Create a JSON dictionary from metadata object and store it as UTF-8
    encoded text in the given object store.

    :param Path object_store: path of the object store in which the metadata
        should be saved.
    :param Metadata metadata: metadata object that should be stored.
    :return: path to the stored object
    :rtype: Path
    """
    with ensure_mapped(metadata):
        instance_dict = {
            extractor_name: [
                {
                    "time_stamp": instance.time_stamp,
                    "version": instance_set.parameter_set[index].version,
                    "parameter": instance_set.parameter_set[index].parameter,
                    "result": {
                        "author_email": instance.author_email,
                        "author_name": instance.author_name,
                        "metadata_content": instance.metadata_content
                    }
                }
                for index, instance in instance_set.instances.items()
            ]
            for extractor_name, instance_set in metadata.instance_sets.items()
        }
    return save_object(object_store, json.dumps(instance_dict), (2,), "json")


def get_dir_for(name: str,
                parts: Iterable[int]
                ) -> Tuple[Path, Path]:
    """Split-off #parts prefixes from name.

    Split name into #(parts + 1) parts in which len(parts[i]) == parts[i].
    This is mainly used to limit directory entry-numbers by increasing directory
    hierarchy depths.

    :param str name: name to split.
    :param Iterable[int] parts: number and length of parts that should be split
        from the name.
    :raise: ValueError if the sum of parts larger or equal to the length of name
    :return: a 2-tuple in which the first element is the directory tree that
        is created with the split-off parts, the second element is a path that
        consists of the remaining name.
    :rtype: Tuple[Path, Path]
    """

    if sum(parts) >= len(name):
        raise ValueError(
            f"name {name} is too short to be separated into"
            f"parts: {parts}")

    path = Path("")
    position = 0
    for part in parts:
        path = path / name[position:position + part]
        position += part
    return path, Path(name[position:])


def save_object(object_store: Path,
                content: str,
                parts: Iterable[int],
                suffix: Optional[str] = None
                ) -> Path:
    """Save content to object store.

    Save content in an object store with the file name
    that is the sha1sum of the UTF-8 encoded content,
    extended by "." + suffix, if suffix is not None.

    :param Path object_store: path to the object store.
    :param str content: string that should be stored in the object store.
    :param Iterable[int] parts: sub-directory path-parts
    :param Optional[str] suffix: suffix that will be appended with "." to the
        object sha1-name.
    :return: path of the stored object
    :rtype: bool
    """
    digest = hashlib.sha1(content.encode()).digest()
    hash_string = binascii.b2a_hex(digest).decode()

    leading_path, remaining_name = get_dir_for(hash_string, parts)
    if not suffix:
        object_file = leading_path / remaining_name
    else:
        object_file = leading_path / Path(str(remaining_name) + "." + suffix)

    full_dir_path = object_store / leading_path
    full_dir_path.mkdir(parents=True, exist_ok=True)
    (object_store / object_file).write_text(content)
    return object_file
