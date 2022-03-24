# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Export metadata of a dataset to a file-system
"""


__docformat__ = 'restructuredtext'


import binascii
import json
import hashlib
import logging
from uuid import UUID
from pathlib import Path
from typing import (
    cast,
    Dict,
    Iterable,
    Optional,
    Tuple,
)

from datalad.distribution.dataset import datasetmethod
from datalad.interface.base import build_doc
from datalad.interface.base import Interface
from datalad.interface.utils import eval_results
from datalad.support.constraints import EnsureStr
from datalad.support.param import Parameter
from dataladmetadatamodel.common import get_top_level_metadata_objects
from dataladmetadatamodel.metadata import Metadata
from dataladmetadatamodel.filetree import FileTree
from dataladmetadatamodel.mappableobject import ensure_mapped
from dataladmetadatamodel.metadatarootrecord import MetadataRootRecord
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import TreeVersionList

from .exceptions import NoMetadataStoreFound


metadata_export_layout_version = "1.0"
default_mapper_family = "git"

lgr = logging.getLogger('datalad.metadata.export')


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
    }) + "\n")


def export_uuid(root: Path, uuid: UUID, uuid_set: UUIDSet):

    uuid_path, uuid_file = get_dir_for(str(uuid), (2, 2))
    uuid_path = uuid_path / uuid_file

    version_list = uuid_set.get_version_list(uuid)
    for version, (time_stamp, dataset_path, mappable_object) in version_list.versioned_elements:

        mrr = cast(MetadataRootRecord, mappable_object)

        version_path, version_file = get_dir_for(version, (2,))
        version_path = root / uuid_path / version_path / version_file
        version_path.mkdir(parents=True, exist_ok=True)

        with ensure_mapped(mrr):
            object_store_path = version_path / "objects"

            dataset_level_path = export_metadata_instances(
                object_store_path, mrr.dataset_level_metadata)
            (version_path / "dataset-level-metadata.id").write_text(
                str(dataset_level_path) + "\n")

            if mrr.file_tree:
                file_metadata = export_file_tree(mrr.file_tree, object_store_path)
                (version_path / "file-tree.json").write_text(json.dumps(file_metadata) + "\n")


def export_file_tree(file_tree: FileTree, object_store_path: Path) -> Dict:
    assert file_tree is not None
    with ensure_mapped(file_tree):
        return {
            str(metadata_path): str(
                export_metadata_instances(
                    object_store_path,
                    cast(Metadata, metadata)))
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
