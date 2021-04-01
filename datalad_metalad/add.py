# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Add metadata to a metadata model instance.
Metadata is usually provided by an extractor, but
can also be created by other means.
"""
import json
import logging
import sys
from itertools import chain
from os import curdir
from pathlib import Path
from typing import Dict, List, Optional, Union
from uuid import UUID

from dataclasses import dataclass

from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.interface.utils import eval_results
from datalad.distribution.dataset import datasetmethod

from datalad.support.constraints import EnsureBool
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr
)
from datalad.support.param import Parameter

from dataladmetadatamodel.common import get_top_nodes_and_metadata_root_record
from dataladmetadatamodel.connector import Connector
from dataladmetadatamodel.filetree import FileTree
from dataladmetadatamodel.mapper.gitmapper.objectreference import \
    flush_object_references
from dataladmetadatamodel.mapper.gitmapper.utils import lock_backend, \
    unlock_backend
from dataladmetadatamodel.metadata import ExtractorConfiguration, Metadata
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.metadatarootrecord import MetadataRootRecord

from .exceptions import MetadataKeyException


JSONObject = Union[Dict, List]


__docformat__ = "restructuredtext"

default_mapper_family = "git"

lgr = logging.getLogger("datalad.metadata.add")


@dataclass
class AddParameter:
    dataset_id: UUID
    dataset_version: str
    file_path: MetadataPath

    root_dataset_id: Optional[UUID]
    root_dataset_version: Optional[str]
    dataset_tree_path: Optional[MetadataPath]

    extractor_name: str
    extractor_version: str
    extraction_time: float
    extraction_parameter: Dict[str, str]
    agent_name: str
    agent_email: str

    extracted_metadata: dict


@build_doc
class Add(Interface):
    r"""Add metadata to metadata model instance.

    This command reads metadata from a source and adds this metadata
    to a metadata model instance. A source can be: arguments, standard
    input, or a local file. The metadata format is a strings with the
    JSON-serialized dictionary that describes the metadata

    [TODO: add a schema]

    If metadata is read from a source, parameter can overwrite or
    amend information that is stored in the source.

    The METADATA and the ADDITIONAL_VALUES arguments can be pre-fixed by '@',
    in which case the pre-fixed argument is interpreted as a file-name and
    the argument value is read from the file.

    """

    _examples_ = [
        dict(
            text='Add metadata stored in the file "metadata-123.json" to the '
                 'metadata model instance in the current directory.',
            code_cmd="datalad meta-add metadata-123.json"),
        dict(
            text='Add metadata stored in the file "metadata-123.json" to the '
                 'metadata stored in the git-repository "/home/user/dataset_0"',
            code_cmd="datalad meta-add --metadata-store /home/user/dataset_0 "
                     "metadata-123.json"),
        dict(
            text='Add metadata stored in the file "metadata-123.json" to the '
                 'metadata model instance in the current directory and '
                 'overwrite the "dataset_id" value stored in '
                 '"metadata-123.json"',
            code_cmd='datalad meta-add --metadata-store /home/user/dataset_0 '
                     'metadata-123.json \'{"dataset_id": '
                     '"00010203-1011-2021-3031-404142434445"}\''
        ),
        dict(
            text='Add metadata read from standard input to the metadata model '
                 'instance in the current directory',
            code_cmd='datalad meta-add --metadata-store /home/user/dataset_0 '
                     'metadata-123.json @extra-info.json'
        ),
        dict(
            text='Add metadata stored in the file "metadata-123.json" to the '
                 'metadata model instance in the current directory and '
                 'overwrite metadata values with the values stored in '
                 '"extra-info.json"',
            code_cmd='atalad meta-add --metadata-store /home/user/dataset_0 '
                     'metadata-123.json @extra-info.json'
        )
    ]

    required_keys = (
        "type",
        "extractor_name",
        "extractor_version",
        "extraction_parameter",
        "extraction_time",
        "agent_name",
        "agent_email",
        "dataset_id",
        "dataset_version",
        "extracted_metadata")

    optional_keys = (
        "path",)

    required_additional_keys = (
        "root_dataset_id",
        "root_dataset_version",
        "inter_dataset_path")

    required_keys_lines = "\n".join(map(repr, required_keys))
    required_additional_keys_lines = "\n".join(
        map(repr, required_additional_keys))

    _params_ = dict(
        metadata=Parameter(
            args=("metadata",),
            metavar="METADATA",
            doc=f"""Path of a file that contains the metadata that
            should be added to the metadata model instance (the
            metadata must be provided as a JSON-serialized metadata
            dictionary).
            
            If the path is "-", metadata is read from standard input.
            
            The dictionary must contain the following keys:
            
            {required_keys_lines}
            
            If the metadata is associated with a file, the following key
            indicates the file path:
            
            'intra_dataset_path'
            
            It may in addition contain either all or none of the
            following keys (they are used to add metadata element
            as a sub-dataset element, i.e. perform aggregation):
            
            {required_additional_keys_lines}            
            """,
            constraints=EnsureStr() | EnsureNone()),
        metadata_store=Parameter(
            args=("-s", "--metadata-store"),
            doc="""Directory in which the metadata model instance is
            stored. If no directory name is provided, the current working
            directory is used.""",
            constraints=EnsureStr() | EnsureNone()),
        additionalvalues=Parameter(
            args=("additionalvalues",),
            metavar="ADDITIONAL_VALUES",
            doc="""A string that contains a JSON serialized dictionary of
            key value-pairs. These key values-pairs are used in addition to
            the key value pairs in the metadata dictionary to describe
            the metadata that should be added. If an additional key is
            already present in the metadata, it will override the value
            from metadata. In this case a warning is issued.""",
            nargs="?",
            constraints=EnsureStr() | EnsureNone()),
        allow_override=Parameter(
            args=("-o", "--allow-override"),
            doc="""Allow the additional values to override values given in
            metadata.""",
            default=False,
            constraints=EnsureBool() | EnsureNone()),
        allow_unknown=Parameter(
            args=("-u", "--allow-unknown"),
            doc="""Allow unknown keys. By default, unknown keys generate
            an errors. If this switch is True, unknown keys will only be
            reported.""",
            default=False,
            constraints=EnsureBool() | EnsureNone()))

    @staticmethod
    @datasetmethod(name="meta_add")
    @eval_results
    def __call__(
            metadata: Union[str, JSONObject],
            metadata_store: Optional[str] = None,
            additionalvalues: Optional[Union[str, JSONObject]] = None,
            allow_override: bool = False,
            allow_unknown: bool = False):

        additionalvalues = additionalvalues or dict()
        metadata_store = Path(metadata_store or curdir)

        metadata = process_parameters(
            metadata=read_json_object(metadata),
            additional_values=get_json_object(additionalvalues),
            allow_override=allow_override,
            allow_unknown=allow_unknown)

        add_parameter = AddParameter(
            dataset_id=UUID(metadata["dataset_id"]),
            dataset_version=metadata["dataset_version"],
            file_path=(
                MetadataPath(metadata["path"])
                if "path" in metadata
                else None),

            root_dataset_id=(
                UUID(metadata["root_dataset_id"])
                if "root_dataset_id" in metadata
                else None),
            root_dataset_version=metadata.get("root_dataset_version", None),
            dataset_tree_path=MetadataPath(
                metadata.get("inter_dataset_path", "")),

            extractor_name=metadata["extractor_name"],
            extractor_version=metadata["extractor_version"],
            extraction_time=metadata["extraction_time"],
            extraction_parameter=metadata["extraction_parameter"],
            agent_name=metadata["agent_name"],
            agent_email=metadata["agent_email"],

            extracted_metadata=metadata["extracted_metadata"])

        # If the key "path" is present in the metadata
        # dictionary, we assume that the metadata-dictionary describes
        # file-level metadata. Otherwise, we assume that the
        # metadata-dictionary contains dataset-level metadata.
        if add_parameter.file_path:
            yield from add_file_metadata(metadata_store, add_parameter)
        else:
            yield from add_dataset_metadata(metadata_store, add_parameter)
        return


def get_json_object(string_or_object: Union[str, JSONObject]):
    if isinstance(string_or_object, str):
        return json.loads(string_or_object)
    return string_or_object


def read_json_object(path_or_object: Union[str, JSONObject]):
    if isinstance(path_or_object, str):
        if path_or_object == "-":
            metadata_file = sys.stdin
        else:
            metadata_file = open(path_or_object, "tr")
        return json.load(metadata_file)
    return path_or_object


def process_parameters(metadata: dict,
                       additional_values: dict,
                       allow_override: bool,
                       allow_unknown: bool):

    overridden_keys = [
        key
        for key in additional_values
        if key in metadata]

    if overridden_keys:
        if allow_override is False:
            raise MetadataKeyException(
                "Keys overridden by additional values",
                overridden_keys)
        lgr.info(
            "keys overridden in additional values: "
            + ", ".join(overridden_keys))

    # Combine keys
    metadata = {
        **metadata,
        **additional_values
    }

    # Check existence of required keys
    missing_keys = [
        key
        for key in Add.required_keys
        if key not in metadata]

    if missing_keys:
        raise MetadataKeyException(
            "Missing keys",
            missing_keys)

    # Check completeness of non-mandatory keys
    non_mandatory_keys = [
        key
        for key in Add.required_additional_keys
        if key in metadata]

    if non_mandatory_keys:
        non_mandatory_missing_keys = [
            key
            for key in Add.required_additional_keys
            if key not in metadata]

        if non_mandatory_missing_keys:
            raise MetadataKeyException(
                "Non mandatory keys missing",
                non_mandatory_missing_keys)

    # Check for unknown keys:
    unknown_keys = [
        key
        for key in metadata
        if key not in chain(
            Add.required_keys,
            Add.required_additional_keys,
            Add.optional_keys)]

    if unknown_keys:
        if not allow_unknown:
            raise MetadataKeyException("Unknown keys", unknown_keys)
        lgr.warning("Unknown keys in metadata: " + ", ".join(unknown_keys))

    # Check dataset/file consistence
    if metadata["type"] == "file":
        if "path" not in metadata:
            raise MetadataKeyException(
                "Missing path-property in file-type metadata")
    elif metadata["type"] == "dataset":
        if "path" in metadata:
            raise MetadataKeyException(
                "Extraneous path-property in dataset-type metadata")
    else:
        raise MetadataKeyException(f"Unknown type {metadata['type']}")

    return metadata


def _get_top_nodes(realm: str, ap: AddParameter):

    if ap.root_dataset_id is None:
        return get_top_nodes_and_metadata_root_record(
            default_mapper_family,
            realm,
            ap.dataset_id,
            ap.dataset_version,
            MetadataPath(""),
            auto_create=True)

    tree_version_list, uuid_set, mrr = get_top_nodes_and_metadata_root_record(
        default_mapper_family,
        realm,
        ap.root_dataset_id,
        ap.root_dataset_version,
        MetadataPath(""),
        auto_create=True)

    _, dataset_tree = tree_version_list.get_dataset_tree(
        ap.root_dataset_version)

    if ap.dataset_tree_path != MetadataPath("") and ap.dataset_tree_path in dataset_tree:
        mrr = dataset_tree.get_metadata_root_record(ap.dataset_tree_path)
        assert mrr.dataset_identifier == ap.dataset_id
    else:
        dataset_level_metadata = Metadata(default_mapper_family, realm)
        file_tree = FileTree(default_mapper_family, realm)
        mrr = MetadataRootRecord(
            default_mapper_family,
            realm,
            ap.dataset_id,
            ap.dataset_version,
            Connector.from_object(dataset_level_metadata),
            Connector.from_object(file_tree))
        dataset_tree.add_dataset(ap.dataset_tree_path, mrr)
    return tree_version_list, uuid_set, mrr


def add_file_metadata(metadata_store: Path, ap: AddParameter):

    realm = str(metadata_store)
    lock_backend(realm)

    tree_version_list, uuid_set, mrr = _get_top_nodes(realm, ap)

    file_tree = mrr.get_file_tree()
    if file_tree is None:
        file_tree = FileTree(default_mapper_family, realm)
        mrr.set_file_tree(file_tree)

    if ap.file_path in file_tree:
        file_level_metadata = file_tree.get_metadata(ap.file_path)
    else:
        file_level_metadata = Metadata(default_mapper_family, realm)
        file_tree.add_metadata(ap.file_path, file_level_metadata)

    add_metadata_content(file_level_metadata, ap)

    tree_version_list.save()
    uuid_set.save()
    flush_object_references(realm)

    unlock_backend(realm)

    yield {
        "status": "ok",
        "action": "add",
        "type": "file",
        "message": "added file metadata"
    }
    return


def add_dataset_metadata(metadata_store: Path, ap: AddParameter):

    realm = str(metadata_store)
    lock_backend(realm)

    tree_version_list, uuid_set, mrr = _get_top_nodes(realm, ap)

    dataset_level_metadata = mrr.get_dataset_level_metadata()
    if dataset_level_metadata is None:
        dataset_level_metadata = Metadata(default_mapper_family, realm)
        mrr.set_dataset_level_metadata(dataset_level_metadata)

    add_metadata_content(dataset_level_metadata, ap)

    tree_version_list.save()
    uuid_set.save()
    flush_object_references(realm)

    unlock_backend(realm)

    yield {
        "status": "ok",
        "action": "add",
        "type": "dataset",
        "message": "added dataset metadata"
    }
    return


def add_metadata_content(metadata: Metadata, ap: AddParameter):
    metadata.add_extractor_run(
        ap.extraction_time,
        ap.extractor_name,
        ap.agent_name,
        ap.agent_email,
        ExtractorConfiguration(
            ap.extractor_version,
            ap.extraction_parameter),
        ap.extracted_metadata)
