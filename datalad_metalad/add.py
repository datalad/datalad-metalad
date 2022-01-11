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
from typing import (
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Union
)
from uuid import UUID

from dataclasses import dataclass

from datalad.distribution.dataset import (
    Dataset,
    EnsureDataset,
    datasetmethod
)
from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.interface.utils import eval_results
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr
)
from datalad.support.param import Parameter

from dataladmetadatamodel.common import get_top_nodes_and_metadata_root_record
from dataladmetadatamodel.filetree import FileTree
from dataladmetadatamodel.metadata import (
    ExtractorConfiguration,
    Metadata,
)
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.metadatarootrecord import MetadataRootRecord
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import TreeVersionList
from dataladmetadatamodel.mapper.gitmapper.objectreference import flush_object_references
from dataladmetadatamodel.mapper.gitmapper.utils import locked_backend

from .exceptions import MetadataKeyException
from .utils import (
    check_dataset,
    read_json_objects,
)


JSONObject = Union[Dict, List]


__docformat__ = "restructuredtext"

default_mapper_family = "git"

lgr = logging.getLogger("datalad.metadata.add")


@dataclass
class AddParameter:
    result_path: Path
    destination_path: Path
    allow_id_mismatch: bool

    dataset_id: UUID
    dataset_version: str
    file_path: MetadataPath

    root_dataset_id: Optional[UUID]
    root_dataset_version: Optional[str]
    dataset_path: Optional[MetadataPath]

    extractor_name: str
    extractor_version: str
    extraction_time: float
    extraction_parameter: Dict[str, str]
    agent_name: str
    agent_email: str

    extracted_metadata: dict

    top_node_cache: dict


@build_doc
class Add(Interface):
    r"""Add metadata to a dataset.

    This command reads metadata from a source and adds this metadata
    to a dataset. A source can be: arguments, standard
    input, or a local file.
    The metadata format is a string with the JSON-serialized dictionary
    that describes the metadata.

    In case of an API-call metadata can also be provided in a python
    dictionary or a list of dictionaries.

    [TODO: add a schema]

    If metadata is read from a source, parameter can overwrite or
    amend information that is stored in the source.

    The ADDITIONAL_VALUES arguments can be pre-fixed by '@',
    in which case the pre-fixed argument is interpreted as a file-name and
    the argument value is read from the file.

    The metadata key "dataset-id" must be identical to the ID of the dataset
    that receives the metadata, unless -i or --allow-id-mismatch is provided.
    """

    _examples_ = [
        dict(
            text='Add metadata stored in the file "metadata-123.json" to the '
                 'dataset in the current directory.',
            code_cmd="datalad meta-add metadata-123.json"),
        dict(
            text='Add metadata stored in the file "metadata-123.json" to the '
                 'dataset "/home/user/dataset_0"',
            code_cmd="datalad meta-add -d /home/user/dataset_0 "
                     "metadata-123.json"),
        dict(
            text='Add metadata stored in the file "metadata-123.json" to the '
                 'dataset in the current directory and overwrite the '
                 '"dataset_id" value provided in "metadata-123.json"',
            code_cmd='datalad meta-add -d /home/user/dataset_0 '
                     'metadata-123.json \'{"dataset_id": '
                     '"00010203-1011-2021-3031-404142434445"}\''
        ),
        dict(
            text='Add metadata read from standard input to the dataset '
                 'in the current directory',
            code_cmd='datalad meta-add -'
        ),
        dict(
            text='Add metadata stored in the file "metadata-123.json" to the '
                 'dataset in the current directory and overwrite the values'
                 'from "metadata-123.json" with the values stored in '
                 '"extra-info.json"',
            code_cmd='datalad meta-add metadata-123.json @extra-info.json'
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
        "dataset_path")

    required_keys_lines = "\n".join(map(repr, required_keys))
    required_additional_keys_lines = "\n".join(
        map(repr, required_additional_keys))

    _params_ = dict(
        metadata=Parameter(
            args=("metadata",),
            metavar="METADATA",
            doc=f"""path of the file that contains the
            metadata that should be added to the metadata model instance
            (metadata must be provided as a JSON-serialized metadata
            dictionary). The file may contain a single metadata-record or
            a JSON-array with multiple metadata-records.

            If the path is "-", the metadata file is read from standard input.

            The dictionary must contain the following keys:

            {required_keys_lines}

            If the metadata is associated with a file, the following key
            indicates the file path:

            'path'

            It may in addition contain either all or none of the
            following keys (they are used to add metadata element
            as a sub-dataset element, i.e. perform aggregation):

            {required_additional_keys_lines}            
            """,
            constraints=EnsureStr()),
        additionalvalues=Parameter(
            args=("additionalvalues",),
            metavar="ADDITIONAL_VALUES",
            doc="""A string that contains a JSON serialized dictionary of
            key value-pairs. These key values-pairs are used in addition to
            the key value pairs in the metadata dictionary to describe
            the metadata that should be added. If an additional key is
            already present in the metadata, an error is raised, unless
            -o, --allow-override is provided. In this case, the additional
            values will override the value in metadata and a warning is 
            issued.
            
            NB! If multiple records are provided in METADATA, the
            additional values will be aplied to all of them.""",
            nargs="?",
            constraints=EnsureStr() | EnsureNone()),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc=""""dataset to which metadata should be added. If not
            provided, the dataset is assumed to be given by the current
            directory.""",
            constraints=EnsureDataset() | EnsureNone()),
        allow_override=Parameter(
            args=("-o", "--allow-override",),
            action='store_true',
            doc="""Allow the additional values to override values given in
            metadata.""",
            default=False),
        allow_unknown=Parameter(
            args=("-u", "--allow-unknown",),
            action='store_true',
            doc="""Allow unknown keys. By default, unknown keys generate
            an errors. If this switch is True, unknown keys will only be
            reported. For processing unknown keys will be ignored.""",
            default=False),
        allow_id_mismatch=Parameter(
            args=("-i", "--allow-id-mismatch",),
            action='store_true',
            doc="""Allow insertion of metadata, even if the "dataset-id" in
            the metadata source does not match the ID of the target
            dataset.""",
            default=False),
        batch_mode=Parameter(
            args=("-b", "--batch-mode",),
            action='store_true',
            doc="""Enable batch mode. In batch mode metadata-records are read
            from stdin, one record per line, and a result is written to stdout,
            one result per line. Batch mode can be exited by sending an empty
            line that just consists of a newline. Meta-add will return an empty
            line that just consists of a newline to confirm the exit request.
            When this flag is given, the metadata file name should be set to 
            "-" (minus).""",
            default=False))

    @staticmethod
    @datasetmethod(name="meta_add")
    @eval_results
    def __call__(
            metadata: Union[str, JSONObject],
            additionalvalues: Optional[Union[str, JSONObject]] = None,
            dataset: Optional[Union[str, Dataset]] = None,
            allow_override: bool = False,
            allow_unknown: bool = False,
            allow_id_mismatch: bool = False,
            batch_mode: bool = False):

        additional_values = additionalvalues or dict()

        # Get costly values
        top_node_cache = dict()
        dataset = check_dataset(dataset or curdir, "add metadata")
        metadata_store = dataset.pathobj
        dataset_id = dataset.id
        additional_values_object = get_json_object(additional_values)

        if batch_mode is False:
            all_metadata_objects = read_json_objects(metadata)
        else:
            if metadata != "-":
                lgr.warning(
                    f"Metadata parameter in batch mode is {metadata} instead "
                    f"of '-' (minus), ignoring it.")
            all_metadata_objects = _stdin_reader()

        with locked_backend(metadata_store):
            for metadata_object in all_metadata_objects:
                metadata = process_parameters(
                    metadata=metadata_object,
                    additional_values=additional_values_object,
                    allow_override=allow_override,
                    allow_unknown=allow_unknown)

                lgr.debug(
                    f"attempting to add metadata: '{json.dumps(metadata)}' to "
                    f"metadata store {metadata_store}")

                add_parameter = AddParameter(
                    result_path=(
                        metadata_store
                        / Path(metadata.get("dataset_path", "."))
                        / Path(metadata.get("path", ""))),
                    destination_path=metadata_store,
                    allow_id_mismatch=allow_id_mismatch,

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
                    dataset_path=MetadataPath(
                        metadata.get("dataset_path", "")),

                    extractor_name=metadata["extractor_name"],
                    extractor_version=metadata["extractor_version"],
                    extraction_time=metadata["extraction_time"],
                    extraction_parameter=metadata["extraction_parameter"],
                    agent_name=metadata["agent_name"],
                    agent_email=metadata["agent_email"],

                    extracted_metadata=metadata["extracted_metadata"],

                    top_node_cache=top_node_cache)

                error_result = check_dataset_ids(dataset.pathobj,
                                                 UUID(dataset_id),
                                                 add_parameter)
                if error_result:
                    if not allow_id_mismatch:
                        if batch_mode is True:
                            sys.stdout.write(json.dumps(error_result) + "\n")
                            sys.stdout.flush()
                        else:
                            yield error_result
                        continue
                    lgr.warning(error_result["message"])

                # If the key "path" is present in the metadata
                # dictionary, we assume that the metadata-dictionary describes
                # file-level metadata. Otherwise, we assume that the
                # metadata-dictionary contains dataset-level metadata.
                if add_parameter.file_path:
                    result = tuple(add_file_metadata(dataset.pathobj, add_parameter))
                else:
                    result = tuple(add_dataset_metadata(dataset.pathobj, add_parameter))

                assert len(result) <= 1, f"expected result length <= 1, got: {len(result)}"
                if len(result) == 1:
                    if batch_mode is True:
                        sys.stdout.write(json.dumps(result[0]) + "\n")
                        sys.stdout.flush()
                    else:
                        yield result[0]

            for value in top_node_cache.values():
                tree_version_list, uuid_set = value[0:2]
                tree_version_list.write_out(str(metadata_store))
                uuid_set.write_out(str(metadata_store))

            flush_object_references(metadata_store)

        return


def get_json_object(string_or_object: Union[str, JSONObject]):
    if isinstance(string_or_object, str):
        return json.loads(string_or_object)
    return string_or_object


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


def check_dataset_ids(dataset_path: Path,
                      dataset_id: UUID,
                      add_parameter: AddParameter) -> Optional[dict]:

    if add_parameter.root_dataset_id is not None:
        if add_parameter.root_dataset_id != dataset_id:
            return dict(
                action="meta_add",
                status="error",
                path=str(add_parameter.result_path),
                message=f'value of "root-dataset-id" '
                        f'({add_parameter.root_dataset_id}) does not match '
                        f'ID of dataset at {dataset_path} ({dataset_id})')
    else:
        if add_parameter.dataset_id != dataset_id:
            return dict(
                action="meta_add",
                status="error",
                path=str(add_parameter.result_path),
                message=f'value of "dataset-id" '
                        f'({add_parameter.dataset_id}) does not match '
                        f'ID of dataset at {dataset_path} ({dataset_id})')


def _get_top_nodes(realm: Path,
                   ap: AddParameter
                   ) -> Tuple[TreeVersionList, UUIDSet, MetadataRootRecord]:

    if ap.root_dataset_id is None:
        return get_top_nodes_and_metadata_root_record(
            default_mapper_family,
            str(realm),
            ap.dataset_id,
            ap.dataset_version,
            MetadataPath(""),
            auto_create=True)

    tree_version_list, uuid_set, mrr = get_top_nodes_and_metadata_root_record(
        default_mapper_family,
        str(realm),
        ap.root_dataset_id,
        ap.root_dataset_version,
        MetadataPath(""),
        auto_create=True)

    _, dataset_tree = tree_version_list.get_dataset_tree(
        ap.root_dataset_version)

    if ap.dataset_path != MetadataPath("") and ap.dataset_path in dataset_tree:
        mrr = dataset_tree.get_metadata_root_record(ap.dataset_path)
        if mrr.dataset_identifier != ap.dataset_id:
            raise ValueError(
                f"provided metadata claims that the metadata store contains "
                f"dataset id {ap.dataset_id} at path {ap.dataset_path}, but "
                f"the id of the stored dataset is {mrr.dataset_identifier}")
    else:
        dataset_level_metadata = Metadata()
        file_tree = FileTree()
        mrr = MetadataRootRecord(
            ap.dataset_id,
            ap.dataset_version,
            dataset_level_metadata,
            file_tree)
        dataset_tree.add_dataset(ap.dataset_path, mrr)
    return tree_version_list, uuid_set, mrr


def get_tvl_uuid_mrr_metadata_file_tree(
    metadata_store: Path,
    ap: AddParameter,
) -> Tuple[TreeVersionList, UUIDSet, MetadataRootRecord, Metadata, FileTree]:
    """
    Read tree version list, uuid set, metadata root record, dataset-level
    metadata, and filetree from the metadata store, for the given root
    dataset id, root dataset version, dataset id, dataset version, and
    dataset path.

    This function caches results in order to avoid costly persist operations.

    :param metadata_store: the metadata store
    :param ap: the add parameters
    :return: a tuple containing the tree version list, the uuid set, the
             metadata root records, the dataset level metadata object, and
             the file tree of the dataset given by dataset id and version
    """
    cache_key = (
        metadata_store,
        ap.root_dataset_id,
        ap.root_dataset_version,
        ap.dataset_id,
        ap.dataset_version,
        ap.dataset_path)

    if cache_key not in ap.top_node_cache:

        tree_version_list, uuid_set, mrr = _get_top_nodes(metadata_store, ap)

        dataset_level_metadata = mrr.get_dataset_level_metadata()
        if dataset_level_metadata is None:
            dataset_level_metadata = Metadata()
            mrr.set_dataset_level_metadata(dataset_level_metadata)

        file_tree = mrr.get_file_tree()
        if file_tree is None:
            file_tree = FileTree()
            mrr.set_file_tree(file_tree)

        ap.top_node_cache[cache_key] = (
            tree_version_list,
            uuid_set,
            mrr,
            dataset_level_metadata,
            file_tree)

    return ap.top_node_cache[cache_key]


def add_file_metadata(metadata_store: Path, ap: AddParameter):

    tree_version_list, uuid_set, mrr, metadata, file_tree = \
        get_tvl_uuid_mrr_metadata_file_tree(metadata_store, ap)

    if ap.file_path in file_tree:
        file_level_metadata = file_tree.get_metadata(ap.file_path)
    else:
        file_level_metadata = Metadata()
        file_tree.add_metadata(ap.file_path, file_level_metadata)

    add_metadata_content(file_level_metadata, ap)

    yield {
        "status": "ok",
        "action": "meta_add",
        "type": "file",
        "path": str(ap.result_path),
        "destination": str(ap.destination_path),
        "message": f"added file metadata to {ap.destination_path}"
    }
    return


def add_dataset_metadata(metadata_store: Path, ap: AddParameter):

    tree_version_list, uuid_set, mrr, metadata, file_tree = \
        get_tvl_uuid_mrr_metadata_file_tree(metadata_store, ap)

    add_metadata_content(metadata, ap)

    yield {
        "status": "ok",
        "action": "meta_add",
        "type": "dataset",
        "path": str(ap.result_path),
        "destination": str(ap.destination_path),
        "message": f"added dataset metadata to {ap.destination_path}"
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


def _stdin_reader() -> Generator:
    for line in sys.stdin:
        if line == "\n":
            sys.stdout.write("\n")
            sys.stdout.flush()
            return
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            sys.stdout.write(
                json.dumps({
                    "status": "error",
                    "message": f"not a JSON string: {line}"}) + "\n")
            sys.stdout.flush()
