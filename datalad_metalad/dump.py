# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Query a dataset's aggregated metadata

We distinguish between two different result formats, large objects and
small objects. Both contain the same information per metadata, but the
large object contains all metadata in a single object, while the small
objects contain only per-item, e.g. dataset or file, metadata together
with context information.

The large objects do not repeat information, but can become hard to
manage in datasets with millions of subdatasets or files.

The small objects are easy to manage, but identical information is
repeated in them.

We currently only support small objects

Large object:
{
   "dataset-1": {
       "dataset_level_metadata": {
           "dataset-info": "some dataset info"
           "extractor1.1": [
                {
                   "extraction_time": "11:00:11",
                   "parameter": { some extraction parameter}
                   metadata: [
                      a HUGE metadata blob
                   ]
                },
                {
                   "extraction_time": "12:23:34",
                   "parameter": { some other extraction parameter}
                   metadata: [
                      another HUGE metadata blob
                   ]
                },
                { more runs}
           ]
           "extractor1.2": ...
       }
       "file_tree": {  LARGE object with file-based metadata }
   },
   "dataset-2": {
       "dataset_level_metadata": {
           "dataset-info": "another dataset info"
           "extractor2.1": [
                {
                   "extraction_time": "1998",
                   "parameter": { some extraction parameter}
                   metadata: [
                      a HUGE metadata blob
                   ]
                },
                { more runs}
           ]
           "extractor2.2": ...
       },
       "file_tree": {  LARGE object with file-based metadata }
}

Such an object would be extremely large, especially if it
contains metadata.

The second approach focuses on minimal result sizes, but
result repetition and therefore also information duplication.
The non-splitable information is the metadata blob.
For example:

Small object 1:
{
    "dataset-1": {
        "dataset_level_metadata": {
            "dataset-info": "some dataset-1 info"
            "extractor1.1": {
                "extraction_time": "11:00:11",
                "parameter": { some extraction parameter}
                "metadata":  " a HUGE metadata blob "
             }
        }
    }
}

Small object 2:
{
    "dataset-1": {
        "dataset_level_metadata": {
            "dataset-info": "some dataset-1 info"
            "extractor1.2": {
                "extraction_time": "12:23:34",
                "parameter": { some other extraction parameter}
                "metadata":  " another HUGE metadata blob "
             }
        }
    }
}


We use the small object approach below
"""


__docformat__ = 'restructuredtext'

import enum
import json
import logging
from typing import Generator
from uuid import UUID

from datalad.distribution.dataset import datasetmethod
from datalad.interface.base import build_doc
from datalad.interface.base import Interface
from datalad.interface.utils import eval_results
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
    EnsureChoice,
)
from datalad.support.param import Parameter
from datalad.ui import ui
from dataladmetadatamodel import JSONObject
from dataladmetadatamodel.metadata import MetadataInstance
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.metadatarootrecord import MetadataRootRecord
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import TreeVersionList

from .metadata import get_top_level_metadata_objects
from .pathutils.metadataurlparser import (
    MetadataURLParser,
    TreeMetadataURL,
    UUIDMetadataURL
)
from .pathutils.treesearch import TreeSearch

default_mapper_family = "git"

lgr = logging.getLogger('datalad.metadata.dump')


class ReportPolicy(enum.Enum):
    INDIVIDUAL = "individual"
    COMPLETE = "complete"


class ReportOn(enum.Enum):
    FILES = "files"
    DATASETS = "datasets"
    ALL = "all"


def _create_result_record(mapper: str,
                          metadata_store: str,
                          metadata_record: JSONObject,
                          element_path: str,
                          report_type: str):
    return {
        "status": "ok",
        "action": "meta_dump",
        "source": {
            "mapper": mapper,
            "metadata_store": metadata_store
        },
        "type": report_type,
        "metadata": metadata_record,
        "path": element_path
    }


def _create_metadata_instance_record(instance: MetadataInstance) -> dict:
    return {
        "extraction_time": instance.time_stamp,
        "extraction_agent_name": instance.author_name,
        "extraction_agent_email": instance.author_email,
        "extractor_version": instance.configuration.version,
        "extraction_parameter": instance.configuration.parameter,
        "extraction_result": instance.metadata_content
    }


def show_dataset_metadata(mapper: str,
                          metadata_store: str,
                          root_dataset_identifier: UUID,
                          root_dataset_version: str,
                          dataset_path: MetadataPath,
                          metadata_root_record: MetadataRootRecord
                          ) -> Generator[dict, None, None]:

    dataset_level_metadata = \
        metadata_root_record.dataset_level_metadata.load_object()

    if dataset_level_metadata is None:
        lgr.warning(
            f"no dataset level metadata for dataset "
            f"uuid:{root_dataset_identifier}@{root_dataset_version}")
        return

    result_json_object = {
        "dataset_level_metadata": {
            "root_dataset_metadata_store": metadata_store,
            "root_dataset_identifier": str(root_dataset_identifier),
            "root_dataset_version": root_dataset_version,
            "dataset_identifier": str(metadata_root_record.dataset_identifier),
            "dataset_version": metadata_root_record.dataset_version,
            "dataset_path": str(dataset_path),
        }
    }

    for extractor_name, extractor_runs in dataset_level_metadata.extractor_runs():

        instances = [
            _create_metadata_instance_record(instance)
            for instance in extractor_runs
        ]

        result_json_object["dataset_level_metadata"]["metadata"] = {
            extractor_name: instances
        }

        yield _create_result_record(
            mapper,
            metadata_store,
            result_json_object,
            str(dataset_path),
            "dataset")

    # Remove dataset-level metadata when we are done with it
    metadata_root_record.dataset_level_metadata.purge()


def show_file_tree_metadata(mapper: str,
                            metadata_store: str,
                            root_dataset_identifier: UUID,
                            root_dataset_version: str,
                            dataset_path: MetadataPath,
                            metadata_root_record: MetadataRootRecord,
                            search_pattern: str,
                            recursive: bool
                            ) -> Generator[dict, None, None]:

    file_tree = metadata_root_record.file_tree.load_object()

    # Determine matching file paths
    tree_search = TreeSearch(file_tree)
    matches, not_found_paths = tree_search.get_matching_paths(
        [search_pattern], recursive, auto_list_root=False)

    for missing_path in not_found_paths:
        lgr.warning(
            f"could not locate file path {missing_path} "
            f"in dataset {metadata_root_record.dataset_identifier}"
            f"@{metadata_root_record.dataset_version} in "
            f"metadata_store {mapper}:{metadata_store}")

    for match_record in matches:
        path = match_record.path
        metadata_connector = match_record.node.value

        # Ignore empty datasets
        if metadata_connector is None:
            continue

        metadata = metadata_connector.load_object()
        result_json_object = {
            "file_level_metadata": {
                "root_dataset_identifier": str(root_dataset_identifier),
                "root_dataset_version": root_dataset_version,
                "dataset_identifier": str(
                    metadata_root_record.dataset_identifier),
                "dataset_version": metadata_root_record.dataset_version,
                "dataset_path": str(dataset_path),
                "path": str(path)
            }
        }

        for extractor_name, extractor_runs in metadata.extractor_runs():
            instances = [
                _create_metadata_instance_record(instance)
                for instance in extractor_runs
            ]

            result_json_object["file_level_metadata"]["metadata"] = {
                extractor_name: instances
            }

            yield _create_result_record(
                mapper,
                metadata_store,
                result_json_object,
                str(dataset_path / path),
                "file")

        # Remove metadata object after all instances are reported
        metadata_connector.purge()

    # Remove file tree metadata when we are done with it
    metadata_root_record.file_tree.purge()


def dump_from_dataset_tree(mapper: str,
                           metadata_store: str,
                           tree_version_list: TreeVersionList,
                           metadata_url: TreeMetadataURL,
                           recursive: bool) -> Generator[dict, None, None]:
    """ Dump dataset tree elements that are referenced in path """

    # Normalize path representation
    if not metadata_url or metadata_url.dataset_path is None:
        metadata_url = TreeMetadataURL(MetadataPath(""), MetadataPath(""))

    # Get specified version, if none is specified, take the first from the
    # tree version list.
    requested_root_dataset_version = metadata_url.version
    if requested_root_dataset_version is None:
        requested_root_dataset_version = (
            # TODO: add an item() method to VersionList
            tuple(tree_version_list.versions())[0]
            if metadata_url.version is None
            else metadata_url.version)

    # Fetch dataset tree for the specified version
    time_stamp, dataset_tree = tree_version_list.get_dataset_tree(
        requested_root_dataset_version)
    root_mrr = dataset_tree.get_metadata_root_record(MetadataPath(""))
    root_dataset_version = root_mrr.dataset_version
    root_dataset_identifier = root_mrr.dataset_identifier

    # Create a tree search object to search for the specified datasets
    tree_search = TreeSearch(dataset_tree)
    matches, not_found_paths = tree_search.get_matching_paths(
        [str(metadata_url.dataset_path)], recursive, auto_list_root=False)

    for missing_path in not_found_paths:
        lgr.error(
            f"could not locate metadata for dataset path {missing_path} "
            f"in tree version {metadata_url.version} in "
            f"metadata_store {mapper}:{metadata_store}")

    for match_record in matches:
        yield from show_dataset_metadata(
            mapper,
            metadata_store,
            root_dataset_identifier,
            root_dataset_version,
            match_record.path,
            match_record.node.value)

        yield from show_file_tree_metadata(
            mapper,
            metadata_store,
            root_dataset_identifier,
            root_dataset_version,
            MetadataPath(match_record.path),
            match_record.node.value,
            str(metadata_url.local_path),
            recursive)

    return


def dump_from_uuid_set(mapper: str,
                       metadata_store: str,
                       uuid_set: UUIDSet,
                       path: UUIDMetadataURL,
                       recursive: bool) -> Generator[dict, None, None]:

    """ Dump UUID-identified dataset elements that are referenced in path """

    # Get specified version, if none is specified, take the first from the
    # UUID version list.
    try:
        version_list = uuid_set.get_version_list(path.uuid)
    except KeyError:
        lgr.error(
            f"could not locate metadata for dataset with UUID {path.uuid} in "
            f"metadata_store {mapper}:{metadata_store}")
        return

    requested_dataset_version = path.version
    if requested_dataset_version is None:
        requested_dataset_version = (
            tuple(version_list.versions())[0]
            if path.version is None
            else path.version)

    try:
        time_stamp, dataset_path, metadata_root_record = \
            version_list.get_versioned_element(requested_dataset_version)
    except KeyError:
        lgr.error(
            f"could not locate metadata for version "
            f"{requested_dataset_version} for dataset with "
            f"UUID {path.uuid} in metadata_store {mapper}:{metadata_store}")
        return

    # Show dataset-level metadata
    yield from show_dataset_metadata(
        mapper,
        metadata_store,
        path.uuid,
        requested_dataset_version,
        dataset_path,
        metadata_root_record)

    # Show file-level metadata
    yield from show_file_tree_metadata(
        mapper,
        metadata_store,
        path.uuid,
        requested_dataset_version,
        dataset_path,
        metadata_root_record,
        str(path.local_path),
        recursive)

    return


@build_doc
class Dump(Interface):
    """Query a dataset's aggregated metadata for dataset and file metadata

    Two types of metadata are supported:

    1. metadata describing a dataset as a whole (dataset-global metadata), and

    2. metadata for files in a dataset (content metadata).

    The DATASET_FILE_PATH_PATTERN argument specifies dataset and file patterns
    that are matched against the dataset and file information in the metadata.
    There are two format, UUID-based and dataset-tree based. The formats are:

        TREE:   ["tree:"] [DATASET_PATH] ["@" VERSION-DIGITS] [":" [LOCAL_PATH]]
        UUID:   "uuid:" UUID-DIGITS ["@" VERSION-DIGITS] [":" [LOCAL_PATH]]

    (the tree-format is the default format and does not require a prefix).
    """

    # Use a custom renderer to emit a self-contained metadata record. The
    # emitted record can be fed into meta-add for example.
    result_renderer = 'tailored'

    _examples_ = [
        dict(
            text='Dump the metadata of the file "dataset_description.json" in '
                 'the dataset "simon". (The queried dataset git-repository is '
                 'determined based on the current working directory)',
            code_cmd="datalad meta-dump simon:dataset_description.json"),
        dict(
            text="Sometimes it is helpful to get metadata records formatted "
                 "in a more accessible form, here as pretty-printed JSON",
            code_cmd="datalad -f json_pp meta-dump "
                     "simon:dataset_description.json"),
        dict(
            text="Same query as above, but specify that all datasets should "
                 "be queried for the given path",
            code_cmd="datalad meta-dump -d . :somedir/subdir/thisfile.dat"),
        dict(
            text="Dump any metadata record of any dataset known to the "
                 "queried dataset",
            code_cmd="datalad meta-dump -r"),
        dict(
            text="Show metadata for all datasets",
            code_cmd="datalad -f json_pp meta-dump -r"),
        dict(
            text="Show metadata for all files ending in `.json´ in the root "
                 "directories of all datasets",
            code_cmd="datalad -f json_pp meta-dump *:*.json -r"),
        dict(
            text="Show metadata for all files ending in `.json´ in all "
                 "datasets by not specifying a dataset at all. This will "
                 "start dumping at the top-level dataset.",
            code_cmd="datalad -f json_pp meta-dump :*.json -r")
    ]

    _params_ = dict(
        backend=Parameter(
            args=("--backend",),
            metavar="BACKEND",
            doc="""metadata storage backend to be used.""",
            constraints=EnsureChoice("git")),
        metadata_store=Parameter(
            args=("-m", "--metadata-store"),
            metavar="METADATA_STORE",
            doc="""Directory in which the metadata model instance is
            stored (often this is the same directory as the dataset
            directory). If no directory name is provided, the current working
            directory is used."""),
        path=Parameter(
            args=("path",),
            metavar="DATASET_FILE_PATH_PATTERN",
            doc="path to query metadata for",
            constraints=EnsureStr() | EnsureNone(),
            nargs='?'),
        recursive=Parameter(
            args=("-r", "--recursive",),
            action="store_true",
            doc="""if set, recursively report on any matching metadata based
            on given paths or reference dataset. Note, setting this option
            does not cause any recursion into potential subdatasets on the
            filesystem. It merely determines what metadata is being reported
            from the given/discovered reference dataset."""))

    @staticmethod
    @datasetmethod(name='meta_dump')
    @eval_results
    def __call__(
            backend="git",
            metadata_store=None,
            path="",
            recursive=False):

        metadata_store = metadata_store or "."
        tree_version_list, uuid_set = get_top_level_metadata_objects(
            default_mapper_family,
            metadata_store)

        # We require both entry points to exist for valid metadata
        if tree_version_list is None or uuid_set is None:

            message = (
                f"No {backend}-mapped datalad metadata "
                f"model found in: {metadata_store}")
            lgr.warning(message)

            yield dict(
                action="meta_dump",
                status='impossible',
                backend=backend,
                metadata_store=metadata_store,
                message=message)
            return

        parser = MetadataURLParser(path)
        metadata_url = parser.parse()

        if isinstance(metadata_url, TreeMetadataURL):
            yield from dump_from_dataset_tree(
                backend,
                metadata_store,
                tree_version_list,
                metadata_url,
                recursive)

        elif isinstance(metadata_url, UUIDMetadataURL):
            yield from dump_from_uuid_set(
                backend,
                metadata_store,
                uuid_set,
                metadata_url,
                recursive)

        return

    @staticmethod
    def custom_result_renderer(res, **kwargs):

        if res["status"] != "ok" or res.get("action", "") != 'meta_dump':
            # logging complained about this already
            return

        render_dataset_level_metadata(
            res["metadata"].get("dataset_level_metadata", dict()))

        render_file_level_metadata(
            res["metadata"].get("file_level_metadata", dict()))


def render_dataset_level_metadata(dl_metadata: dict):
    if not dl_metadata:
        return

    result_base = dict(
        type="dataset",
        dataset_id=dl_metadata["dataset_identifier"],
        dataset_version=dl_metadata["dataset_version"])

    render_common_metadata(dl_metadata, result_base)


def render_file_level_metadata(fl_metadata: dict):
    if not fl_metadata:
        return

    result_base = dict(
        type="file",
        dataset_id=fl_metadata["dataset_identifier"],
        dataset_version=fl_metadata["dataset_version"],
        path=fl_metadata["path"])

    render_common_metadata(fl_metadata, result_base)


def render_common_metadata(metadata: dict, result_base: dict):

    if result_base["dataset_version"] != metadata["root_dataset_version"]:
        assert metadata["dataset_path"] != ""
        result_base["root_dataset_id"] = metadata["root_dataset_identifier"]
        result_base["root_dataset_version"] = metadata[
            "root_dataset_version"]
        result_base["dataset_path"] = metadata["dataset_path"]

    for extractor_name, extractions in metadata["metadata"].items():
        for extraction in extractions:
            extraction_result = dict(
                extractor_name=extractor_name,
                extractor_version=extraction["extractor_version"],
                extraction_parameter=extraction["extraction_parameter"],
                extraction_time=extraction["extraction_time"],
                agent_name=extraction["extraction_agent_name"],
                agent_email=extraction["extraction_agent_email"],
                extracted_metadata=extraction["extraction_result"])

            ui.message(json.dumps({
                **result_base,
                **extraction_result
            }))
