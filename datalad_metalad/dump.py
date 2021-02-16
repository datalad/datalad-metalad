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
import logging
from typing import Generator

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
from dataladmetadatamodel import JSONObject
from dataladmetadatamodel.connector import Connector
from dataladmetadatamodel.metadata import MetadataInstance
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import TreeVersionList
from dataladmetadatamodel.mapper.reference import Reference

from .pathutils.metadatapathparser import (
    MetadataPathParser,
    TreeMetadataPath,
    UUIDMetadataPath
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


def _create_result_record(mapper: str, realm: str, metadata_record: JSONObject, report_type: str):
    return {
        "status": "ok",
        "action": "meta_dump",
        "source": {
            "mapper": mapper,
            "realm": realm
        },
        "type": report_type,
        "metadata": metadata_record
    }


def _create_metadata_instance_record(instance: MetadataInstance) -> dict:
    return {
        "extraction_time": instance.time_stamp,
        "extraction_agent": f"{instance.author_name} <{instance.author_email}>",
        "extractor_version": instance.configuration.version,
        "extractor_parameter": instance.configuration.parameter,
        "extractor_result": instance.metadata_location
    }


def get_top_level_metadata_objects(mapper_family, realm):
    """
    Load the two top-level elements of the metadata, i.e.
    the tree version list and the uuid list.

    We do this be creating references from known locations
    in the mapper family and loading the referenced objects.
    """
    from dataladmetadatamodel.mapper import get_uuid_set_location, get_tree_version_list_location

    tree_version_list_connector = Connector.from_reference(
        Reference(
            mapper_family,
            "TreeVersionList",
            get_tree_version_list_location(mapper_family)))

    uuid_set_connector = Connector.from_reference(
        Reference(
            mapper_family,
            "UUIDSet",
            get_uuid_set_location(mapper_family)))

    try:
        return (
            tree_version_list_connector.load_object(mapper_family, realm),
            uuid_set_connector.load_object(mapper_family, realm))
    except RuntimeError:
        return None, None


def show_dataset_metadata(mapper,
                          realm,
                          root_dataset_identifier,
                          root_dataset_version,
                          dataset_path,
                          metadata_root_record
                          ) -> Generator[dict, None, None]:

    dataset_level_metadata = \
        metadata_root_record.dataset_level_metadata.load_object(
            default_mapper_family,
            realm)

    result_json_object = {
        "dataset_level_metadata": {
            "root_dataset_identifier": str(root_dataset_identifier),
            "root_dataset_version": root_dataset_version,
            "dataset_identifier": str(metadata_root_record.dataset_identifier),
            "dataset_version": metadata_root_record.dataset_version,
            "dataset_path": dataset_path,
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
            realm,
            result_json_object,
            "dataset"
        )

    # Remove dataset-level metadata when we are done with it
    metadata_root_record.dataset_level_metadata.purge()


def show_file_tree_metadata(mapper,
                            realm,
                            root_dataset_identifier,
                            root_dataset_version,
                            dataset_path,
                            metadata_root_record,
                            file_pattern,
                            recursive
                            ) -> Generator[dict, None, None]:

    file_tree = metadata_root_record.file_tree.load_object(
            default_mapper_family,
            realm)

    # Determine matching file paths
    tree_search = TreeSearch(file_tree)
    matches, not_found_paths = tree_search.get_matching_paths(
        [file_pattern], recursive, auto_list_root=False)

    for missing_path in not_found_paths:
        lgr.warning(
            f"could not locate file path {missing_path} "
            f"in dataset {metadata_root_record.dataset_identifier}"
            f"@{metadata_root_record.dataset_version} in "
            f"realm {mapper}:{realm}")

    for match_record in matches:
        path = match_record.path
        metadata_connector = match_record.node.value

        # Ignore empty datasets
        if metadata_connector is None:
            continue

        metadata = metadata_connector.load_object(default_mapper_family, realm)
        result_json_object = {
            "file_level_metadata": {
                "root_dataset_identifier": str(root_dataset_identifier),
                "root_dataset_version": root_dataset_version,
                "dataset_path": dataset_path,
                "file_path": path
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
                realm,
                result_json_object,
                "file"
            )

    # Remove file tree metadata when we are done with it
    metadata_root_record.file_tree.purge()


def dump_from_dataset_tree(mapper: str,
                           realm: str,
                           tree_version_list: TreeVersionList,
                           path: TreeMetadataPath,
                           recursive: bool) -> Generator[dict, None, None]:
    """ Dump dataset tree elements that are referenced in path """

    # Normalize path representation
    if not path or path.dataset_path is None:
        path = TreeMetadataPath("", "")

    # Get specified version, if none is specified, take the first from the
    # tree version list.
    requested_root_dataset_version = path.version
    if requested_root_dataset_version is None:
        requested_root_dataset_version = (
            tuple(tree_version_list.versions())[0]   # TODO: add an item() method to VersionList
            if path.version is None
            else path.version)

    # Fetch dataset tree for the specified version
    time_stamp, dataset_tree = tree_version_list.get_dataset_tree(requested_root_dataset_version)
    root_mrr = dataset_tree.get_metadata_root_record("")
    root_dataset_version = root_mrr.dataset_version
    root_dataset_identifier = root_mrr.dataset_identifier

    # Create a tree search object to search for the specified datasets
    tree_search = TreeSearch(dataset_tree)
    matches, not_found_paths = tree_search.get_matching_paths(
        [path.dataset_path], recursive, auto_list_root=False)

    for missing_path in not_found_paths:
        lgr.error(
            f"could not locate metadata for dataset path {missing_path} "
            f"in tree version {path.version} in "
            f"realm {mapper}:{realm}")

    for match_record in matches:
        yield from show_dataset_metadata(
            mapper,
            realm,
            root_dataset_identifier,
            root_dataset_version,
            match_record.path,
            match_record.node.value
        )

        # TODO: check the different file paths
        yield from show_file_tree_metadata(
            mapper,
            realm,
            root_dataset_identifier,
            root_dataset_version,
            match_record.path,
            match_record.node.value,
            path.local_path,
            recursive
        )

    return


def dump_from_uuid_set(mapper: str,
                       realm: str,
                       uuid_set: UUIDSet,
                       path: UUIDMetadataPath,
                       recursive: bool) -> Generator[dict, None, None]:

    """ Dump UUID-identified dataset elements that are referenced in path """

    # Get specified version, if none is specified, take the first from the
    # UUID version list.
    try:
        version_list = uuid_set.get_version_list(path.uuid)
    except KeyError:
        lgr.error(
            f"could not locate metadata for dataset with UUID {path.uuid} in "
            f"realm {mapper}:{realm}")
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
            f"could not locate metadata for version {requested_dataset_version} "
            f"for dataset with UUID {path.uuid} in "
            f"realm {mapper}:{realm}")
        return

    # Show dataset-level metadata
    yield from show_dataset_metadata(
        mapper,
        realm,
        path.uuid,
        requested_dataset_version,
        dataset_path,
        metadata_root_record
    )

    # Show file-level metadata
    yield from show_file_tree_metadata(
        mapper,
        realm,
        path.uuid,
        requested_dataset_version,
        dataset_path,
        metadata_root_record,
        path.local_path,
        recursive
    )

    return


@build_doc
class Dump(Interface):
    """Query a dataset's aggregated metadata for dataset and file metadata

    Two types of metadata are supported:

    1. metadata describing a dataset as a whole (dataset-global metadata), and

    2. metadata for files in a dataset (content metadata).

    Both types can be queried with this command, and a specific type is
    requested via the `--reporton` argument.

    The DATASET_FILE_PATH_PATTERN argument specifies dataset and file patterns
    that are matched against the dataset and file information in the metadata. There are two
    format, UUID-based and dataset-tree based. The formats are:

        TREE:   ["tree:"] [DATASET_PATH] ["@" VERSION-DIGITS] [":" [LOCAL_PATH]]
        UUID:   "uuid:" UUID-DIGITS ["@" VERSION-DIGITS] [":" [LOCAL_PATH]]

    (the tree-format is the default format and does not require a prefix).

    The elements DATASET_PATH and LOCAL_PATH take wild-card patterns. So you can
    find all JSON files in the root directory of all datasets by specifying:

        *:*.json

    as DATASET_FILE_PATH_PATTERN and specifying recursive in order to
    go through metadata for all datasets, e.g.:

      % datalad -f json_pp meta-dump *:*.json -r --reporton files

    or simply do not specify a specific dataset at all:

      % datalad -f json_pp meta-dump :*.json -r --reporton files

    Examples:

      Dump the metadata of the file "dataset_description.json" in the
      dataset "simon". (The queried dataset git-repository is determined
      based on the current working directory)::

        % datalad meta-dump --reporton files simon:dataset_description.json

      Sometimes it is helpful to get metadata records formatted in a more
      accessible form, here as pretty-printed JSON::

        % datalad -f json_pp meta-dump simon:dataset_description.json

      Same query as above, but specify that all datasets should be queried
      for the given path::

        % datalad meta-dump -d . :somedir/subdir/thisfile.dat

      Dump any metadata record of any dataset known to the queried dataset::

        % datalad meta-dump -r --reporton datasets

    """
    # make the custom renderer the default, path reporting isn't the top
    # priority here
    result_renderer = 'tailored'

    _params_ = dict(
        backend=Parameter(
            args=("--backend",),
            metavar="BACKEND",
            doc="""metadata storage backend to be used.""",
            constraints=EnsureChoice("git")),
        realm=Parameter(
            args=("--realm",),
            metavar="REALM",
            doc="""realm where the metadata is stored. If not given, realm will be determined
            to be the current working directory."""),
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
            realm=None,
            path="",
            recursive=False):

        realm = realm or "."
        tree_version_list, uuid_set = get_top_level_metadata_objects(default_mapper_family, realm)

        # We require both entry points to exist for valid metadata
        if tree_version_list is None or uuid_set is None:
            message = f"No {backend}-mapped datalad metadata model found in: {realm}"
            lgr.warning(message)
            yield dict(
                backend=backend,
                realm=realm,
                status='impossible',
                message=message)
            return

        parser = MetadataPathParser(path)
        metadata_path = parser.parse()

        if isinstance(metadata_path, TreeMetadataPath):
            yield from dump_from_dataset_tree(
                backend,
                realm,
                tree_version_list,
                metadata_path,
                recursive
            )

        elif isinstance(metadata_path, UUIDMetadataPath):
            yield from dump_from_uuid_set(
                backend,
                realm,
                uuid_set,
                metadata_path,
                recursive
            )

        return
