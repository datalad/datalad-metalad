# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Query a dataset's aggregated metadata"""

__docformat__ = 'restructuredtext'

import json
import logging
import sys
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
from dataladmetadatamodel.versionlist import TreeVersionList
from dataladmetadatamodel.mapper.reference import Reference

from .pathutils.metadatapathparser import MetadataPathParser, TreeMetadataPath, UUIDMetadataPath
from .pathutils.treesearch import TreeSearch


default_mapper_family = "git"


lgr = logging.getLogger('datalad.metadata.dump')


def debug_out(message: str, indent: int = 0):
    for line in message.splitlines(keepends=True):
        sys.stderr.write((" " * indent) + line)


def debug_out_json_obj(json_object: JSONObject,
                       indent: int = 0,
                       separator: str = ""):
    debug_out(json.dumps(json_object, indent=4) + separator, indent)


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


def show_dataset_metadata(realm,
                          root_dataset_version,
                          dataset_path,
                          dataset_tree
                          ) -> Generator[dict, None, None]:
    """
    Large object:
    {
       "dataset-1": {
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
       },
       "dataset-2": {
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
       ...
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
           "dataset-info": "some dataset-1 info"
           "extractor1.1":
                {
                   "extraction_time": "11:00:11",
                   "parameter": { some extraction parameter}
                   metadata: [
                      a HUGE metadata blob
                   ]
                }
            }
        }
    }

    Small object 2:
    {
       "dataset-1": {
           "dataset-info": "some dataset-1 info"
           "extractor1.1":
                {
                   "extraction_time": "12:23:34",
                   "parameter": { some other extraction parameter}
                   metadata: [
                      another HUGE metadata blob
                   ]
                }
            }
        }
    }

    """
    metadata_root_record = dataset_tree.value
    dataset_level_metadata = \
        metadata_root_record.dataset_level_metadata.load_object(
            default_mapper_family,
            realm)

    result_json_object = {
        "root_dataset_version": root_dataset_version,
        "dataset_path": dataset_path
    }
    
    for extractor_name, extractor_runs in dataset_level_metadata.extractor_runs():
        for instance in extractor_runs:
            result_json_object["extractor_name"] = {
                "extraction_time": instance.time_stamp,
                "extraction_agent": f"{instance.author_name} <{instance.author_email}>",
                "extractor_version": instance.configuration.version,
                "parameter": instance.configuration.parameter,
                "metadata": instance.metadata_location
            }
            yield result_json_object

    metadata_root_record.dataset_level_metadata.purge()
    # TODO: show file tree: file_tree = metadata_root_record.file_tree.load_object(default_mapper_family, realm)


def dump_from_dataset_tree(mapper: str,
                           realm: str,
                           tree_version_list: TreeVersionList,
                           path: TreeMetadataPath,
                           recursive: bool) -> Generator[dict, None, None]:
    """ Dump dataset tree elements that are referenced in path """

    # Get specified version or default version
    version = path.version
    if version is None:
        version = (
            tuple(tree_version_list.versions())[0]   # TODO: add an item() method to VersionList
            if path.version is None
            else path.version)

    time_stamp, dataset_tree = tree_version_list.get_dataset_tree(version)

    # Get matching dataset names
    tree_search = TreeSearch(dataset_tree)
    matches, not_found_paths = tree_search.get_matching_paths(
        [path.dataset_path], recursive, auto_list_root=False)

    for nf_path in not_found_paths:
        lgr.warning(
            f"could not locate dataset path {nf_path} "
            f"in tree version {path.version} in "
            f"realm {mapper}:{realm}")

    for match_record in matches:
        yield from show_dataset_metadata(
            realm,
            version,
            match_record.path,
            match_record.node)
    return


def dump_from_uuid_set(mapper, realm, uuid_set, path: UUIDMetadataPath, recursive):
    raise NotImplementedError


@build_doc
class Dump(Interface):
    """Query a dataset's aggregated metadata for dataset and file metadata

    Two types of metadata are supported:

    1. metadata describing a dataset as a whole (dataset-global metadata), and

    2. metadata for files in a dataset (content metadata).

    Both types can be queried with this command, and a specific type is
    requested via the `--reporton` argument.

    Examples:

      Dump the metadata of a single file, the queried dataset is determined
      based on the current working directory::

        % datalad meta-dump somedir/subdir/thisfile.dat

      Sometimes it is helpful to get metadata records formatted in a more
      accessible form, here as pretty-printed JSON::

        % datalad -f json_pp meta-dump somedir/subdir/thisfile.dat

      Same query as above, but specify which dataset to query (must be
      containing the query path)::

        % datalad meta-dump -d . somedir/subdir/thisfile.dat

      Dump any metadata record of any dataset known to the queried dataset::

        % datalad meta-dump --recursive --reporton datasets

      Get a JSON-formatted report of metadata aggregates in a dataset, incl.
      information on enabled metadata extractors, dataset versions, dataset
      IDs, and dataset paths::

        % datalad -f json meta-dump --reporton aggregates
    """
    # make the custom renderer the default, path reporting isn't the top
    # priority here
    result_renderer = 'tailored'

    _params_ = dict(
        mapper=Parameter(
            args=("--mapperfamily",),
            metavar="MAPPERFAMILY",
            doc="""mapper family to be used.""",
            constraints=EnsureChoice("git")),
        realm=Parameter(
            args=("--realm",),
            metavar="REALM",
            doc="""realm where the metadata is stored. If not given, realm will be determined
            to be the current working directory."""),
        path=Parameter(
            args=("path",),
            metavar="PATH",
            doc="path(s) to query metadata for",
            nargs="*",
            constraints=EnsureStr() | EnsureNone()),
        reporton=Parameter(
            args=('--reporton',),
            constraints=EnsureChoice('all', 'jsonld', 'datasets', 'files',
                                     'aggregates'),
            doc="""what type of metadata to report on: dataset-global
            metadata only ('datasets'), metadata on dataset content/files only
            ('files'), both ('all', default). 'jsonld' is an alternative mode
            to report all available metadata with JSON-LD markup. A single
            metadata result with the entire metadata graph matching the query
            will be reported, all non-JSON-LD-type metadata will be ignored.
            There is an auxiliary category 'aggregates' that reports on which
            metadata aggregates are present in the queried dataset."""),
        recursive=Parameter(
            args=("-r", "--recursive",),
            action="store_true",
            doc="""if set, recursively report on any matching metadata based
            on given paths or reference dataset. Note, setting this option
            does not cause any recursion into potential subdatasets on the
            filesystem. It merely determines what metadata is being reported
            from the given/discovered reference dataset."""),
        nameonly=Parameter(
            args=("-n", "--nameonly"),
            action="store_true",
            doc="""if set show only the names of files, not any metadata. This
            is similar to ls."""),
        datasetonly=Parameter(
            args=("--datasetonly",),
            action="store_true",
            doc="""if set show only information about datasets, do not list the
            files within the dataset."""))

    @staticmethod
    @datasetmethod(name='meta_dump')
    @eval_results
    def __call__(
            mapper='git',
            realm=None,
            path=None,
            reporton='all',
            recursive=False):

        tree_version_list, uuid_set = get_top_level_metadata_objects(default_mapper_family, realm)

        # We require both entry points to exist for valid metadata
        if tree_version_list is None or uuid_set is None:
            message = f"No {mapper}-mapped datalad metadata model found in: {realm}"
            lgr.warning(message)
            yield dict(
                mapper=mapper,
                realm=realm,
                status='impossible',
                message=message)
            return

        if not path:
            path = ""

        parser = MetadataPathParser(path)
        metadata_path = parser.parse()

        if isinstance(metadata_path, TreeMetadataPath):
            for metadata_info in dump_from_dataset_tree(mapper,
                                                        realm,
                                                        tree_version_list,
                                                        metadata_path,
                                                        recursive):

                debug_out_json_obj(metadata_info, separator="\n")

                yield dict(
                    mapper=mapper,
                    realm=realm,
                    status="ok",
                    metadata=metadata_info)

        elif isinstance(metadata_path, UUIDMetadataPath):
            yield from dump_from_uuid_set(
                mapper,
                realm,
                uuid_set,
                metadata_path,
                recursive
            )

        return
