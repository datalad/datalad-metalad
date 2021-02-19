# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Interface for aggregating metadata from (sub)dataset into (super)datasets


Aggregating a subdataset (sds) into the UUID set of the root dataset (rds)
is relativly simple. (There is a possible error condition, where different UUIDS would
be added to the same rds-pd-version at the same path).


Assumption:

The sds are real subdatasets of the rds



Issue 1: path determination
----------------------------

For UUID-set aggregation and for tree-version-list aggregation, the new path
of the sds metadata has to be determined.

UUIDSet:

     UUID    ---connected to--> [{sds-pd-version, path, metadata-root-record}, ...]

Since the sds UUID-set metadata has no path yet in the aggregated rds metadata,
the path has to be determined. For a given uuid and a given rds, with metadata
md(rds) all possible paths would be:

    relevant-rds-versions = for all versions(rds) where sds-version in [sds-pd-version of uuid]

    for rds-version in relevant-rds-versions:
        for sds-pd-version, _, metadata-root-record in version-list(uuid):
            if sds-pd-version is subdataset of rds-version:
                sds-path = path of sds-pd-version in rds-version
                add metadata_root_record to uuidset(rds).sds-pd-version, sds-path
            else:
                Error("Cannot find path of sds-uuid@sds-pd-version in any rds@version)
                Error("What can you do? Not much besides re-aggregating")
                Error("What can we do? Add a structure that allows for 'detached' metadata")



UUID setis more complex with multiple primary data versions that can carry
metadata.

"""

__docformat__ = 'restructuredtext'


import logging
from typing import Generator, List


import dataclasses


from datalad.interface.base import Interface
from datalad.interface.utils import (
    eval_results
)
from datalad.interface.base import build_doc
from datalad.interface.common_opts import (
    recursion_limit,
    recursion_flag
)
from datalad.distribution.dataset import (
    datasetmethod
)
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureStr,
    EnsureNone
)
from datalad.support.constraints import EnsureChoice
from dataladmetadatamodel.versionlist import TreeVersionList
from dataladmetadatamodel.uuidset import UUIDSet

from .metadata import get_top_level_metadata_objects


lgr = logging.getLogger('datalad.metadata.aggregate')


@dataclasses.dataclass
class AggregateItem:
    source_tree_version_list: TreeVersionList
    source_uuid_set: UUIDSet
    destination_path: str


@build_doc
class Aggregate(Interface):
    """Aggregate metadata of one or more (sub)datasets for later reporting.

    Metadata aggregation refers to a procedure that extracts metadata present
    in a dataset into a portable representation that is stored in a
    standardized (internal) format. Moreover, metadata aggregation can also
    extract metadata in this format from one dataset and store it in another
    (super)dataset. Based on such collections of aggregated metadata it is then
    possible to discover particular (sub)datasets and individual files in them,
    without having to obtain the actual dataset repositories first (see the
    DataLad 'meta-dump' command).

    To enable aggregation of metadata that are contained in files of a dataset,
    one has to enable one or more metadata extractor for a dataset. DataLad
    supports a number of common metadata standards, such as the Exchangeable
    Image File Format (EXIF), Adobe's Extensible Metadata Platform (XMP), and
    various audio file metadata systems like ID3. DataLad extension packages
    can provide metadata data extractors for additional metadata sources.
    The list of metadata extractors available to a particular DataLad
    installation is reported by the 'wtf' command ('datalad wtf').

    Enabling a metadata extractor for a dataset is done by adding its name to
    the 'datalad.metadata.nativetype' configuration variable in the dataset's
    configuration file (.datalad/config), e.g.::

      [datalad "metadata"]
        nativetype = exif
        nativetype = xmp

    If an enabled metadata extractor is not available in a particular DataLad
    installation, metadata extraction will not succeed in order to avoid
    inconsistent aggregation results.

    Enabling multiple extractors is supported. In this case, metadata are
    extracted by each extractor individually, and stored alongside each other.
    Metadata aggregation will also extract DataLad's internal metadata
    ('metalad_core'), and git-annex file metadata ('metalad_annex').

    Metadata aggregation can be performed recursively, in order to aggregate
    all metadata from all subdatasets. By default, re-aggregation of metadata
    inspects modifications of datasets and metadata extractor parameterization
    with respect to the last aggregated state. For performance reasons,
    re-aggregation will be automatically skipped, if no relevant change is
    detected. This default behavior can be altered via the ``--force``
    argument.

    Depending on the versatility of the present metadata and the number of
    dataset or files, aggregated metadata can grow prohibitively large or take
    a long time to process. See the documentation of the ``extract-metadata``
    command for a number of configuration settings that can be used to tailor
    this process on a per-dataset basis.

    *Aggregation of aggregates*

    Sometimes it is desirable to re-use existing metadata aggregates, instead
    of performing a metadata extraction, even if a particular dataset is
    locally available (e.g. because large files are not downloaded, or
    extraction runtime is prohibitively long). Such behavior is enabled through
    a special, rsync-like, path specification syntax. Consider three nested
    datasets: `top` / `mid` / `bottom`. The syntax for aggregating a record on
    `bottom` from `mid` into `top`, while being in the root top `top`, is::

        datalad meta-aggregate mid/bottom

    In order to use an aggregate on `bottom` from `bottom` itself, or to
    trigger re-extraction in case of detected changes, add a trailing
    path separator to the path. In POSIX-compatible machines, this looks like::

        datalad meta-aggregate mid/bottom/

    """
    _params_ = dict(
        backend=Parameter(
            args=("--backend",),
            metavar="BACKEND",
            doc="""metadata storage backend to be used. Currently only
            "git" is supported.""",
            constraints=EnsureChoice("git")),
        realm=Parameter(
            args=("--realm",),
            metavar="REALM",
            doc="""realm where metadata will be aggregated into.
            If not given, realm will be determined to be the
            current working directory."""),
        path=Parameter(
            args=("path",),
            metavar="PATH",
            doc="""path to (sub)datasets whose metadata shall be
            aggregated. When a given path is pointing into a dataset (instead of
            to its root), the metadata of the containing dataset will be
            aggregated.""",
            nargs="*",
            constraints=EnsureStr() | EnsureNone()),
        recursive=recursion_flag,
        recursion_limit=recursion_limit)

    @staticmethod
    @datasetmethod(name='meta_aggregate')
    @eval_results
    def __call__(
            backend="git",
            realm=None,
            path=None,
            recursive=False,
            recursion_limit=None):

        # TODO: for now we assume that a path describes:
        #  a) a path to a git repo that contains dataset metadata
        #  b) the path of the dataset within the super dataset.
        #  _
        #  Conceptually, these are two different things, the
        #  sub-dataset path is independent from the metadata
        #  storage. The assumptions just make it easier for now
        #  to focus on the aggregate implementation.

        assert len(path) % 2 == 0, "you must provide the same number of repos as paths"
        path_realm_associations = tuple(zip(
            map(
                lambda index_value: index_value[1],
                filter(
                    lambda index_value: index_value[0] % 2 == 0,
                    enumerate(path))),
            map(
                lambda index_value: index_value[1],
                filter(
                    lambda index_value: index_value[0] % 2 == 1,
                    enumerate(path)))))

        realm = realm or "."

        # Load destination
        tree_version_list, uuid_set = get_top_level_metadata_objects(backend, realm)
        if tree_version_list is None or uuid_set is None:
            message = f"No {backend}-mapped datalad metadata model found in: {realm}"
            lgr.error(message)
            yield dict(
                backend=backend,
                realm=realm,
                status='impossible',
                message=message)
            return

        # Collect aggregate information
        aggregate_items = []
        for ag_path, ag_realm in path_realm_associations:

            ag_tree_version_list, ag_uuid_set = get_top_level_metadata_objects(
                backend,
                ag_realm)

            if ag_tree_version_list is None or ag_uuid_set is None:
                message = f"No {backend}-mapped datalad metadata model found in: {ag_realm}, " \
                          f"ignoring metadata location {ag_realm}."
                lgr.warning(message)
                yield dict(
                    backend=backend,
                    realm=realm,
                    status='error',
                    message=message)
                continue

            aggregate_items.append(
                AggregateItem(
                    ag_tree_version_list,
                    ag_uuid_set,
                    ag_path))

        yield from perform_aggregation(
            tree_version_list,
            uuid_set,
            aggregate_items)

        tree_version_list.save()
        uuid_set.save()

        yield dict(
            backend=backend,
            realm=realm,
            status='ok',
            message="aggregation performed")

        return


def perform_aggregation(tree_version_list: TreeVersionList,
                        uuid_set: UUIDSet,
                        aggregate_items: List[AggregateItem]
                        ) -> Generator[dict, None, None]:

    yield from copy_tree_version_lists(
        tree_version_list,
        aggregate_items)

    yield from copy_uuid_sets(
        uuid_set,
        aggregate_items)


def copy_tree_version_lists(tree_version_list: TreeVersionList,
                            aggregate_items: List[AggregateItem]
                            ) -> Generator[dict, None, None]:

    for aggregate_item in aggregate_items:
        copy_tree_version_list(
            tree_version_list,
            aggregate_item.source_tree_version_list,
            aggregate_item.destination_path
        )


def copy_uuid_sets(uuid_set: UUIDSet,
                   aggregate_items: List[AggregateItem]
                   ) -> Generator[dict, None, None]:

    for aggregate_item in aggregate_items:
        copy_uuid_set(
            uuid_set,
            aggregate_item.source_uuid_set,
            aggregate_item.destination_path
        )


def copy_tree_version_list(destination_tree_version_list: TreeVersionList,
                           source_tree_version_list: TreeVersionList,
                           destination_path: str):
