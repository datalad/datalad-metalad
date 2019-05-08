# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Interface for aggregating metadata from (sub)dataset into (super)datasets
"""

__docformat__ = 'restructuredtext'

import logging
import tempfile
from six import (
    iteritems,
    text_type,
)
from collections import (
    OrderedDict,
)

import os.path as op

import shutil

# API commands we need
from .extract import Extract
from datalad.core.local.status import Status
from datalad.core.local.save import Save
from datalad.core.local.diff import (
    _diff_ds,
)

import datalad
from datalad.interface.base import Interface
from datalad.interface.utils import (
    eval_results,
    discover_dataset_trace_to_targets,
)
from datalad.interface.base import build_doc
from datalad.interface.common_opts import (
    recursion_limit,
    recursion_flag,
)
from datalad.interface.results import (
    success_status_map,
)
from . import (
    exclude_from_metadata,
    location_keys,
    ReadOnlyDict,
    _val2hashable,
)
from .utils import sort_paths_by_datasets
from .report import (
    get_ds_aggregate_db_locations,
    get_ds_aggregate_db,
)
from datalad.distribution.dataset import (
    Dataset,
    datasetmethod,
    EnsureDataset,
    require_dataset,
)
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureStr,
    EnsureNone,
    EnsureBool,
)
from datalad.support.constraints import EnsureChoice
from datalad.support import json_py
from datalad.support.digests import Digester
from datalad.utils import (
    assure_list,
    rmtree,
    Path,
    PurePosixPath,
    as_unicode,
)
from datalad.log import log_progress

lgr = logging.getLogger('datalad.metadata.aggregate')


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
    DataLad 'meta-report' command).

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
    """
    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""topmost dataset metadata will be aggregated into. If no
            dataset is specified, a datasets will be discovered based on the
            current working directory.""",
            constraints=EnsureDataset() | EnsureNone()),
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
        recursion_limit=recursion_limit,
        into=Parameter(
            args=('--into',),
            constraints=EnsureChoice('top', 'all'),
            doc="""which datasets shall receive the aggregated metadata:
            all datasets from any leaf dataset to the top-level target dataset
            including all intermediate datasets (all), or just the top-level
            dataset (top)."""),
        force=Parameter(
            args=('--force',),
            constraints=EnsureChoice(
                'extraction', 'fromscratch', 'ignoreextractorchange', None),
            doc="""Disable specific optimizations: 'extraction' overrides
            change detection and engages all enabled extractors regardless of
            whether an actual change in a dataset's state is detected with
            respect to any existing metadata aggregate; 'fromscratch' wipes out
            any existing metadata aggregates first, including aggregates for
            unavailable datasets (implies 'extraction').
            'ignoreextractorchange' disables comparison of current and
            recorded extractor parametrization and avoids re-extraction
            due to extractor changes alone."""),
    )

    @staticmethod
    @datasetmethod(name='meta_aggregate')
    @eval_results
    def __call__(
            path=None,
            dataset=None,
            recursive=False,
            recursion_limit=None,
            into='top',
            force=None):

        ds = require_dataset(
            dataset, check_installed=True, purpose='metadata aggregation')

        # path args could be
        # - installed datasets
        # - names of pre-aggregated dataset that are not around
        # - -like rev-status they should match anything underneath them

        # Step 1: figure out which available dataset is closest to a given path
        if path:
            extract_from_ds, errors = sort_paths_by_datasets(
                dataset, assure_list(path))
            for e in errors:  # pragma: no cover
                e.update(
                    logger=lgr,
                    refds=ds.path,
                )
                yield e
        else:
            extract_from_ds = OrderedDict({ds.pathobj: []})

        # convert the values into sets to ease processing below
        extract_from_ds = {
            Dataset(k): set(assure_list(v))
            for k, v in iteritems(extract_from_ds)
        }

        #
        # Step 1: figure out which available dataset need to be processed
        #
        ds_with_pending_changes = set()
        # note that depending on the recursion settings, we may not
        # actually get a report on each dataset in question
        detector = Status()(
            # pass arg in as-is to get proper argument semantics
            dataset=dataset,
            # query on all paths to get desired result with recursion
            # enables
            path=path,
            # never act on anything untracked, we cannot record its identity
            untracked='no',
            # we are not interested in a more expensive test for untracked content
            # in any subdataset. Any metadata extraction is driven by the content
            # recorded for a particular state. Limiting status detection to
            # just commits boosts performance considerably
            eval_subdataset_state='commit',
            recursive=recursive,
            recursion_limit=recursion_limit,
            result_renderer='disabled',
            return_type='generator',
            # let the top-level caller handle failure
            on_failure='ignore')
        for s in detector:
            # TODO act on generator errors?

            # path reports are always absolute and anchored on the dataset
            # (no repo) path
            ds_candidate = Dataset(s['parentds'])

            # ignore anything that isn't all clean, otherwise we have no
            # reliable record of an ID
            if s['state'] != 'clean':
                if ds_candidate not in ds_with_pending_changes:
                    yield dict(
                        action='meta_aggregate',
                        path=ds_candidate.path,
                        type='dataset',
                        logger=lgr,
                        status='error' if ds_candidate == ds else 'impossible',
                        message='dataset has pending changes',
                    )
                    ds_with_pending_changes.add(ds_candidate)
                    if ds_candidate == ds:
                        # this is the target dataset, this is the only one that
                        # we cannot skip, as extraction can and will happen
                        return
                continue

            # we always know that the parent was modified too
            fromds = extract_from_ds.get(ds_candidate, set())
            if s['type'] == 'dataset':
                # record that this dataset could have info on this subdataset
                # TODO at the moment this unconditional inclusion makes it
                # impossible to just pick a single unavailable dataset from the
                # aggregated metadata in an available dataset, it will always
                # include all of them as candidates. We likely need at least
                # some further testing below which metadata aggregates are
                # better then those already present in the aggregation target
                # related issue: when no subdataset is present, but a path to
                # an unavailable subsub...dataset is given, the most top-level
                # subdataset and the identified subdataset that is contained by
                # the top-level sub will have a record and their metadata will
                # be aggregated. This likely is not an actual issue, though...
                fromds.add(Dataset(s['path']))
            else:
                # extract from this available dataset information on
                # itself
                fromds.add(ds_candidate)
            extract_from_ds[ds_candidate] = fromds

        # shed all records of datasets with pending changes, those have either
        # led to warning and depending on the user's desired might have stopped
        # the aggregation machinery
        extract_from_ds = {
            # remove all aggregation subjects for datasets that are actually
            # available
            k: set([i for i in v if i not in extract_from_ds])
            for k, v in iteritems(extract_from_ds)
            # remove all datasets that have been found to have pending
            # modifications
            if k not in ds_with_pending_changes}
        # at this point extract_from_ds is a dict where the keys are
        # locally available datasets that matched the `path` and `recursion`
        # configuration given as arguments (as Dataset() instances), and
        # values are lists of datasets for which to extract aggregated
        # metadata. The key/source dataset is not listed and always
        # implied. Values can be Dataset() instances, which identified
        # registered (possibly unavailable) subdatasets. Values can also
        # be Path object that correspond to input arguments that have to
        # be matched against path of dataset on which there is aggregated
        # metadata in the source/key dataset. Such Paths are always assigned
        # to the closest containing available dataset

        if recursive:
            # when going recursive, we also want to capture all pre-aggregated
            # metadata underneath the discovered datasets
            # this trick looks for dataset records that have no query path
            # assigned already (which will be used to match aggregate records)
            # and assign them their own path). This will match all stored
            # records and captures the aggregate --recursive case without
            # a dedicated `path` argument.
            extract_from_ds = {
                k:
                v
                if len([i for i in v if not isinstance(i, Dataset)])
                else v.union([k.pathobj])
                for k, v in iteritems(extract_from_ds)
            }

        # this will gather all subdataset that have been found removed
        # and used as a filter to disable the aggregation of their associated
        # metadata objects
        vanished_datasets = set()

        # the content info lookup cache
        cache = {}
        # HERE IS WHERE THE "INTO" MODES NEED TO BE TREATED DIFFERENTLY
        if into == 'top':
            for res in _do_top_aggregation(
                    ds, extract_from_ds, force, vanished_datasets, cache):
                yield res
        elif into == 'all':
            # which datasets have we dealt with already
            processed_ds = set()
            # adjust extract_from_ds appropriately for individual
            # call to _do_top_aggregation for each dataset in the affected
            # hierarchy from the bottom up

            # discover all affected dataset, these may not have been
            # found by the status() call above, dependening on the given path
            # argument
            spec = {}
            known_dataset_paths = [d.path for d in extract_from_ds]
            discover_dataset_trace_to_targets(
                ds.path,
                known_dataset_paths,
                [],
                spec,
            )

            # we don't care for the actual dataset linkage (edges), just the
            # datasets involved (nodes)
            to_update = set()
            for k, v in iteritems(spec):
                to_update.add(k)
                to_update.update(v)
            # make sure to include all the datasets we already knew about
            to_update.update(known_dataset_paths)

            # loop over all datasets we need to aggregate into from the bottom
            # up
            for topds in sorted(to_update, reverse=True):
                topds = Dataset(topds)

                # compose an instruction set for this dataset's aggregation
                task = {k: v for k, v in iteritems(extract_from_ds)
                        if topds == k or topds.pathobj in k.pathobj.parents}

                # before we aggregate this dataset, we must make sure that any
                # potential subdataset changes get saved, so that we have
                # a defined subdataset state that will not change upon
                # re-aggregation with not other modification
                for res in Save()(
                        dataset=topds,
                        path=[d.pathobj for d in processed_ds
                         if topds.pathobj in d.pathobj.parents],
                        message="Update subdataset state after metadata aggregation",
                        # never recursive
                        recursive=False,
                        # we cannot have anything new
                        updated=True,
                        # these are only submodule commits
                        to_git=True):
                    res['refds'] = ds.path
                    yield res

                for res in _do_top_aggregation(
                        topds,
                        task,
                        force,
                        vanished_datasets,
                        cache):
                    # recode results to it becomes clear via which dataset
                    # aggregation was invoked
                    res['refds'] = ds.path
                    yield res
                processed_ds.add(topds)

                # for the next round we only need to note that data on the
                # current topds is available from itself
                if topds in extract_from_ds:
                    extract_from_ds[topds].add(topds)
        else:
            raise ValueError('Unkown aggregation mode: --into {}'.format(
                into))


def _do_top_aggregation(ds, extract_from_ds, force, vanished_datasets, cache):
    """Internal helper

    Performs non-recursive aggergation for a single dataset.

    Parameters
    ----------
    ds : Dataset
      Top-level reference dataset
    extract_from_ds : dict
      Aggregation instructions, what to take from where.
    force : str
      Force-mode label, just relayed from the command API
    vanished_datasets : set
      Path instances of datasets that are known to have been removed
      from any inspected dataset since the last aggregated state. This
      is modified in-place!
    cache: dict
      content into lookup cache
    """
    if force == 'fromscratch':
        # all we have to do is to remove the directory from the working
        # tree
        metadir = ds.pathobj / '.datalad' / 'metadata'
        if metadir.exists():
            rmtree(text_type(metadir))

    # load the info that we have on the top-level dataset's aggregated
    # metadata
    top_agginfo_db = get_ds_aggregate_db(ds.pathobj, warn_absent=False)

    # XXX keep in mind that recursion can
    # - traverse the file system
    # - additionally end up recursion into pre-aggregated metadata

    # this will assemble all aggregation records
    agginfo_db = {}
    # this will gather no longer needed metadata object files
    obsolete_objs = set()

    # this for loop does the heavy lifting (extraction/aggregation)
    # wrap in progress bar
    log_progress(
        lgr.info,
        'metadataaggregation',
        'Start metadata aggregation into %s', ds,
        total=len(extract_from_ds),
        label='Metadata aggregation',
        unit=' datasets',
    )
    # start from the top the get a chance to load info on any existing
    # metadata aggregates
    for aggsrc in sorted(extract_from_ds, key=lambda x: x.path):
        log_progress(
            lgr.info,
            'metadataaggregation',
            'Aggregate from dataset %s', aggsrc,
            update=1,
            increment=True)
        # get extractor change and compare to recorded state
        # if there is a change, no diff'ing is needed and extraction
        # can start right away
        # TODO implies ignoreextractorchange force flag
        exinfo = {
            e['extractor']: dict(
                e,
                # check if unique value aggregation was disabled in the
                # receiving dataset
                unique=ds.config.obtain(
                    'datalad.metadata.aggregate-unique-{}'.format(
                        e['extractor'].replace('_', '-')),
                    default=True,
                    valtype=EnsureBool())
            )
            # important to get this info from the source dataset and not the
            # receiver, there is no reason to assume that both have the same
            # extractors enabled
            for e in aggsrc.meta_extract(
                process_type='extractors',
                result_renderer='disabled')
        }
        exstate_rec = top_agginfo_db.get(
            aggsrc.pathobj, {}).get('extractors', None)
        if (
                # old aggregate catalag with a plain extractor name list
                not isinstance(exstate_rec, dict) \
                or sorted(exinfo.keys()) != sorted(exstate_rec.keys()) \
                or any(exinfo[k]['state'] != exstate_rec[k]
                       for k in exinfo)):
            lgr.debug(
                'Difference between recorded and current extractor detected, '
                'force (re-)extraction (was: %s; is: %s)',
                exstate_rec, exinfo.get('state', {}))
            force = 'extraction'
        # check extraction is actually needed, by running a diff on the
        # dataset against the last known refcommit, to see whether it had
        # any metadata relevant changes
        last_refcommit = top_agginfo_db.get(
            aggsrc.pathobj, {}).get('refcommit', None)
        have_diff = False
        # we are instructed to take pre-aggregated metadata
        use_self_aggregate = aggsrc in extract_from_ds[aggsrc]
        # TODO should we fall back on the PRE_COMMIT_SHA in case there is
        # no recorded refcommit. This might turn out to be more efficient,
        # as it could avoid working with dataset that have no
        # metadata-relevant content
        # skip diff'ing when extraction is forced
        if not use_self_aggregate and force != 'extraction' and last_refcommit:
            lgr.debug('Diff %s against refcommit %s', aggsrc, last_refcommit)
            # the following diff duplicates the logic in get_refcommit()
            # however, we need to look deeper into the diff against the
            # refcommit to find removed subdatasets
            exclude_paths = [
                aggsrc.pathobj / PurePosixPath(e)
                for e in (
                    list(exclude_from_metadata) + assure_list(
                        aggsrc.config.get('datalad.metadata.exclude-path', []))
                )
            ]
            for res in _diff_ds(
                    ds=aggsrc,
                    fr=last_refcommit,
                    to='HEAD',
                    constant_refs=False,
                    recursion_level=0,
                    # query on all paths to get desired result with
                    # recursion enables
                    origpaths=None,
                    untracked='no',
                    annexinfo=None,
                    # not possible here, but turn off detection anyways
                    eval_file_type=False,
                    cache=cache):
                if res.get('action', None) != 'diff':  # pragma: no cover
                    # something unexpected, send upstairs
                    yield res
                if res['state'] == 'clean':
                    # not an actual diff
                    continue
                p = Path(res['path'])
                if not have_diff and \
                        p not in exclude_paths and \
                        not any(e in p.parents for e in exclude_paths):
                    # this is a difference that could have an impact on
                    # metadata stop right here and proceed to extraction
                    have_diff = True
                    lgr.debug('Found metadata relevant diff in %s: %s -- %s',
                              aggsrc, res, exclude_paths)
                    # we cannot break, we have to keep looking for
                    # deleted subdatasets
                    #break

                # if we find a deleted subdataset, we have to wipe them off
                # the todo list, stage their objects for deletion, and
                # remove them from the DB
                if res['type'] == 'dataset' and res['state'] == 'deleted':
                    rmdspath = Path(res['path'])
                    lgr.debug(
                        'Found removed subdataset %s, stage metadata '
                        'aggregates for deletion', rmdspath)
                    if rmdspath in top_agginfo_db:
                        # stage its metadata object files for deletion
                        for objtype in ('dataset_info', 'content_info'):
                            obj_path = top_agginfo_db[rmdspath].get(
                                objtype, None)
                            if obj_path is None:
                                # nothing to act on
                                continue
                            obsolete_objs.add(Path(obj_path))
                        # wipe out dataset entry from DB
                        del top_agginfo_db[rmdspath]
                    # prevent further processing of downward datasets
                    # if a dataset is gone, it can only be listed as
                    # a value, but not as a key in extract_from_ds
                    vanished_datasets.add(rmdspath)

        if not use_self_aggregate and (
                force == 'extraction' or last_refcommit is None or have_diff):
            lgr.debug(
                'Extract metadata from %s '
                '(use_self_aggregate=%s, force=%s, last_refcommit=%s, '
                'have_diff=%s)',
                aggsrc, use_self_aggregate, force, last_refcommit, have_diff)
            # really _extract_ metadata for aggsrc
            agginfo = {}
            for res in _extract_metadata(aggsrc, ds, exinfo):
                if res.get('action', None) == 'meta_extract' \
                        and res.get('status', None) == 'ok' \
                        and 'info' in res:
                    agginfo = res['info']
                    # identify the dataset by ID in the aggregation record
                    agginfo['id'] = aggsrc.id
                # report if fishy
                if success_status_map.get(res['status'], False) != 'success':
                    yield res
            # logic based on the idea that there will only be one
            # record per dataset (extracted or from pre-aggregate)
            if aggsrc.pathobj in agginfo_db:
                lgr.debug(
                    'Replace existing metadata aggregate for %s '
                    'with new extract.', aggsrc)
            # place in DB under full path, needs to become relative
            # to any updated dataset later on
            agginfo_db[aggsrc.pathobj] = agginfo
        else:
            # we already have what we need for this locally available dataset
            yield dict(
                action="meta_extract",
                path=aggsrc.path,
                status='notneeded',
                type='dataset',
                logger=lgr,
            )

        # filter out any dataset records that belong to a vanished dataset
        # or to a dataset underneath a vanished dataset
        lgr.debug('Discovered records of removed datasets: %s',
                  vanished_datasets)
        obsolete_datasets = {
            d for d in top_agginfo_db
            if d in vanished_datasets
            or any(p in vanished_datasets for p in d.parents)
        }
        lgr.debug('Present, now obsolete, dataset records: %s',
                  obsolete_datasets)
        for od in obsolete_datasets:
            for objtype in ('dataset_info', 'content_info'):
                obj_path = top_agginfo_db[od].get(objtype, None)
                if obj_path is None:
                    # nothing to act on
                    continue
                obsolete_objs.add(Path(obj_path))
            del top_agginfo_db[od]

        # if there is a path in aggsubjs match it against all datasets on
        # which we have aggregated metadata, and expand aggsubjs with a
        # list of such dataset instances
        subjs = []
        for subj in extract_from_ds[aggsrc]:
            if not isinstance(subj, Dataset):
                subjs.extend(
                    Dataset(aggds) for aggds in top_agginfo_db
                    # TODO think about distinguishing a direct match
                    # vs this match of any parent (maybe the
                    # latter/current only with --recursive)
                    if Path(aggds) == subj \
                    or subj in Path(aggds).parents
                )
            else:
                subjs.append(subj)

        if not subjs:
            continue

        src_agginfo_db = get_ds_aggregate_db(aggsrc.pathobj, warn_absent=False)

        referenced_objs = set()
        # loop over aggsubjs and pull aggregated metadata for them
        # sorting is not really needed
        for dssubj in sorted(set(subjs), key=lambda x: x.path):
            if dssubj.pathobj in agginfo_db:
                # logic based on the idea that there can only be one
                # record per dataset (extracted or from pre-aggregate)
                # if we have one already, it was extracted.
                # we can get here during recursion
                lgr.debug(
                    'Prefer extracted metadata record over pre-aggregated '
                    'one for %s', dssubj.pathobj)
                continue
            # at this point two things can be the case (simultaneously):
            # - we have aggregated metadata in the topds
            # - we have aggregated metadata in the srcds
            # both of which can be different degrees of outdated
            # -> we prefer the topds record, because we diff'ed against
            # its refcommit, and the fact that agginfo_db does not have an
            # update means that there was no change
            # if topds does not have anything, we fall back on the srcds
            agginfo = top_agginfo_db.get(dssubj.pathobj, None)
            if agginfo is None:
                agginfo = src_agginfo_db.get(dssubj.pathobj, None)
            if agginfo is None:
                # TODO proper error/warning result: we don't have metadata
                # for a locally unavailable dataset
                continue
            agginfo_db[dssubj.pathobj] = agginfo
            for objtype in ('dataset_info', 'content_info'):
                if objtype in agginfo:
                    referenced_objs.add(Path(agginfo[objtype]))
        # make sure all referenced metadata objects are actually around
        # use a low-level report methods for speed, as we should know
        # that everything is local to aggsrc
        # only attempt, if anything is referenced at all
        if referenced_objs and hasattr(aggsrc, 'get'):
            lgr.debug('Ensure availability of referenced metadata objects: %s',
                      referenced_objs)
            # only query for objects in the src repo (we might also reference
            # up-to-date ones in the topds)
            togetobjs = [
                text_type(o.relative_to(aggsrc.pathobj))
                for o in referenced_objs
                if aggsrc.pathobj in o.parents
            ]
            if togetobjs:
                res = aggsrc.repo.get(togetobjs)
                # TODO evaluate the results, but ATM get() output is not
                # very helpful. Do when it gives proper results
    log_progress(
        lgr.info,
        'metadataaggregation',
        'Finished metadata aggregation into %s', ds,
    )
    # at this point top_agginfo_db has everything on the previous
    # aggregation state that is still valid, and agginfo_db everything newly
    # found in this run

    # procedure
    # 1. whatever is in agginfo_db goes into top_agginfo_db
    # 2. if top_agginfo_db gets an entry replaced, we delete the associated
    #    files (regular unlink)
    # 3. stuff that moved into the object tree gets checksumed and placed
    #    at the target destination
    # 4. update DB file

    # this is where incoming metadata objects would be
    aggtmp_basedir = _get_aggtmp_basedir(ds, mkdir=False)

    # top_agginfo_db has the status quo, agginfo_db has all
    # potential changes
    for srcds, agginfo in iteritems(agginfo_db):
        # Check of object files have to be slurped in from TMP
        for objtype in ('dataset_info', 'content_info'):
            obj_path = agginfo.get(objtype, None)
            if obj_path is None:
                # nothing to act on
                continue
            obj_path = Path(obj_path)

            # TODO obtain file content, possibly in a concerted
            # effort for all source datasets at once

            # we treat all metadata objects alike, regardless of
            # whether we just build them in TMP or import them from
            # another object store. This makes sure that we
            # get homogeneous objects stores (as much as possible)
            # while still being able to improve layout and
            # fileformats without maintaining version-dependent
            # processing conditions -- for the price of having to
            # checksum each file

            # checksum and place in obj tree
            shasum = Digester(
                digests=['sha1'])(text_type(obj_path))['sha1']
            target_obj_location = _get_obj_location(
                ds, obj_path, shasum)
            # already update location in incoming DB now
            # a potential file move is happening next
            agginfo[objtype] = target_obj_location

            if op.lexists(text_type(target_obj_location)):
                # we checksum by content, if it exists, it is identical
                # use exist() to be already satisfied by a dangling symlink
                lgr.debug(
                    "Metadata object already exists at %s, skipped",
                    target_obj_location)
                continue
            # get srcfile into the object store
            target_obj_location.parent.mkdir(parents=True, exist_ok=True)
            # if it is from TMP we can move the file and slim down
            # the storage footprint ASAP
            (shutil.move
             if aggtmp_basedir in obj_path.parents
             else shutil.copyfile)(
                # in TMP
                text_type(obj_path),
                # in object store
                text_type(target_obj_location)
            )

        if srcds in top_agginfo_db:
            old_srcds_info = top_agginfo_db[srcds]
            # we already know something about this dataset
            # check if referenced objects need to be deleted
            # replace this record with the incoming one
            for objtype in ('dataset_info', 'content_info'):
                if objtype not in old_srcds_info:
                    # all good
                    continue
                if agginfo[objtype] != old_srcds_info[objtype]:
                    # the old record references another file
                    # -> mark for deletion
                    # Rational: it could be that the exact some dataset
                    # (very same commit) appears as two sub(sub)dataset at
                    # different locations. This would mean we have to scan
                    # the entire DB for potential match and not simply
                    # delete it here instead we have to gather candidate
                    # for deletion and check that they are no longer
                    # referenced at the very end of the DB update
                    obsolete_objs.add(Path(old_srcds_info[objtype]))
        # replace the record
        top_agginfo_db[srcds] = agginfo_db[srcds]

    # we are done with moving new stuff into the store, clean our act up
    if aggtmp_basedir.exists():
        rmtree(text_type(aggtmp_basedir))

    # TODO THIS NEEDS A TEST
    obsolete_objs = [
        obj for obj in obsolete_objs
        if all(all(dinfo.get(objtype, None) != obj
                   for objtype in ('dataset_info', 'content_info'))
               for d, dinfo in iteritems(top_agginfo_db))
    ]
    for obsolete_obj in obsolete_objs:
        # remove from the object store
        # there is no need to fiddle with `remove()`, rev-save will do that
        # just fine on its own
        lgr.debug("Remove obsolete metadata object %s", obsolete_obj)
        obsolete_obj.unlink()
        try:
            # make an attempt to kill the parent dir too, to leave the
            # object store clean(er) -- although git won't care
            # catch error, in case there is more stuff in the dir
            obsolete_obj.parent.rmdir()
        except OSError:  # pragma: no cover
            # this would be expected and nothing to make a fuzz about
            pass
    # store the updated DB
    _store_agginfo_db(ds, top_agginfo_db)

    # and finally save the beast
    something_changed = False
    for res in Save()(
            dataset=ds,
            # be explicit, because we have to take in untracked content,
            # and there might be cruft lying around
            path=[
                ds.pathobj / '.datalad' / 'metadata' / 'aggregate_v1.json',
                ds.pathobj / '.datalad' / 'metadata' / 'objects',
            ],
            message="Update aggregated metadata",
            # never recursive, this call might be triggered from a more
            # complex algorithm that does a "better" recursion and there
            # should be nothing to recurse into for the given paths
            recursive=False,
            # we need to capture new/untracked content
            updated=False,
            # leave this decision to the dataset config
            to_git=None):

        # inspect these results to figure out if anything was actually
        # done, we rely on save as a proxy to figure this out. If save
        # doesn't do anything, nothing was necessary, and the various tests
        # above should have minimized the actual work -> issue NOTNEEDED vs
        # OK to make it easy for a caller to act on a relevant change vs no
        # change
        if res['action'] == 'save' and res.get('status', None) == 'ok':
            something_changed = True
        yield res
    yield dict(
        action='meta_aggregate',
        status='ok' if something_changed else 'notneeded',
        path=ds.path,
        type='dataset',
    )


def _get_aggtmp_basedir(ds, mkdir=False):
    """Return a pathlib Path for a temp directory to put aggregated metadata"""
    tmp_basedir = ds.pathobj / '.git' / 'tmp' / 'aggregate-metadata'
    if mkdir:
        tmp_basedir.mkdir(parents=True, exist_ok=True)
    return tmp_basedir


def _extract_metadata(fromds, tods, exinfo):
    """Extract metadata from a dataset into a temporary location in a dataset

    Parameters
    ----------
    fromds : Dataset
      To extract from
    tods : Dataset
      Aggregate into
    exinfo : dict
      Extractor information, as reported by
      meta_extract(process_type=extractors)

    Yields
    ------
    dict
      Any extraction error status results will be re-yielded, otherwise
      result dict 'info' key will contain version, extractors, dataset and
      content metadata object file locations.
    """
    # this will gather information on the extraction result
    info = {}
    meta = {
        'dataset': None,
        'content': [],
    }
    extracted_metadata_sources = set()

    unique_cm = {}

    # perform the actual extraction
    for res in fromds.meta_extract(
            # just let it do its thing
            path=None,
            # None indicates to honor a datasets per-extractor configuration
            # and to be on by default
            process_type=None,
            # error handlingis done upstairs
            on_failure='ignore',
            return_type='generator',
            result_renderer='disabled'):
        if success_status_map.get(res['status'], False) != 'success':
            yield res
            continue
        restype = res.get('type', None)
        extracted_metadata_sources.update(
            # assumes that any non-JSONLD-internal key is a metadata
            # extractor, which should be valid
            (k for k in res.get('metadata', {}) if not k.startswith('@')))
        if restype == 'dataset':
            if meta['dataset'] is not None:  # pragma: no cover
                res.update(
                    message=(
                        'Metadata extraction from %s yielded second dataset '
                        'metadata set',
                        fromds),
                    status='error',
                )
                yield res
                continue
            refcommit = res.get('refcommit', None)
            if refcommit:
                lgr.debug('Update %s refcommit to %s', fromds, refcommit)
                # place recorded refcommit in info dict to facilitate
                # subsequent change detection
                info['refcommit'] = refcommit
            else:
                lgr.debug(
                    'Could not determine a reference commit for the metadata '
                    'extracted from %s', fromds)

            meta['dataset'] = res['metadata']
        elif restype == 'file':
            fmeta = res['metadata']
            # build-up unique values
            _update_unique_cm(unique_cm, fmeta, exinfo)

            meta['content'].append(
                dict(
                    fmeta,
                    path=text_type(Path(res['path']).relative_to(
                        fromds.pathobj))
                )
            )
        else:  # pragma: no cover
            res.update(
                message=(
                    'Metadata extraction from %s yielded unexpected '
                    'result type (%s), ignored record',
                    fromds, restype),
                status='error',
            )
            yield res
            continue
    # inject unique values into dataset metadata
    if unique_cm:
        # produce final unique record in dsmeta for this extractor
        meta['dataset']['datalad_unique_content_properties'] = \
            _finalize_unique_cm(unique_cm, meta['dataset'])

    # store esssential extraction config in dataset record
    info['datalad_version'] = datalad.__version__
    # inject extractor state information
    # report on all enabled extractors, even if they did not report
    # anything, so we can act on configuration changes relevant
    # to them
    info['extractors'] = {k: v['state'] for k, v in iteritems(exinfo)}

    if meta.get('dataset', None) is None:  # pragma: no cover
        # this is a double safety net, for any repository that has anything
        # metalad_core should report something. If it doesn't, it should have
        # blown already
        yield dict(
            path=fromds.path,
            type='dataset',
            action='meta_extract',
            status='error',
            message='extraction yielded no dataset-global metadata',
            info=info,
            logger=lgr,
        )
        return

    # create a tempdir for this dataset under .git/tmp
    tmp_basedir = _get_aggtmp_basedir(tods, mkdir=True)
    tmpdir = tempfile.mkdtemp(
        dir=text_type(tmp_basedir),
        prefix=fromds.id + '_')

    # for both types of metadata
    for label, props in iteritems(meta):
        if not meta[label]:
            # we got nothing from extraction
            continue

        tmp_obj_fname = op.join(tmpdir, '{}.xz'.format(label))
        # place JSON dump of the metadata into this dir
        (json_py.dump if label == 'dataset' else json_py.dump2stream)(
            meta[label], tmp_obj_fname, compressed=True)

        # place info on objects into info dict
        info['{}_info'.format(label)] = tmp_obj_fname

    # do not place the files anywhere, just report where they are
    yield dict(
        path=fromds.path,
        type='dataset',
        action='meta_extract',
        status='ok',
        info=info,
        logger=lgr,
    )


def _get_obj_location(ds, srcfile, hash_str):
    """Determine the location of a metadata object in a dataset's object store

    Parameters
    ----------
    ds : Dataset
      The reference dataset whose store shall be used.
    srcfile : Path
      The path of the object's sourcefile (to determine the correct
      file extension.
    hash_str : str
      The hash the object should be stored under.

    Returns
    -------
    Path
      pathlib Path instance to the absolute location for the metadata object
    """
    objpath = \
        ds.pathobj / '.datalad' / 'metadata' / 'objects' / \
        hash_str[:2] / (hash_str[2:] + srcfile.suffix)

    return objpath


def _store_agginfo_db(ds, db):
    # base path in which aggregate.json and objects is located
    # TODO avoid this call
    agginfo_path, agg_base_path = get_ds_aggregate_db_locations(
        ds.pathobj, warn_absent=False)
    # make DB paths on disk always relative
    json_py.dump(
        {
            text_type(Path(p).relative_to(ds.pathobj)):
            {k: text_type(Path(v).relative_to(agg_base_path))
             if k in location_keys else v
             for k, v in props.items()}
            for p, props in db.items()
        },
        text_type(agginfo_path)
    )


def _update_unique_cm(unique_cm, cnmeta, exinfo):
    """Sift through a new content metadata set and update the unique value
    record

    Parameters
    ----------
    unique_cm : dict
      unique value records for all extractors, modified in place
    cnmeta : dict
      Metadata for a content item to sift through.
    exinfo : dict
      Extractor information dict
    """
    # go through content metadata and inject report of unique keys
    # and values into `dsmeta`
    for msrc_key in cnmeta:
        msrc_info = exinfo[msrc_key]
        ucm = unique_cm.get(msrc_key, {})
        if not msrc_info['unique']:
            # unique value aggregation disabled for this extractor
            # next please
            continue
        for k, v in iteritems(cnmeta[msrc_key]):
            if k in msrc_info.get('state', {}).get('unique_exclude', []):
                # XXX this is untested ATM and waiting for
                # https://github.com/datalad/datalad/issues/3135
                #
                # the extractor thinks this key is worthless for the purpose
                # of discovering whole datasets
                # we keep the key (so we know that some file is providing this
                # key), but ignore any value it came with
                val = None
            else:
                val = _val2hashable(v)
            vset = ucm.get(k, set())
            vset.add(val)
            ucm[k] = vset
        if ucm:
            unique_cm[msrc_key] = ucm


def _finalize_unique_cm(unique_cm, dsmeta):
    """Convert harvested unique values in a serializable, ordered
    representation

    Parameters
    ----------
    unique_cm : dict
      unique value records for all extractors
    dsmeta : dict
      dataset metadata

    Returns
    -------
    dict
    """
    # important: we want to have a stable order regarding
    # the unique values (a list). we cannot guarantee the
    # same order of discovery, hence even when not using a
    # set above we would still need sorting. the callenge
    # is that any value can be an arbitrarily complex nested
    # beast
    # we also want to have each unique value set always come
    # in a top-level list, so we know if some unique value
    # was a list, os opposed to a list of unique values

    def _ensure_serializable(val):
        # XXX special cases are untested, need more convoluted metadata
        if isinstance(val, ReadOnlyDict):
            return {k: _ensure_serializable(v) for k, v in iteritems(val)}
        if isinstance(val, (tuple, list)):
            return [_ensure_serializable(v) for v in val]
        else:
            return val

    out = {
        ename: {
            k: [_ensure_serializable(i)
                for i in sorted(
                    v,
                    key=_unique_value_key)] if v is not None else None
            for k, v in iteritems(ucm)
            # v == None (disable unique, but there was a value at some point)
            # otherwise we only want actual values, and also no
            # single-item-lists of a non-value
            # those contribute no information, but bloat the operation
            # (inflated number of keys, inflated storage, inflated search
            # index, ...)
            # also strip any keys that conflict with a key in the actual
            # dataset metadata -- the dataset clearly has a better idea
            # than a blindly generated unique value list
            if v is None or (v and not (
                v == {''} or k in dsmeta.get(ename, {})
            ))
        }
        for ename, ucm in iteritems(unique_cm)
    }
    # only report on extractors with any report
    return {k: v for k, v in iteritems(out) if v or v is None}


def _unique_value_key(x):
    """Small helper for sorting unique content metadata values"""
    if isinstance(x, ReadOnlyDict):
        # XXX special case untested, needs more convoluted metadata
        #
        # turn into an item tuple with keys sorted and values plain
        # or as a hash if *dicts
        x = [(k,
              hash(x[k])
              if isinstance(x[k], ReadOnlyDict) else x[k])
             for k in sorted(x)]
    # we need to force str, because sorted in PY3 refuses to compare
    # any heterogeneous type combinations, such as str/int,
    # tuple(int)/tuple(str)
    return as_unicode(x)
