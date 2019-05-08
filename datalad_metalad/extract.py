# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Run one or more metadata extractors on a dataset or file(s)"""

__docformat__ = 'restructuredtext'

from os import curdir
import os.path as op
import logging
from six import (
    iteritems,
    text_type,
)
from datalad import cfg
from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.interface.results import (
    get_status_dict,
    success_status_map,
)
from datalad.interface.utils import eval_results
from datalad.distribution.dataset import (
    datasetmethod,
    EnsureDataset,
    require_dataset,
)
from .extractors.base import MetadataExtractor

from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
    EnsureChoice,
    EnsureBool,
)
from . import (
    get_refcommit,
    exclude_from_metadata,
    get_metadata_type,
    collect_jsonld_metadata,
    format_jsonld_metadata,
)
from datalad.utils import (
    assure_list,
    Path,
    PurePosixPath,
)
from datalad.dochelpers import exc_str
from datalad.log import log_progress
from datalad.ui import ui
import datalad.support.ansi_colors as ac

# API commands needed
from datalad.core.local import status as _status

lgr = logging.getLogger('datalad.metadata.extract')


@build_doc
class Extract(Interface):
    """Run one or more metadata extractors on a dataset or file.

    This command does not modify a dataset, but may initiate required data
    transfers to perform metadata extraction that requires local file content
    availability. This command does not support recursion into subdataset.

    The result(s) are structured like the metadata DataLad would extract
    during metadata aggregation (in fact, this command is employed during
    aggregation). There is one result per dataset/file.

    Examples:

      Extract metadata with two extractors from a dataset in the current
      directory and also from all its files::

        $ datalad extract-metadata -d . --source xmp --source metalad_core

      Extract XMP metadata from a single PDF that is not part of any dataset::

        $ datalad extract-metadata --source xmp Downloads/freshfromtheweb.pdf


    Customization of extraction:

    The following configuration settings can be used to customize extractor
    behavior

    ``datalad.metadata.extract-from-<extractorname> = {all|dataset|content}``
       which type of information an enabled extractor will be operating on
       (see --process-type argument for details)

    ``datalad.metadata.exclude-path = <path>``
      ignore all content underneath the given path for metadata extraction,
      must be relative to the root of the dataset and in POSIX convention,
      and can be given multiple times
    """
    result_renderer = 'tailored'

    _params_ = dict(
        sources=Parameter(
            args=("--source",),
            dest="sources",
            metavar=("NAME"),
            action='append',
            doc="""Name of a metadata extractor to be executed.
            If none is given, a set of default configured extractors,
            plus any extractors enabled in a dataset's configuration
            and invoked.
            [CMD: This option can be given more than once CMD][PY: Multiple
            extractors can be given as a list PY]."""),
        process_type=Parameter(
            args=("--process-type",),
            doc="""type of information to process. If 'all',
            metadata will be extracted for the entire dataset and its content.
            If not specified, the dataset's configuration will determine
            the selection, and will default to 'all'. Note that not processing
            content can influence the dataset metadata composition (e.g. report
            of total size). There is an auxiliary category 'extractors' that
            will cause all enabled extractors to be loaded, and reports
            on their status and configuration.""",
            constraints=EnsureChoice(
                None, 'all', 'dataset', 'content', 'extractors')),
        path=Parameter(
            args=("path",),
            metavar="FILE",
            nargs="*",
            doc="Path of a file to extract metadata from.",
            constraints=EnsureStr() | EnsureNone()),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc=""""Dataset to extract metadata from. If no further
            constraining path is given, metadata is extracted from all files
            of the dataset.""",
            constraints=EnsureDataset() | EnsureNone()),
        format=Parameter(
            args=('--format',),
            doc="""format to use for the 'metadata' result property. 'native'
            will report the output of extractors as separate metadata
            properties that are stored under the name of the associated
            extractor; 'jsonld' composes a JSON-LD graph document, while
            stripping any information that does not appear to be properly
            typed linked data (extractor reports no '@context' field).""",
            constraints=EnsureChoice(
                'native', 'jsonld')),
    )

    @staticmethod
    @datasetmethod(name='meta_extract')
    @eval_results
    def __call__(dataset=None, path=None, sources=None, process_type=None,
                 format='native'):
        ds = require_dataset(
            dataset or curdir,
            purpose="extract metadata",
            check_installed=not path)

        # check what extractors we want as sources, and whether they are
        # available
        if not sources:
            sources = ['metalad_core', 'metalad_annex'] \
                + assure_list(get_metadata_type(ds))
        # keep local, who knows what some extractors might pull in
        from pkg_resources import iter_entry_points  # delayed heavy import
        extractors = {}
        for ep in iter_entry_points('datalad.metadata.extractors'):
            if ep.name not in sources:
                # not needed here
                continue
            rec = dict(entrypoint=ep)
            if ep.name in extractors:  # pragma: no cover
                # potential conflict
                if extractors[ep.name]['entrypoint'].dist.project_name == 'datalad':
                    # this is OK, just state it is happening
                    lgr.debug(
                        'Extractor %s overrides datalad-core variant', ep)
                    extractors[ep.name] = rec
                elif ep.dist.project_name == 'datalad':
                    # also OK
                    lgr.debug(
                        'Prefer extractor %s over datalad-core variant', ep)
                else:
                    msg = (
                        'At least two DataLad extensions provide metadata '
                        'extractor %s: %s vs. %s',
                        ep.name,
                        ep.dist,
                        extractors[ep.name].dist)
                    if ep.name in sources:
                        # this extractor is required -> blow hard
                        raise RuntimeError(msg[0] % msg[1:])
                    else:
                        # still moan
                        lgr.warn(msg)
                    # ignore the newcomer, is listed second in sys.path
            else:
                # this fresh and unique
                extractors[ep.name] = rec
        for msrc in sources:
            if msrc not in extractors:
                # we said that we want to fail, rather then just moan about
                # less metadata
                raise ValueError(
                    "Enabled metadata extractor '{}' not available".format(msrc),
                )
            # load extractor implementation
            rec = extractors[msrc]
            rec['process_type'] = process_type \
                if process_type and not process_type == 'extractors' \
                else ds.config.obtain(
                    'datalad.metadata.extract-from-{}'.format(
                        msrc.replace('_', '-')),
                    default='all')
            # load the extractor class, no instantiation yet
            try:
                rec['class'] = rec['entrypoint'].load()
            except Exception as e:  # pragma: no cover
                msg = ('Failed %s metadata extraction from %s: %s',
                       msrc, ds, exc_str(e))
                log_progress(lgr.error, 'metadataextractors', *msg)
                raise ValueError(msg[0] % msg[1:])

        res_props = dict(
            action='meta_extract',
            logger=lgr,
        )

        # build report on extractors and their state info
        if process_type == 'extractors':
            for ename, eprops in iteritems(extractors):
                state = {}
                # do not trip over old extractors
                if hasattr(eprops['class'], 'get_state'):
                    state.update(eprops['class']().get_state(ds))

                yield dict(
                    action='meta_extract',
                    path=ds.path,
                    status='ok',
                    logger=lgr,
                    extractor=ename,
                    state=dict(
                        state,
                        process_type=eprops['process_type'],
                    )
                )
            return


        # build a representation of the dataset's content (incl subds
        # records)
        # go through a high-level command (not just the repo methods) to
        # get all the checks and sanitization of input arguments
        # this call is relatively expensive, but already anticipates
        # demand for information by our core extractors that always run
        # unconditionally, hence no real slowdown here
        # TODO this could be a dict, but MIH cannot think of an access
        # pattern that does not involve iteration over all items
        status = []
        exclude_paths = [
            ds.pathobj / PurePosixPath(e)
            for e in (
                list(exclude_from_metadata) + assure_list(
                    ds.config.get('datalad.metadata.exclude-path', []))
            )
        ]
        if ds.is_installed():
            # we can make use of status
            res_props.update(refds=ds.path)

            for r in ds.status(
                    # let status sort out all path arg handling
                    # but this will likely make it impossible to use this
                    # command to just process an individual file independent
                    # of a dataset
                    path=path,
                    # it is safe to ask for annex info even when a dataset is
                    # plain Git
                    # NOTE changing to 'annex=availability' has substantial
                    # performance costs, as it involved resolving each annex
                    # symlink on the file-system, which can be really slow
                    # depending on the FS and the number of annexed files
                    annex='basic',
                    # TODO we never want to aggregate metadata from untracked
                    # content, but we might just want to see what we can get
                    # from a file
                    untracked='no',
                    # this command cannot and will not work recursively
                    recursive=False,
                    result_renderer='disabled'):
                # path reports are always absolute and anchored on the dataset
                # (no repo) path
                p = Path(r['path'])
                if p in exclude_paths or \
                        any(e in p.parents for e in exclude_paths):
                    # this needs to be ignore for any further processing
                    continue
                # strip useless context information
                status.append(
                    {k: v for k, v in iteritems(r)
                     if (k not in ('refds', 'parentds', 'action', 'status')
                         and not k.startswith('prev_'))})

            # determine the commit that we are describing
            refcommit = get_refcommit(ds)
            if refcommit is None or not len(status):
                # this seems extreme, but without a single commit there is
                # nothing we can have, or describe -> blow
                yield dict(
                    res_props,
                    status='error',
                    message=\
                    'No metadata-relevant repository content found. ' \
                    'Cannot determine reference commit for metadata ID',
                    type='dataset',
                    path=ds.path,
                )
                return
            # stamp every result
            res_props['refcommit'] = refcommit
        else:
            # no dataset at hand, take path arg at face value and hope
            # for the best
            # TODO we have to resolve the given path to make it match what
            # status is giving (abspath with ds (not repo) anchor)
            status = [dict(path=p, type='file') for p in assure_list(path)]
            # just for compatibility, mandatory argument list below
            refcommit = None

        if ds.is_installed():
            # check availability requirements and obtain data as needed
            needed_paths = set()
            for rec in extractors.values():
                if hasattr(rec['class'], 'get_required_content'):
                    needed_paths.update(
                        # new extractors do not need any instantiation args
                        s['path'] for s in rec['class']().get_required_content(
                            ds,
                            rec['process_type'],
                            status
                        )
                    )
            if needed_paths:
                for r in ds.get(
                        path=needed_paths,
                        return_type='generator',
                        result_renderer='disabled'):
                    if success_status_map.get(
                            r['status'],
                            False) != 'success':  # pragma: no cover
                        # online complain when something goes wrong
                        yield r

        contexts = {}
        nodes_by_context = {}
        try:
            for res in _proc(
                    ds,
                    refcommit,
                    sources,
                    status,
                    extractors,
                    process_type):
                if format == 'native':
                    # that is what we pass around internally
                    res.update(**res_props)
                    yield res
                elif format == 'jsonld':
                    collect_jsonld_metadata(
                        ds.pathobj, res, nodes_by_context, contexts)
        finally:
            # extractors can come from any source with no guarantee for
            # proper implementation. Let's make sure that we bring the
            # dataset back into a sane state (e.g. no batch processes
            # hanging around). We should do this here, as it is not
            # clear whether extraction results will be saved to the
            # dataset(which would have a similar sanitization effect)
            if ds.repo:
                ds.repo.precommit()
        if format == 'jsonld':
            yield dict(
                status='ok',
                type='dataset',
                path=ds.path,
                metadata=format_jsonld_metadata(nodes_by_context),
                **res_props)

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        if res['status'] != 'ok' or \
                not res.get('action', None) == 'meta_extract':
            # logging complained about this already
            return
        if 'state' in res and 'extractor' in res:
            # extractor report, special treatment
            ui.message('{name}({state})'.format(
                name=ac.color_word(res['extractor'], ac.BOLD),
                state=','.join('{}{}{}{}'.format(
                    # boolean states get a + or - prefix
                    '+' if v is True else '-' if v is False else '',
                    k,
                    '=' if not isinstance(v, bool) else '',
                    v if not isinstance(v, bool) else '')
                    for k, v in iteritems(res['state'])
                    # this is an extractor property, and mostly serves
                    # internal purposes
                    if k not in ('unique_exclude',)),
            ))
            return
        # list the path, available metadata keys, and tags
        path = op.relpath(
            res['path'],
            res['refds']) if res.get('refds', None) else res['path']
        meta = res.get('metadata', {})
        ui.message('{path}{type}:{spacer}{meta}{tags}'.format(
            path=ac.color_word(path, ac.BOLD),
            type=' ({})'.format(
                ac.color_word(res['type'], ac.MAGENTA))
            if 'type' in res else '',
            spacer=' ' if len([m for m in meta if m != 'tag']) else '',
            meta=','.join(k for k in sorted(meta.keys())
                          if k not in ('tag', '@context', '@id'))
                 if meta else ' -' if 'metadata' in res else
                 ' {}'.format(
                     ','.join(e for e in res['extractors']
                              if e not in ('datalad_core', 'metalad_core', 'metalad_annex'))
                 ) if 'extractors' in res else '',
            tags='' if 'tag' not in meta else ' [{}]'.format(
                 ','.join(assure_list(meta['tag'])))))


def _proc(ds, refcommit, sources, status, extractors, process_type):
    dsmeta = dict()
    contentmeta = {}

    log_progress(
        lgr.info,
        'metadataextractors',
        'Start metadata extraction from %s', ds,
        total=len(sources),
        label='Metadata extraction',
        unit=' extractors',
    )
    for msrc in sources:
        msrc_key = msrc
        extractor = extractors[msrc]
        log_progress(
            lgr.info,
            'metadataextractors',
            'Engage %s metadata extractor', msrc_key,
            update=1,
            increment=True)

        # actually pull the metadata records out of the extractor
        for res in _run_extractor(
                extractor['class'],
                msrc,
                ds,
                refcommit,
                status,
                extractor['process_type']):
            # always have a path, use any absolute path coming in,
            # make any relative path absolute using the dataset anchor,
            # use the dataset path if nothing is coming in (better then
            # no path at all)
            # for now normalize the reported path to be a plain string
            # until DataLad as a whole can deal with pathlib objects
            if 'path' in res:
                res['path'] = text_type(Path(res['path']))
            res.update(
                path=ds.path
                if 'path' not in res else res['path']
                if op.isabs(res['path']) else op.join(ds.path, res['path'])
            )

            # the following two conditionals are untested, as a test would
            # require a metadata extractor to yield broken metadata, and in
            # order to have such one, we need a mechanism to have the test
            # inject one on the fly MIH thinks that the code neeeded to do that
            # is more chances to be broken then the code it would test
            if success_status_map.get(res['status'], False) != 'success':  # pragma: no cover
                yield res
                # no further processing of broken stuff
                continue
            else:  # pragma: no cover
                # if the extractor was happy check the result
                if not _ok_metadata(res, msrc, ds, None):
                    res.update(
                        # this will prevent further processing a few lines down
                        status='error',
                        # TODO have _ok_metadata report the real error
                        message=('Invalid metadata (%s)', msrc),
                    )
                    yield res
                    continue

            # we do not want to report info that there was no metadata
            if not res['metadata']:  # pragma: no cover
                lgr.debug(
                    'Skip %s %s metadata in record of %s: '
                    'extractor reported nothing',
                    msrc_key, res.get('type', ''), res['path'])
                continue

            if res['type'] == 'dataset':
                # TODO warn if two dataset records are generated by the same
                # extractor
                dsmeta[msrc_key] = res['metadata']
            else:
                # this is file metadata, _ok_metadata() checks unknown types
                # assign only ask each metadata extractor once, hence no
                # conflict possible
                loc_dict = contentmeta.get(res['path'], {})
                loc_dict[msrc_key] = res['metadata']
                contentmeta[res['path']] = loc_dict

    log_progress(
        lgr.info,
        'metadataextractors',
        'Finished metadata extraction from %s', ds,
    )
    # top-level code relies on the fact that any dataset metadata
    # is yielded before content metadata
    if process_type in (None, 'all', 'dataset') and \
            dsmeta and ds is not None and ds.is_installed():
        yield get_status_dict(
            ds=ds,
            metadata=dsmeta,
            # any errors will have been reported before
            status='ok',
        )

    for p in contentmeta:
        res = get_status_dict(
            # TODO avoid is_installed() call
            path=op.join(ds.path, p) if ds.is_installed() else p,
            metadata=contentmeta[p],
            type='file',
            # any errors will have been reported before
            status='ok',
        )
        # TODO avoid is_installed() call, check if such info is
        # useful and accurate at all
        if ds.is_installed():
            res['parentds'] = ds.path
        yield res


def _run_extractor(extractor_cls, name, ds, refcommit, status, process_type):
    """Helper to control extractor using the right API

    Central switch to deal with alternative/future APIs is inside
    """
    try:
        # detect supported API and interface as needed
        if issubclass(extractor_cls, MetadataExtractor):
            # new-style, command-like extractors
            extractor = extractor_cls()
            for r in extractor(
                    dataset=ds,
                    refcommit=refcommit,
                    status=status,
                    process_type=process_type):
                yield r
        elif hasattr(extractor_cls, 'get_metadata'):  # pragma: no cover
            # old-style, keep around for a while, but don't sweat over it much
            for res in _yield_res_from_pre2019_extractor(
                    ds,
                    name,
                    extractor_cls,
                    process_type,
                    # old extractors only take a list of relative paths
                    # and cannot benefit from outside knowledge
                    # TODO avoid is_installed() call
                    [text_type(Path(p['path']).relative_to(ds.pathobj))
                     if ds.is_installed()
                     else p['path']
                     for p in status]):
                yield res
        else:  # pragma: no cover
            raise RuntimeError(
                '{} does not have a recognised extractor API'.format(
                    extractor_cls))
    except Exception as e:  # pragma: no cover
        if cfg.get('datalad.runtime.raiseonerror'):
            log_progress(
                lgr.error,
                'metadataextractors',
                'Failed %s metadata extraction from %s', name, ds,
            )
            raise
        yield get_status_dict(
            ds=ds,
            # any errors will have been reported before
            status='error',
            message=('Failed to get %s metadata (%s): %s',
                     ds, name, exc_str(e)),
        )


def _yield_res_from_pre2019_extractor(
        ds, name, extractor_cls, process_type, paths):  # pragma: no cover
    """This implements dealing with our first extractor class concept"""

    want_dataset_meta = process_type in ('all', 'dataset') \
        if process_type else ds.config.obtain(
            'datalad.metadata.extract-dataset-{}'.format(
                name.replace('_', '-')),
            default=True,
            valtype=EnsureBool())
    want_content_meta = process_type in ('all', 'content') \
        if process_type else ds.config.obtain(
            'datalad.metadata.extract-content-{}'.format(
                name.replace('_', '-')),
            default=True,
            valtype=EnsureBool())

    if not (want_dataset_meta or want_content_meta):  # pragma: no cover
        log_progress(
            lgr.info,
            'metadataextractors',
            'Skipping %s metadata extraction from %s, '
            'disabled by configuration',
            name, ds,
        )
        return

    try:
        extractor = extractor_cls(ds, paths)
    except Exception as e:  # pragma: no cover
        log_progress(
            lgr.error,
            'metadataextractors',
            'Failed %s metadata extraction from %s', name, ds,
        )
        raise ValueError(
            "Failed to load metadata extractor for '%s', "
            "broken dataset configuration (%s)?: %s",
            name, ds, exc_str(e))

    # this is the old way of extractor operation
    dsmeta_t, contentmeta_t = extractor.get_metadata(
        dataset=want_dataset_meta,
        content=want_content_meta,
    )
    # fake the new way of reporting results directly
    # extractors had no way to report errors, hence
    # everything is unconditionally 'ok'
    for loc, meta in contentmeta_t or []:
        yield dict(
            status='ok',
            path=loc,
            type='file',
            metadata=meta,
        )
    yield dict(
        status='ok',
        path=ds.path,
        type='dataset',
        metadata=dsmeta_t,
    )


def _ok_metadata(res, msrc, ds, loc):
    restype = res.get('type', None)
    if restype not in ('dataset', 'file'):  # pragma: no cover
        # untested, would need broken extractor
        lgr.error(
            'metadata report for something other than a file or dataset: %s',
            restype
        )
        return False

    meta = res.get('metadata', None)
    if meta is None or isinstance(meta, dict):
        return True
    else:  # pragma: no cover
        # untested, needs broken extract
        # extractor
        msg = (
            "Metadata extractor '%s' yielded something other than a "
            "dictionary for dataset %s%s -- this is likely a bug, "
            "please consider reporting it. "
            "This type of native metadata will be ignored. Got: %s",
            msrc,
            ds,
            '' if loc is None else ' content {}'.format(loc),
            repr(meta))
        if cfg.get('datalad.runtime.raiseonerror'):
            raise RuntimeError(*msg)

        lgr.error(*msg)
        return False
