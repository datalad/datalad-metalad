# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for custom (JSON-LD) metadata contained in a dataset

One or more source files with metadata can be specified via the
'datalad.metadata.custom-dataset-source' configuration variable.
The content of these files must be a JSON object, and a metadata
dictionary is built by updating it with the content of the JSON
objects in the order in which they are given.

By default a single file is read: '.metadata/dataset.json'
"""

from .base import MetadataExtractor

import os.path as op
from six import text_type
import logging
lgr = logging.getLogger('datalad.metadata.extractors.custom')

from datalad.log import log_progress
from datalad.support.json_py import load as jsonload
from datalad.dochelpers import exc_str
from datalad.utils import (
    assure_list,
    Path,
    PurePosixPath,
)


class CustomMetadataExtractor(MetadataExtractor):
    def get_required_content(self, dataset, process_type, status):
        if process_type in ('all', 'content'):
            mfile_expr = _get_fmeta_expr(dataset)
            for rec in status:
                # build metadata file path
                meta_fpath = _get_fmeta_objpath(dataset, mfile_expr, rec)
                # use op.lexists to also match broken symlinks
                if meta_fpath is not None and op.lexists(meta_fpath):
                    yield dict(path=meta_fpath)

        if process_type in ('all', 'dataset'):
            srcfiles, _ = _get_dsmeta_srcfiles(dataset)
            for f in srcfiles:
                f = text_type(dataset.pathobj / f)
                if op.lexists(f):
                    yield dict(path=f)

    def __call__(self, dataset, refcommit, process_type, status):
        # shortcut
        ds = dataset

        log_progress(
            lgr.info,
            'extractorcustom',
            'Start custom metadata extraction from %s', ds,
            total=len(status) + 1,
            label='Custom metadata extraction',
            unit=' Files',
        )
        if process_type in ('all', 'content'):
            mfile_expr = _get_fmeta_expr(ds)
            for rec in status:
                log_progress(
                    lgr.info,
                    'extractorcustom',
                    'Extracted custom metadata from %s', rec['path'],
                    update=1,
                    increment=True)
                # build metadata file path
                meta_fpath = _get_fmeta_objpath(ds, mfile_expr, rec)
                if meta_fpath is not None and op.exists(meta_fpath):
                    try:
                        meta = jsonload(text_type(meta_fpath))
                        if meta:
                            yield dict(
                                path=rec['path'],
                                metadata=meta,
                                type=rec['type'],
                                status='ok',
                            )
                    except Exception as e:
                        yield dict(
                            path=rec['path'],
                            type=rec['type'],
                            status='error',
                            message=exc_str(e),
                        )

        if process_type in ('all', 'dataset'):
            for r in _yield_dsmeta(ds):
                yield r
            log_progress(
                lgr.info,
                'extractorcustom',
                'Extracted custom metadata from %s', ds.path,
                update=1,
                increment=True)

        log_progress(
            lgr.info,
            'extractorcustom',
            'Finished custom metadata extraction from %s', ds.path
        )

    def get_state(self, dataset):
        ds = dataset
        return {
            'dataset-source': ds.config.get(
                'datalad.metadata.custom-dataset-source',
                '.metadata/dataset.json'),
            'content-source': _get_fmeta_expr(ds),
        }


def _get_dsmeta_srcfiles(ds):
    # which files to look at
    cfg_srcfiles = ds.config.obtain(
        'datalad.metadata.custom-dataset-source',
        [])
    cfg_srcfiles = assure_list(cfg_srcfiles)
    # OK to be always POSIX
    srcfiles = ['.metadata/dataset.json'] \
        if not cfg_srcfiles and op.lexists(
            text_type(ds.pathobj / '.metadata' / 'dataset.json')) \
        else cfg_srcfiles
    return srcfiles, cfg_srcfiles


def _get_fmeta_expr(ds):
    return ds.config.obtain(
        'datalad.metadata.custom-content-source',
        '.metadata/content/{freldir}/{fname}.json')


def _get_fmeta_objpath(ds, expr, rec):
    fpath = Path(rec['path'])
    if rec.get('type', None) != 'file':  # pragma: no cover
        # nothing else in here
        return
    # build associated metadata file path from POSIX
    # pieces and convert to platform conventions at the end
    return text_type(
        ds.pathobj / PurePosixPath(expr.format(
            freldir=fpath.relative_to(
                ds.pathobj).parent.as_posix(),
            fname=fpath.name)))


def _yield_dsmeta(ds):
    srcfiles, cfg_srcfiles = _get_dsmeta_srcfiles(ds)
    dsmeta = {}
    for srcfile in srcfiles:
        abssrcfile = ds.pathobj / PurePosixPath(srcfile)
        # TODO get annexed files, or do in a central place?
        if not abssrcfile.exists():
            # nothing to load
            # warn if this was configured
            if srcfile in cfg_srcfiles:
                yield dict(
                    path=ds.path,
                    type='dataset',
                    status='impossible',
                    message=(
                        'configured custom metadata source is not '
                        'available in %s: %s',
                        ds, srcfile),
                )
                # no further operation on half-broken metadata
                return
        lgr.debug('Load custom metadata from %s', abssrcfile)
        meta = jsonload(text_type(abssrcfile))
        dsmeta.update(meta)
    if dsmeta:
        yield dict(
            path=ds.path,
            metadata=dsmeta,
            type='dataset',
            status='ok',
        )
