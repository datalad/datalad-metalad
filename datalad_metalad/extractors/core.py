# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for Datalad's own core storage"""

# TODO dataset metadata
# - known annex UUIDs
# - avoid anything that is specific to a local clone
#   (repo mode, etc.) limit to description of dataset(-network)

from .base import MetadataExtractor
from .. import (
    default_context,
    get_file_id,
    get_agent_id,
)
from datalad.utils import (
    Path,
)
from six import (
    iteritems,
    string_types,
)

import logging
lgr = logging.getLogger('datalad.metadata.extractors.metalad_core')
from datalad.log import log_progress
import datalad.distribution.subdatasets
from datalad.support.constraints import EnsureBool
import datalad.support.network as dsn
from datalad.dochelpers import exc_str

import os.path as op


class DataladCoreExtractor(MetadataExtractor):
    # reporting unique file sizes has no relevant use case that I can think of
    # identifiers are included explicitly
    _unique_exclude = {'@id', 'contentbytesize', }

    def __call__(self, dataset, refcommit, process_type, status):
        # shortcut
        ds = dataset

        log_progress(
            lgr.info,
            'extractordataladcore',
            'Start core metadata extraction from %s', ds,
            total=len(status) + 1,
            label='Core metadata extraction',
            unit=' Files',
        )
        total_content_bytesize = 0
        if process_type in ('all', 'content'):
            for res in self._get_contentmeta(ds, status):
                total_content_bytesize += res['metadata'].get(
                    'contentbytesize', 0)
                log_progress(
                    lgr.info,
                    'extractordataladcore',
                    'Extracted core metadata from %s', res['path'],
                    update=1,
                    increment=True)
                yield dict(
                    res,
                    type='file',
                    status='ok',
                )
        if process_type in ('all', 'dataset'):
            log_progress(
                lgr.info,
                'extractordataladcore',
                'Extracted core metadata from %s', ds.path,
                update=1,
                increment=True)
            dsmeta = [
                r for r in self._yield_dsmeta(
                    ds, status, refcommit, process_type,
                    total_content_bytesize)
            ]
            yield dict(
                metadata={
                    '@context': default_context,
                    '@graph': dsmeta,
                },
                type='dataset',
                status='ok',
            )
        log_progress(
            lgr.info,
            'extractordataladcore',
            'Finished core metadata extraction from %s', ds
        )

    def _yield_dsmeta(self, ds, status, refcommit, process_type,
                      total_content_bytesize):
        commitinfo = _get_commit_info(ds, refcommit, status)
        contributor_ids = []
        for contributor in commitinfo.pop('contributors', []):
            contributor_id = get_agent_id(*contributor[:2])
            yield {
                '@id': contributor_id,
                # we cannot distinguish real people from machine-committers
                '@type': 'agent',
                'name': contributor[0],
                'email': contributor[1],
            }
            contributor_ids.append(contributor_id)
        meta = {
            # the uniquest ID for this metadata record is the refcommit SHA
            '@id': refcommit,
            # the dataset UUID is the main identifier
            'identifier': ds.id,
            '@type': 'Dataset',
        }
        meta.update(commitinfo)
        if contributor_ids:
            c = [{'@id': i} for i in contributor_ids]
            meta['hasContributor'] = c[0] if len(c) == 1 else c
        parts = [{
            # schema.org doesn't have anything good for a symlink, as it could
            # be anything
            '@type': 'Thing'
            if part['type'] == 'symlink'
            else 'DigitalDocument',
            # relative path within dataset, always POSIX
            # TODO find a more specific term for "local path relative to root"
            'name': Path(part['path']).relative_to(ds.pathobj).as_posix(),
            '@id': get_file_id(part),
        }
            for part in status
            if part['type'] != 'dataset'
        ]
        for subds in [s for s in status if s['type'] == 'dataset']:
            subdsinfo = {
                # reference by subdataset commit
                '@id': subds['gitshasum'],
                '@type': 'Dataset',
                'name': Path(subds['path']).relative_to(ds.pathobj).as_posix(),
            }
            subdsid = ds.subdatasets(
                contains=subds['path'],
                return_type='item-or-list').get('gitmodule_datalad-id', None)
            if subdsid:
                subdsinfo['identifier'] = subdsid
            parts.append(subdsinfo)
        if parts:
            meta['hasPart'] = parts
        if ds.config.obtain(
                'datalad.metadata.datalad-core.report-remotes',
                True, valtype=EnsureBool()):
            remote_names = ds.repo.get_remotes()
            distributions = []
            known_uuids = {}
            # start with configured Git remotes
            for r in remote_names:
                info = {
                    'name': r,
                    # not very informative
                    #'description': 'DataLad dataset sibling',
                }
                url = ds.config.get('remote.{}.url'.format(r), None)
                # best effort to recode whatever is configured into a URL
                if url is not None:
                    url = ri2url(dsn.RI(url))
                if url:
                    info['url'] = url
                # do we have information on the annex ID?
                annex_uuid = ds.config.get(
                    'remote.{}.annex-uuid'.format(r), None)
                if annex_uuid is not None:
                    info['@id'] = annex_uuid
                    known_uuids[annex_uuid] = info
                if 'url' in info or '@id' in info:
                    # only record if we have any identifying information
                    # otherwise it is pointless cruft
                    distributions.append(info)
            # now look for annex info
            if hasattr(ds.repo, 'repo_info'):
                info = ds.repo.repo_info(fast=True)
                for cat in ('trusted repositories',
                            'semitrusted repositories',
                            'untrusted repositories'):
                    for r in info[cat]:
                        if r['here'] or r['uuid'] in (
                                '00000000-0000-0000-0000-000000000001',
                                '00000000-0000-0000-0000-000000000002'):
                            # ignore local and universally available
                            # remotes
                            continue
                        # avoid duplicates, but record all sources, even
                        # if not URLs are around
                        if r['uuid'] not in known_uuids:
                            distributions.append({'@id': r['uuid']})
            if len(distributions):
                meta['distribution'] = sorted(
                    distributions,
                    key=lambda x: x.get('@id', x.get('url', None))
                )
        if total_content_bytesize:
            meta['contentbytesize'] = total_content_bytesize
        yield meta

    def _get_contentmeta(self, ds, status):
        """Get ALL metadata for all dataset content.

        Returns
        -------
        generator((location, metadata_dict))
        """
        # cache whereis info of tarball/zip/archives, tend to be used
        # more than once, can save a chunk of runtime
        arxiv_whereis = {}
        # start batched 'annex whereis' and query for availability info
        # there is no need to make sure a batched command is terminated
        # properly, the harness in meta_extract will do this
        wic = whereis_file if hasattr(ds.repo, 'repo_info') \
            else lambda x, y: dict(status='error')
        for rec in status:
            recorded_archive_keys = set()
            if rec['type'] == 'dataset':
                # subdatasets have been dealt with in the dataset metadata
                continue
            md = self._describe_file(rec)
            wi = wic(ds.repo, rec['path'])
            if wi['status'] != 'ok':
                yield dict(
                    path=rec['path'],
                    metadata=md,
                )
                continue
            urls = _get_urls_from_whereis(wi)
            # urls we the actual file content can be obtained
            # directly
            dist = sorted([url for url in urls if url.startswith('http')])
            if dist:
                md['distribution'] = dict(url=dist)

            ispart = []
            for arxiv_url in [url for url in urls
                              if url.startswith('dl+archive:') and \
                              '#' in url]:
                key = _get_archive_key(arxiv_url)
                if not key or key in recorded_archive_keys:
                    # nothing we can work with, or all done
                    continue
                arxiv_urls = arxiv_whereis.get(key, None)
                if arxiv_urls is None:
                    try:
                        arxiv_urls = _get_urls_from_whereis(
                            ds.repo.whereis(key, key=True, output='full'))
                    except Exception as e:
                        lgr.debug(
                            'whereis query failed for key %s: %s',
                            key, exc_str(e))
                        arxiv_urls = []
                    arxiv_whereis[key] = arxiv_urls
                if arxiv_urls:
                    ispart.append({
                        '@id': key,
                        'distribution': {
                            'url': sorted(arxiv_urls),
                        },
                    })
                    recorded_archive_keys.add(key)
            if ispart:
                md['isPartOf'] = sorted(
                    ispart,
                    key=lambda x: x['@id']
                )
            yield dict(
                path=rec['path'],
                metadata=md,
            )

    def _describe_file(self, rec):
        info = {
            '@id': get_file_id(rec),
            # schema.org doesn't have a useful term, only contentSize
            # and fileSize which seem to be geared towards human consumption
            # not numerical accuracy
            # TODO define the term
            'contentbytesize': rec.get('bytesize', 0)
            if 'bytesize' in rec or rec['type'] == 'symlink'
            else op.getsize(rec['path']),
            # TODO the following list are optional enhancement that should come
            # with individual ON/OFF switches
            # TODO run `git log` to find earliest and latest commit to determine
            # 'dateModified' and 'dateCreated'
            # TODO determine per file 'contributor' from git log
        }
        return info

    def get_state(self, dataset):
        ds = dataset
        return {
            # increment when output format changes
            'version': 1,
            'unique_exclude': list(self._unique_exclude),
            'remotes': ds.config.obtain(
                'datalad.metadata.datalad-core.report-remotes',
                True, valtype=EnsureBool()),
            'contributors': ds.config.obtain(
                'datalad.metadata.datalad-core.report-contributors',
                True, valtype=EnsureBool()),
            'modification-dates': ds.config.obtain(
                'datalad.metadata.datalad-core.report-modification-dates',
                True, valtype=EnsureBool()),
        }


def _get_urls_from_whereis(wi, prefixes=('http', 'dl+archive:')):
    """Extract a list of URLs starting with any of the given prefixes
    from "whereis" output"""
    return [
        url
        for remote, rprops in iteritems(wi.get('remotes', {}) if 'status' in wi else wi)
        for url in rprops.get('urls', [])
        if any(url.startswith(pref) for pref in prefixes)
    ]


def _get_archive_key(whereis):
    """trying to decode the various flavors of whereis info for archives"""
    if whereis.startswith(u'dl+archive:'):
        whereis = whereis[11:]
        if u'tar#path' in whereis or 'zip#path' in whereis:
            return whereis.split('#')[0]
        elif u'.zip/' in whereis:
            # key will not have a slash
            return whereis.split('/')[0]


def _get_commit_info(ds, refcommit, status):
    """Get info about all commits, up to (and incl. the refcommit)"""
    #- get all the commit info with git log --pretty='%aN%x00%aI%x00%H'
    #  - use all first-level paths other than .datalad and .git for the query
    #- from this we can determine all modification timestamps, described refcommit
    #- do a subsequent git log query for the determined refcommit to determine
    #  a version by counting all commits since inception up to the refcommit
    #  - we cannot use the first query, because it will be constrained by the
    #    present paths that may not have existed previously at all

    # grab the history until the refcommit
    stdout, stderr = ds.repo._git_custom_command(
        None,
        # name, email, timestamp, shasum
        ['git', 'log', '--pretty=format:%aN%x00%aE%x00%aI%x00%H', refcommit]
    )
    commits = [line.split('\0') for line in stdout.splitlines()]
    # version, always anchored on the first commit (tags could move and
    # make the integer commit count ambigous, and subtantially complicate
    # version comparisons
    version = '0-{}-g{}'.format(
        len(commits),
        # abbreviated shasum (like git-describe)
        ds.repo.get_hexsha(commits[0][3], short=True),
    )
    meta = {
        'version': version,
    }
    if ds.config.obtain(
            'datalad.metadata.datalad-core.report-contributors',
            True, valtype=EnsureBool()):
        meta.update(
            contributors=sorted(set(tuple(c[:2]) for c in commits)))
    if ds.config.obtain(
            'datalad.metadata.datalad-core.report-modification-dates',
            True, valtype=EnsureBool()):
        meta.update(
            dateCreated=commits[-1][2],
            dateModified=commits[0][2],
        )
    return meta


# TODO RF to be merged with datalad.support.network
def ri2url(ri):
    f = ri.fields
    if isinstance(ri, dsn.URL):
        return ri.as_str()
    elif isinstance(ri, dsn.SSHRI):
        # construct a URL that Git would understand
        return 'ssh://{}{}{}{}{}{}'.format(
            f['username'],
            '@' if f['username'] else '',
            f['hostname'],
            ':' if f['port'] else '',
            f['port'],
            f['path'] if op.isabs(f['path'])
            else '/{}'.format(f['path']) if f['path'].startswith('~')
            else '/~/{}'.format(f['path'])
        )
    elif isinstance(ri, dsn.PathRI):
        # this has no chance of being resolved outside this machine
        # not work reporting
        return None


# The following function pair should be part of AnnexRepo, but a PR was
# rejected, because there is already an old whereis() -- but with an
# overcomplicated API and no batch-mode support -- going solo...
def whereis_file(self, path):
    """Same as `whereis_file_()`, but for a single path and return-dict"""
    #return list(self.whereis_file_([path]))[0]
    return list(whereis_file_(self, [path]))[0]


def whereis_file_(self, paths):
    """
    Parameters
    ----------
    paths : iterable
        Paths of files to query for, either absolute paths matching the
        repository root (self.path), or paths relative to the root of the
        repository

    Yields
    ------
    dict
        A response dictionary to each query path with the following keys:
        'path' with the queried path in the same form t was provided;
        'status' {ok|error} indicating whether git annex was queried
        successfully for a path; 'key' with the annex key for the file;
        'remotes' with a dictionary of remotes that have a copy of the
        respective file (annex UUIDs are keys, and values are dictionaries
        with keys: 'description', 'here', 'urls' (list) that contain
        the values of the respective 'git annex whereis' response.
    """
    if isinstance(paths, string_types):
        raise ValueError('whereis_file(paths): paths must be '
                         'iterable, not a string type')

    cmd = self._batched.get('whereis', json=True, path=self.path)
    for path in paths:
        r = cmd(path)
        # give path back in the same shape as it came in
        res = dict(path=path)
        if not r:
            yield dict(res, status='error')
            continue
        yield dict(
            res,
            status='ok' if r.get('success', False) else 'error',
            key=r['key'],
            remotes={
                remote['uuid']:
                {x: remote.get(x, None)
                 for x in ('description', 'here', 'urls')}
                for remote in r['whereis']},
        )
