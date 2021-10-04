"""DataLad MetaLad extension"""

__docformat__ = 'restructuredtext'

import os
from .version import __version__
from collections.abc import Mapping
from six import iteritems
import hashlib
from datalad.utils import (
    Path,
    PurePosixPath,
)
from datalad.consts import PRE_INIT_COMMIT_SHA
from datalad.support.digests import Digester


# defines a datalad command suite
# this symbol must be identified as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "DataLad semantic metadata command suite",
    [
        (
            'datalad_metalad.dump',
            'Dump',
            'meta-dump',
            'meta_dump'
        ),
        (
            'datalad_metalad.extract',
            'Extract',
            'meta-extract',
            'meta_extract'
        ),
        (
            'datalad_metalad.aggregate',
            'Aggregate',
            'meta-aggregate',
            'meta_aggregate'
        ),
        (
            'datalad_metalad.add',
            'Add',
            'meta-add',
            'meta_add'
        ),
        (
            'datalad_metalad.conduct',
            'Conduct',
            'meta-conduct',
            'meta_conduct'
        ),
    ]
)

aggregate_layout_version = 1

# relative paths which to exclude from any metadata processing
# including anything underneath them
# POSIX conventions (if needed)
exclude_from_metadata = ('.datalad', '.git', '.gitmodules', '.gitattributes')

# TODO filepath_info is obsolete
location_keys = ('dataset_info', 'content_info', 'filepath_info')


# this is the default context, but any node document can define
# something more suitable
default_context = {
    # schema.org definitions by default
    "@vocab": "http://schema.org/",
    # DataLad ID prefix, pointing to our own resolver
    "datalad": "http://dx.datalad.org/",
}


def get_metadata_type(ds):
    """Return the metadata type(s)/scheme(s) of a dataset

    Parameters
    ----------
    ds : Dataset
      Dataset instance to be inspected

    Returns
    -------
    list(str)
      Metadata type labels or an empty list if no type setting is found and
      optional auto-detection yielded no results
    """
    cfg_key = 'datalad.metadata.nativetype'
    old_cfg_key = 'metadata.nativetype'
    if cfg_key in ds.config:
        return ds.config[cfg_key]
    # FIXME this next conditional should be removed once datasets at
    # datasets.datalad.org have received the metadata config update
    elif old_cfg_key in ds.config:
        return ds.config[old_cfg_key]
    return []


def get_refcommit(ds):
    """Get most recent commit that changes any metadata-relevant content.

    This function should be executed in a clean dataset, with no uncommitted
    changes (untracked is OK).

    Returns
    -------
    str or None
      None if there is no matching commit, a hexsha otherwise.
    """
    exclude_paths = [
        ds.repo.pathobj / PurePosixPath(e)
        for e in exclude_from_metadata
    ]
    count = 0
    diff_cache = {}
    precommit = False
    while True:
        cur = 'HEAD~{:d}'.format(count)
        try:
            # get the diff between the next pair of previous commits
            diff = {
                p.relative_to(ds.repo.pathobj): props
                for p, props in iteritems(ds.repo.diffstatus(
                    PRE_INIT_COMMIT_SHA
                    if precommit
                    else 'HEAD~{:d}'.format(count + 1),
                    cur,
                    # superfluous, but here to state the obvious
                    untracked='no',
                    # this should be OK, unit test covers the cases
                    # of subdataset addition, modification and removal
                    # refcommit evaluation only makes sense in a clean
                    # dataset, and if that is true, any change in the
                    # submodule record will be visible in the parent
                    # already
                    eval_submodule_state='no',
                    # boost performance, we don't care about file types
                    # here
                    eval_file_type=False,
                    _cache=diff_cache))
                if props.get('state', None) != 'clean' \
                and p not in exclude_paths \
                and not any(e in p.parents for e in exclude_paths)
            }
        except ValueError as e:
            # likely ran out of commits to check
            if precommit:
                # end of things
                return None
            else:
                # one last round, taking in the entire history
                precommit = True
                continue
        if diff:
            return ds.repo.get_hexsha(cur)
        # next pair
        count += 1


class ReadOnlyDict(Mapping):
    # Taken from https://github.com/slezica/python-frozendict
    # License: MIT

    # XXX entire class is untested

    """
    An immutable wrapper around dictionaries that implements the complete
    :py:class:`collections.Mapping` interface. It can be used as a drop-in
    replacement for dictionaries where immutability is desired.
    """
    dict_cls = dict

    def __init__(self, *args, **kwargs):
        self._dict = self.dict_cls(*args, **kwargs)
        self._hash = None

    def __getitem__(self, key):
        return self._dict[key]

    def __contains__(self, key):
        return key in self._dict

    def copy(self, **add_or_replace):
        return self.__class__(self, **add_or_replace)

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self._dict)

    def __hash__(self):
        if self._hash is None:
            h = 0
            for key, value in iteritems(self._dict):
                h ^= hash((key, _val2hashable(value)))
            self._hash = h
        return self._hash


def _val2hashable(val):
    """Small helper to convert incoming mutables to something hashable

    The goal is to be able to put the return value into a set, while
    avoiding conversions that would result in a change of representation
    in a subsequent JSON string.
    """
    # XXX special cases are untested, need more convoluted metadata
    if isinstance(val, dict):
        return ReadOnlyDict(val)
    elif isinstance(val, list):
        return tuple(map(_val2hashable, val))
    else:
        return val


def _hashable2val(val):
    """Undo _val2hashable()
    """
    if isinstance(val, ReadOnlyDict):
        return dict(val)
    elif isinstance(val, tuple):
        return list(map(_hashable2val, val))
    else:
        return val


def collect_jsonld_metadata(dspath, res, nodes_by_context, contexts):
    """Sift through a metadata result and gather JSON-LD documents

    Parameters
    ----------
    dspath : str or Path
      Native absolute path of the dataset that shall be used to determine
      the relative path (name) of a file-result. This would typically be
      the path to the dataset that contains the file.
    res : dict
      Result dictionary as produced by `meta_extract()` or
      `meta_dump()`.
    nodes_by_context : dict
      JSON-LD documented are collected in this dict, using their context
      as keys.
    contexts : dict
      Holds a previously discovered context for any extractor.
    """
    if res['type'] == 'dataset':
        _native_metadata_to_graph_nodes(
            res['metadata'],
            nodes_by_context,
            contexts,
        )
    else:
        fmeta = res['metadata']
        # pull out a datalad ID from -core, if there is any
        fid = fmeta.get('metalad_core', {}).get('@id', None)
        _native_metadata_to_graph_nodes(
            fmeta,
            nodes_by_context,
            contexts,
            defaults={
                '@id': fid,
                # do not have a @type default here, it would
                # duplicate across all extractor records
                # let the core extractor deal with this
                #'@type': "DigitalDocument",
                # maybe we need something more fitting than
                # name
                'name': Path(res['path']).relative_to(
                    dspath).as_posix(),
            },
        )


def _native_metadata_to_graph_nodes(
        md, nodes_by_context, contexts, defaults=None):
    """Turn our native metadata format into a true JSON-LD syntax

    This is not necessarily a lossless conversion, all garbage will be
    stripped.
    """
    for extractor, report in iteritems(md):
        if '@context' in report:
            # this is linked data!
            context = ReadOnlyDict(report['@context']) \
                if isinstance(report['@context'], dict) \
                else report['@context']
            if extractor in contexts \
                    and context != contexts[extractor]:
                raise RuntimeError(
                    '{} metadata reports contains conflicting contexts, '
                    'not supported'.format(extractor))
            else:
                # this is extractor was known, or is now known to talk LD
                contexts[extractor] = context
        else:
            # no context reported, either we already have a context on record
            # or this report is not usable as JSON-LD
            context = contexts.get(extractor, None)
        if context is None:
            # unusable
            continue

        # harvest documents in the report
        # TODO add some kind of "describedBy" to the graph nodes
        hashable_context = _val2hashable(context)
        nodes = nodes_by_context.get(hashable_context, [])
        if '@graph' not in report:
            # not a multi-document graph, remove context and treat as
            # a single-node graph
            report.pop('@context', None)
            if not report:
                # there is no other information, and we have the context
                # covered already
                continue
            if defaults is not None:
                # this will typically happen for content/file reports
                report = dict(
                    defaults,
                    **report)
                if report.get('@id', None) is None:
                    # a document without an identifier, ignore
                    continue
            nodes.extend([report])
        else:
            # we are not applying `defaults` assuming this is a full-blown
            # report and nothing trimmed by datalad internally for
            # space-saving reasons
            nodes.extend(report['@graph'])
        nodes_by_context[hashable_context] = nodes


def format_jsonld_metadata(nbc):
    # build the full graph
    jsonld = []
    for context, graph in iteritems(nbc):
        # document with a different context: add as a sub graph
        jsonld.append({
            '@context': _hashable2val(context),
            '@graph': graph,
        })
    return jsonld[0] if len(jsonld) == 1 else jsonld


def get_file_id(rec):
    """Returns a suitable '@id' of a file metadata from a status result

    Prefer a present annex key, but fall back on the Git shasum that is
    always around. Identify the GITSHA as such, and in a similar manner
    to git-annex's style.

    Any ID string is prefixed with 'datalad:' to identify it as a
    DataLad-recognized ID. This prefix is defined in the main JSON-LD
    context defintion.
    """
    id_ = rec['key'] if 'key' in rec else 'SHA1-s{}--{}'.format(
        rec['bytesize'] if 'bytesize' in rec
        else 0 if rec['type'] == 'symlink'
        else os.stat(rec['path']).st_size,
        rec['gitshasum'] if 'gitshasum' in rec
        else Digester(digests=['sha1'])(rec['path'])['sha1'])
    return 'datalad:{}'.format(id_)


def get_agent_id(name, email):
    """Return a suitable '@id' for committers/authors

    In most cases we will not have a URL for people/software agents.
    Let's create a string ID that is based on the combination of both
    name and email. Return an MD5 hash instead of a plain-text string
    to discourage direct interpretation by humans.
    """
    return hashlib.md5(u'{}<{}>'.format(
        name.replace(' ', '_'),
        email
    ).encode('utf-8')).hexdigest()


from datalad import setup_package
from datalad import teardown_package


from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
