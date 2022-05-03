"""DataLad MetaLad extension"""
import os
import hashlib

from datalad.support.digests import Digester


__docformat__ = 'restructuredtext'


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
        (
            'datalad_metalad.filter',
            'Filter',
            'meta-filter',
            'meta_filter'
        ),
    ]
)


# relative paths which to exclude from any metadata processing
# including anything underneath them
# POSIX conventions (if needed)
exclude_from_metadata = ('.datalad', '.git', '.gitmodules', '.gitattributes')


# this is the default context, but any node document can define
# something more suitable
default_context = {
    # schema.org definitions by default
    "@vocab": "http://schema.org/",
    # DataLad ID prefix, pointing to our own resolver
    "datalad": "http://dx.datalad.org/",
}


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
