# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test metadata """

from six import text_type
import os

from datalad.distribution.dataset import Dataset
from datalad.support.gitrepo import GitRepo
from .. import (
    get_metadata_type,
    get_refcommit,
)
from datalad.tests.utils import (
    with_tempfile,
    eq_,
    create_tree,
    assert_repo_status,
    known_failure
)


@with_tempfile(mkdir=True)
def test_get_metadata_type(path):
    ds = Dataset(path).create()
    # nothing set, nothing found
    eq_(get_metadata_type(ds), [])
    # minimal setting
    ds.config.set(
        'datalad.metadata.nativetype', 'mamboschwambo',
        where='dataset')
    eq_(get_metadata_type(ds), 'mamboschwambo')


# FIXME remove when support for the old config var is removed
@with_tempfile(mkdir=True)
def test_get_metadata_type_oldcfg(path):
    ds = Dataset(path).create()
    # minimal setting
    ds.config.set(
        'metadata.nativetype', 'mamboschwambo',
        where='dataset')
    eq_(get_metadata_type(ds), 'mamboschwambo')
