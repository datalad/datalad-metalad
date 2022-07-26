# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test filters"""
from unittest.mock import patch

from datalad.api import meta_filter
from datalad.utils import chpwd

from datalad.tests.utils_pytest import (
    assert_raises,
    with_tempfile,
)


meta_tree = {
    'sub': {
        'one': '1',
        'nothing': '2',
    },
}


@with_tempfile(mkdir=True)
def test_empty_dataset_filter_error(path=None):
    # Change into virgin dir to avoid detection of any dataset
    with chpwd(path):
        assert_raises(
            ValueError,
            meta_filter,
            filtername="metalad_demofilter",
            metadataurls=["."])


@with_tempfile(mkdir=True)
def test_unknown_filter_error(path=None):
    # Ensure failure on unavailable metadata filter
    with chpwd(path):
        assert_raises(
            ValueError,
            meta_filter,
            filtername="bogus__",
            metadataurls=["."])


def test_filter_call():
    with patch("datalad_metalad.filter.run_filter") as run_filter_mock:
        pass
