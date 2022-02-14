# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test metadata extraction"""
import subprocess
from uuid import UUID
from typing import Optional
from unittest.mock import patch

from datalad.distribution.dataset import Dataset
from datalad.api import meta_filter
from datalad.utils import chpwd

from datalad.tests.utils import (
    assert_repo_status,
    assert_raises,
    assert_result_count,
    assert_in,
    eq_,
    known_failure_windows,
    with_tempfile,
    with_tree
)

from dataladmetadatamodel.metadatapath import MetadataPath

from ..extract import get_extractor_class


meta_tree = {
    'sub': {
        'one': '1',
        'nothing': '2',
    },
}


@with_tempfile(mkdir=True)
def test_empty_dataset_filter_error(path):
    # Change into virgin dir to avoid detection of any dataset
    with chpwd(path):
        assert_raises(
            ValueError,
            meta_filter,
            filtername="metalad_demofilter",
            metadataurls=["."])


@with_tempfile(mkdir=True)
def test_unknown_filter_error(path):
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
