# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test all extractors at a basic level"""

from __future__ import annotations

from datalad.support.entrypoints import iter_entrypoints
from datalad.tests.utils_pytest import (
    SkipTest,
    assert_equal,
    assert_repo_status,
    known_failure_githubci_win,
    with_tree,
)


def check_api(use_annex: bool):
    processed_extractors = []
    for ename, emod, eload in iter_entrypoints('datalad.metadata.extractors'):
        # we need to be able to query for metadata, even if there is none
        # from any extractor
        processed_extractors.append(ename)
    assert \
        "datalad_core" in processed_extractors, \
        "Should have managed to find at least the core extractor extractor"


@known_failure_githubci_win
def test_api_git():
    # should tollerate both pure git and annex repos
    check_api(False)


@known_failure_githubci_win
def test_api_annex():
    check_api(True)
