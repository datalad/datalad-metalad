# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test annex metadata extractor"""

from six import text_type

from datalad.distribution.dataset import Dataset
# API commands needed
from datalad.api import (
    create,
    rev_save,
    meta_extract,
)
from datalad.tests.utils import (
    with_tempfile,
    assert_result_count,
)


@with_tempfile
def test_annex_contentmeta(path):
    ds = Dataset(path).create()
    mfile_path = ds.pathobj / 'sudir' / 'dummy.txt'
    mfile_path.parent.mkdir()
    mfile_path.write_text(u'nothing')
    (ds.pathobj / 'ignored').write_text(u'nometa')
    ds.rev_save()
    ds.repo.set_metadata(
        text_type(mfile_path.relative_to(ds.pathobj)),
        init={'tag': 'mytag', 'fancy': 'this?'}
    )
    res = ds.meta_extract(sources=['metalad_annex'], process_type='content')
    # there are only results on files with annex metadata, nothing else
    #  dataset record, no records on files without annex metadata
    assert_result_count(res, 1)
    assert_result_count(
        res, 1,
        path=text_type(mfile_path),
        type='file',
        status='ok',
        metadata={'metalad_annex': {'tag': 'mytag', 'fancy': 'this?'}},
        action='meta_extract'
    )
