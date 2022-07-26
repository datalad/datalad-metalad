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
from datalad.tests.utils_pytest import (
    assert_equal,
    assert_result_count,
    known_failure_windows,
    with_tempfile,
)


@known_failure_windows
@with_tempfile
def test_annex_contentmeta(path=None):
    ds = Dataset(path).create()
    mfile_path = ds.pathobj / 'sudir' / 'dummy.txt'
    mfile_path.parent.mkdir()
    mfile_path.write_text(u'nothing')
    (ds.pathobj / 'ignored').write_text(u'nometa')
    ds.save(result_renderer="disabled")
    ds.repo.set_metadata(
        text_type(mfile_path.relative_to(ds.pathobj)),
        init={'tag': 'mytag', 'fancy': 'this?'}
    )
    res = ds.meta_extract(extractorname='metalad_annex', path=str(mfile_path))
    # there are only results on files with annex metadata, nothing else
    #  dataset record, no records on files without annex metadata
    assert_result_count(res, 1)
    assert_result_count(
        res, 1,
        path=text_type(mfile_path),
        type='file',
        status='ok',
        action='meta_extract')
    assert_equal(
        res[0]['metadata_record']['extracted_metadata'],
        {'tag': 'mytag', 'fancy': 'this?'}
    )
