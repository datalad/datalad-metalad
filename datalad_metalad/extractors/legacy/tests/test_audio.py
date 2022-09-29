# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test audio extractor"""

from pathlib import Path

from datalad.tests.utils_pytest import (
    SkipTest,
    assert_repo_status,
    assert_result_count,
    eq_,
    with_tempfile,
)

try:
    import mutagen
except ImportError:
    raise SkipTest

from shutil import copy

from datalad.api import Dataset


target = {
    "format": "mime:audio/mp3",
    "duration(s)": 1.0,
    "name": "dltracktitle",
    "music:album": "dlalbumtitle",
    "music:artist": "dlartist",
    "music:channels": 1,
    "music:sample_rate": 44100,
    "music:Genre": "dlgenre",
    "date": "",
    "tracknumber": "dltracknumber",
}


@with_tempfile(mkdir=True)
def test_audio(path=None):
    ds = Dataset(path).create()
    copy(Path(__file__).parent / 'data' / 'audio.mp3', path)
    ds.save()
    assert_repo_status(ds.path)

    res = ds.meta_extract('audio', str(Path(path) / 'audio.mp3'))
    assert_result_count(res, 1)

    # from this extractor
    meta = res[0]['metadata_record']['extracted_metadata']
    for k, v in target.items():
        eq_(meta[k], v)
