# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test generic json file-level metadata extractor"""

import json
from datalad.support.exceptions import CapturedException
from datalad.distribution.dataset import Dataset
from datalad.tests.utils_pytest import (
    assert_in_results,
    assert_raises,
    assert_repo_status,
    assert_result_count,
    assert_status,
    assert_equal,
    known_failure_windows,
    with_tree,
)


@known_failure_windows
@with_tree(
    tree={
        'sub': {
            'one': '1',
            '_one.dl.json': '{"some":"thing"}',
        }
    })
def test_custom_contentmeta(path=None):
    ds = Dataset(path).create(force=True)
    # use custom location
    # ds.config.add('datalad.metadata.custom-content-source',
    #               '{freldir}/_{fname}.dl.json',
    #               scope='branch')
    ds.save(result_renderer="disabled")
    res = ds.meta_extract(
        extractorname='metalad_genericjson_file',
        extractorargs=['metadata_source', '{freldir}/_{fname}.dl.json'],
        path="sub/one",
        result_renderer="disabled")
    assert_status('ok', res)
    assert_result_count(res, 1)
    assert_equal(res[0]["metadata_record"]["extracted_metadata"], {
            'some': 'thing',
    })


@with_tree(
    tree={
        '.metadata': {
            'content': {
                'sub': {
                    'one.json': 'not JSON',
                },
            },
        },
        'sub': {
            'one': '1',
        }
    })
def test_custom_content_broken(path=None):
    ds = Dataset(path).create(force=True)
    ds.save(result_renderer="disabled")
    res = ds.meta_extract(
        extractorname='metalad_genericjson_file',
        path='sub/one',
        on_failure='ignore',
        result_renderer="disabled")
    assert_result_count(res, 1)
    assert_in_results(
        res,
        action="meta_extract",
        status="error",
        path=ds.pathobj / 'sub' / 'one',
    )

@with_tree(
    tree={
        '.metadata': {
            'content': {
                'sub': {
                    'one.json': '{"correct":"JSON"}',
                },
            },
        },
        'sub': {
            'one': '1',
        }
    })
def test_default_content(path=None):
    ds = Dataset(path).create(force=True)
    ds.save(result_renderer="disabled")
    res = ds.meta_extract(
        extractorname='metalad_genericjson_file',
        path='sub/one',
        on_failure='ignore',
        result_renderer="disabled")
    assert_result_count(res, 1)
    assert_in_results(
        res,
        action="meta_extract",
        status="ok",
        path=ds.pathobj / 'sub' / 'one',
    )
    
