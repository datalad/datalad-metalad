# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test runprov metadata extractor"""

from six import text_type

from datalad.distribution.dataset import Dataset
from datalad.tests.utils_pytest import (
    assert_in,
    assert_not_in,
    assert_repo_status,
    assert_result_count,
    known_failure_windows,
    eq_,
    neq_,
    with_tree,
)


@known_failure_windows
@with_tree({'existing_file': 'some_content'})
def test_custom_dsmeta(path=None):
    ds = Dataset(path).create(force=True)
    # enable custom extractor
    # use default location
    ds.config.add('datalad.metadata.nativetype',
                  'metalad_runprov',
                  scope='branch')
    ds.save(result_renderer="disabled")
    assert_repo_status(ds.path)
    # run when there are no run records
    res = ds.meta_extract(extractorname='metalad_runprov')
    # no report
    assert_result_count(res, 0)

    # now let's have a record
    ds.run("cd .> dummy0", message="pristine")
    res = ds.meta_extract(extractorname='metalad_runprov')
    assert_result_count(res, 1, type='dataset', path=ds.path)
    res = ds.meta_extract(extractorname='metalad_runprov', path='existing_file')
    assert_result_count(res, 0)
    res = ds.meta_extract(extractorname='metalad_runprov', path='dummy0')
    assert_result_count(res, 1)
    eq_(res[0]["type"], "file")
    eq_(res[0]["path"], text_type(ds.pathobj / 'dummy0'))
    for r in res:
        # we have something from the extractor
        md = r.get('metadata_record', {}).get('extracted_metadata', None)
        neq_(md, None)
        assert(isinstance(md, dict))
        if r['type'] == 'file':
            # simple report on a file, any non-file reports must come with
            # the dataset
            assert_not_in('@graph', md)
            for k in ('@id', '@type'):
                assert_in(k, md)
        else:
            # dataset report
            # multi-document report on a dataset, at least an agent and an
            # activity
            assert_in('@graph', md)
            nodes = md['@graph']
            assert(len(nodes) > 1)
            assert(all('@type' in n and '@id' in n for n in nodes))
            assert(any(n['@type'] == 'activity' for n in nodes))
            assert(any(n['@type'] == 'agent' for n in nodes))

    # switching works in standard fashion
    dsres = ds.meta_extract(extractorname='metalad_runprov')
    assert_result_count(dsres, 1)
    assert_result_count(
        dsres, 1, type='dataset', path=ds.path)
    fileres = ds.meta_extract(
        extractorname='metalad_runprov', path='dummy0')
    assert_result_count(fileres, 1)
    assert_result_count(
        fileres, 1, type='file', path=text_type(ds.pathobj / 'dummy0'))

    # smoke test to see if anything breaks with a record in a sidecar
    # file
    # ATM we are not doing anything with the information in the sidecar
    # for fear of leakage
    ds.run("cd .> dummy_side", message="sidecar arg", sidecar=True)
    res = ds.meta_extract(extractorname='metalad_runprov')
    assert_result_count(res, 1)
    assert_result_count(res, 1, type='dataset', path=ds.path)
    res = ds.meta_extract(extractorname='metalad_runprov', path='dummy0')
    assert_result_count(res, 1)
    eq_(res[0]['type'], 'file')
    res = ds.meta_extract(extractorname='metalad_runprov', path='dummy_side')
    assert_result_count(res, 1)
    eq_(res[0]['type'], 'file')
    assert_result_count(
        res, 1, type='file', path=text_type(ds.pathobj / 'dummy_side'))

    # check that it survives a partial report (no _core metadata extracted)
    # for JSON-LD reporting
    res = ds.meta_extract(extractorname='metalad_runprov')
    # only a single results
    assert_result_count(res, 1)
    # 2 actvities, 1 agent, 2 generated entities
    eq_(len(res[0]['metadata_record']['extracted_metadata']['@graph']), 3)
    # all properly ID'ed
    assert(all('@id' in d for d in res[0]['metadata_record']['extracted_metadata']['@graph']))
