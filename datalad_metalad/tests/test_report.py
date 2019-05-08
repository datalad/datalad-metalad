import os.path as op
from six import (
    text_type,
    iteritems,
)

from datalad.support.gitrepo import GitRepo
from datalad.support.annexrepo import AnnexRepo
from datalad.distribution.dataset import Dataset

from datalad.api import (
    install,
    meta_report,
    meta_aggregate,
)
from datalad.utils import (
    chpwd,
)
from datalad.tests.utils import (
    with_tree,
    with_tempfile,
    assert_result_count,
    assert_true,
    assert_raises,
    assert_repo_status,
    eq_,
)
from . import (
    make_ds_hierarchy_with_metadata,
)


@with_tempfile(mkdir=True)
def test_ignore_nondatasets(path):
    # we want to ignore the version/commits for this test
    def _kill_time(meta):  # pragma: no cover
        for m in meta:
            for k in ('version', 'shasum'):
                if k in m:
                    del m[k]
        return meta

    ds = Dataset(path).create()
    meta = _kill_time(
        ds.meta_report(reporton='datasets', on_failure='ignore'))
    n_subm = 0
    # placing another repo in the dataset has no effect on metadata
    for cls, subpath in ((GitRepo, 'subm'), (AnnexRepo, 'annex_subm')):
        subm_path = op.join(ds.path, subpath)
        r = cls(subm_path, create=True)
        with open(op.join(subm_path, 'test'), 'w') as f:
            f.write('test')
        r.add('test')
        r.commit('some')
        assert_true(Dataset(subm_path).is_installed())
        eq_(meta,
            _kill_time(
                ds.meta_report(reporton='datasets', on_failure='ignore')))
        # making it a submodule has no effect either
        ds.rev_save(subpath)
        eq_(len(ds.subdatasets()), n_subm + 1)
        eq_(meta,
            _kill_time(
                ds.meta_report(reporton='datasets', on_failure='ignore')))
        n_subm += 1


@with_tree({'dummy': 'content'})
@with_tempfile(mkdir=True)
def test_bf2458(src, dst):
    ds = Dataset(src).create(force=True)
    ds.rev_save(to_git=False)

    # no clone (empty) into new dst
    clone = install(source=ds.path, path=dst)
    # XXX whereis says nothing in direct mode
    # content is not here
    eq_(clone.repo.whereis('dummy'), [ds.config.get('annex.uuid')])
    # check that plain metadata access does not `get` stuff
    clone.meta_report('.', on_failure='ignore')
    # XXX whereis says nothing in direct mode
    eq_(clone.repo.whereis('dummy'), [ds.config.get('annex.uuid')])


@with_tempfile(mkdir=True)
def test_get_aggregates_fails(path):
    with chpwd(path), assert_raises(ValueError):
        meta_report(reporton='aggregates')
    ds = Dataset(path).create()
    res = ds.meta_report(reporton='aggregates', on_failure='ignore')
    assert_result_count(res, 1, path=ds.path, status='impossible')


@with_tempfile(mkdir=True)
def test_query_empty(path):
    with chpwd(path), assert_raises(ValueError):
        meta_report()
    ds = Dataset(path).create()
    res = ds.meta_report(on_failure='ignore')
    assert_result_count(res, 1)
    assert_result_count(
        res, 1,
        status='impossible',
        message='metadata aggregation has never been performed '
        'in this dataset',
    )


@with_tempfile
@with_tempfile
def test_query(path, orig):
    origds, subds = make_ds_hierarchy_with_metadata(orig)
    origds.meta_aggregate(recursive=True)
    assert_repo_status(origds.path)
    # now clone to a new place to ensure no content is present
    ds = install(source=origds.path, path=path)
    res = ds.meta_report()
    # we get identical results in the local, (initially) empty clone
    # and the original dataset with all the real content and aggregated
    # metadata
    for remote, local in zip(res, origds.meta_report()):
        eq_(remote['metadata'], local['metadata'])
    # we get nothing on the subdataset without recursion (see test below)
    assert_result_count(res, 0, dsid=subds.id)
    # make path-specific queries

    # fails on queries outside the dataset
    assert_raises(ValueError, ds.meta_report, path=orig)
    # asking for a file, we only get a report for this file
    res = ds.meta_report('file.dat', reporton='files')
    assert_result_count(res, 1)
    assert_result_count(
        res, 1, dsid=origds.id, path=text_type(ds.pathobj / 'file.dat')
    )

    # now with "recursion" to get info on the subdatasets
    res = ds.meta_report(recursive=True)
    # now we see the subdataset too
    assert_result_count(res, 1, dsid=subds.id, type='dataset')

    # same distinction re recursion hold for aggregate reporting
    res = ds.meta_report(reporton='aggregates')
    assert_result_count(res, 1)
    assert_result_count(res, 1, id=origds.id)
    res = ds.meta_report(reporton='aggregates', recursive=True)
    assert_result_count(res, 2)
    assert_result_count(res, 1, id=origds.id)
    assert_result_count(res, 1, id=subds.id)

    extract_res = origds.meta_extract(format='jsonld')
    assert_result_count(extract_res, 1)
    query_res = ds.meta_report(reporton='jsonld')
    assert_result_count(query_res, 1)
    strip = ('path', 'action', 'refds')
    eq_(
        {k: v for k, v in iteritems(extract_res[0]) if k not in strip},
        {k: v for k, v in iteritems(query_res[0]) if k not in strip},
    )
