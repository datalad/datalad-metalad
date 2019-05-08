# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test metadata aggregation"""


import os.path as op
from six import text_type
from simplejson import dumps as jsondumps

from datalad.api import (
    meta_report,
    install,
    create,
    meta_aggregate,
)
from datalad.distribution.dataset import Dataset

from datalad.utils import (
    chpwd,
    assure_unicode,
    Path,
    PurePosixPath,
)
from datalad.tests.utils import (
    slow,
    skip_ssh,
    with_tree,
    with_tempfile,
    assert_result_count,
    assert_raises,
    assert_status,
    assert_true,
    assert_in,
    assert_repo_status,
    assert_dict_equal,
    eq_,
    skip_if_on_windows,
)
from . import (
    make_ds_hierarchy_with_metadata,
    _get_dsid_from_core_metadata,
)


def _assert_metadata_empty(meta):
    ignore = set(['@id', '@context'])
    assert (not len(meta) or set(meta.keys()) == ignore), \
        'metadata record is not empty: {}'.format(
            {k: meta[k] for k in meta if k not in ignore})


_dataset_hierarchy_template_friction = {
    'origin': {
        'datapackage.json': """
{
    "name": "MOTHER_äöü東",
    "keywords": ["example", "multitype metadata"]
}""",
    'sub': {
        'datapackage.json': """
{
    "name": "child_äöü東"
}""",
    'subsub': {
        'datapackage.json': """
{
    "name": "grandchild_äöü東"
}"""}}}}


_dataset_hierarchy_template_bids = {
    'origin': {
        'dataset_description.json': """
{
    "Name": "mother_äöü東"
}""",
        'sub': {
            'dataset_description.json': """
{
    "Name": "child_äöü東"
}""",
            'subsub': {
                'dataset_description.json': """
            {
    "Name": "grandchild_äöü東"
}"""}}}}


@with_tree(tree=_dataset_hierarchy_template_bids)
def test_basic_aggregate(path):
    # TODO give datasets some more metadata to actually aggregate stuff
    base = Dataset(op.join(path, 'origin')).create(force=True)
    sub = base.create('sub', force=True)
    #base.meta_report(sub.path, init=dict(homepage='this'), apply2global=True)
    subsub = base.create(op.join('sub', 'subsub'), force=True)
    base.rev_save(recursive=True)
    assert_repo_status(base.path)
    # we will first aggregate the middle dataset on its own, this will
    # serve as a smoke test for the reuse of metadata objects later on
    sub.meta_aggregate()
    base.rev_save()
    assert_repo_status(base.path)
    base.meta_aggregate(recursive=True, into='all')
    assert_repo_status(base.path)
    direct_meta = base.meta_report(recursive=True, return_type='list')
    # loose the deepest dataset
    sub.uninstall('subsub', check=False)
    # no we should be able to reaggregate metadata, and loose nothing
    # because we can aggregate aggregated metadata of subsub from sub
    base.meta_aggregate(recursive=True, into='all')
    # same result for aggregate query than for (saved) direct query
    agg_meta = base.meta_report(recursive=True, return_type='list')
    for d, a in zip(direct_meta, agg_meta):
        assert_dict_equal(d, a)
    # no we can throw away the subdataset tree, and loose no metadata
    base.uninstall('sub', recursive=True, check=False)
    assert(not sub.is_installed())
    assert_repo_status(base.path)
    # same result for aggregate query than for (saved) direct query
    agg_meta = base.meta_report(recursive=True, return_type='list')
    for d, a in zip(direct_meta, agg_meta):
        assert_dict_equal(d, a)


def _compare_metadata_helper(origres, compds):
    for ores in origres:
        rpath = op.relpath(ores['path'], ores['refds'])
        cres = compds.meta_report(
            rpath,
            reporton='{}s'.format(ores['type']))
        if ores['type'] == 'file':
            # TODO implement file based lookup
            continue
        assert_result_count(cres, 1)
        cres = cres[0]
        assert_dict_equal(ores['metadata'], cres['metadata'])
        if ores['type'] == 'dataset':
            eq_(_get_dsid_from_core_metadata(ores['metadata']['metalad_core']),
                _get_dsid_from_core_metadata(cres['metadata']['metalad_core']))


@slow  # ~16s
@with_tree(tree=_dataset_hierarchy_template_friction)
def test_aggregation(path):
    # a hierarchy of three (super/sub)datasets, each with some native metadata
    ds = Dataset(op.join(path, 'origin')).create(force=True)
    ds.config.add('datalad.metadata.nativetype', 'frictionless_datapackage',
                  where='dataset')
    subds = ds.create('sub', force=True)
    subds.config.add('datalad.metadata.nativetype', 'frictionless_datapackage',
                     where='dataset')
    subsubds = subds.create('subsub', force=True)
    subsubds.config.add('datalad.metadata.nativetype', 'frictionless_datapackage',
                        where='dataset')
    assert_status('ok', ds.rev_save(recursive=True))
    # while we are at it: dot it again, nothing should happen
    assert_status('notneeded', ds.rev_save(recursive=True))

    assert_repo_status(ds.path)
    # aggregate metadata from all subdatasets into any superdataset, including
    # intermediate ones
    res = ds.meta_aggregate(recursive=True, into='all')
    # we get success report for both subdatasets and the superdataset,
    # and they get saved
    assert_result_count(res, 3, status='ok', action='meta_aggregate')
    # the respective super datasets see two saves, one to record the change
    # in the subdataset after its own aggregation, and one after the super
    # updated with aggregated metadata
    assert_result_count(res, 5, status='ok', action='save', type='dataset')
    # nice and tidy
    assert_repo_status(ds.path)

    # quick test of aggregate report
    aggs = ds.meta_report(reporton='aggregates', recursive=True)
    # one for each dataset
    assert_result_count(aggs, 3)
    # mother also report layout version
    assert_result_count(aggs, 1, path=ds.path, layout_version=1)

    # store clean direct result
    origres = ds.meta_report(recursive=True)
    # basic sanity check
    assert_result_count(origres, 3, type='dataset')
    assert_result_count(
        [r for r in origres if r['path'].endswith('.json')],
        3, type='file')  # Now that we have annex.key
    # three different IDs
    eq_(
        3,
        len(set([_get_dsid_from_core_metadata(s['metadata']['metalad_core'])
                 for s in origres
                 if s['type'] == 'dataset'])))
    # and we know about all three datasets
    for name in ('MOTHER_äöü東', 'child_äöü東', 'grandchild_äöü東'):
        assert_true(
            sum([s['metadata']['frictionless_datapackage']['name'] \
                    == assure_unicode(name) for s in origres
                 if s['type'] == 'dataset']))

    # now clone the beast to simulate a new user installing an empty dataset
    clone = install(
        op.join(path, 'clone'), source=ds.path,
        result_xfm='datasets', return_type='item-or-list')
    # ID mechanism works
    eq_(ds.id, clone.id)

    # get fresh metadata
    cloneres = clone.meta_report()
    # basic sanity check
    assert_result_count(cloneres, 1, type='dataset')
    # payload file
    assert_result_count(cloneres, 1, type='file')

    # now loop over the previous results from the direct metadata query of
    # origin and make sure we get the extact same stuff from the clone
    _compare_metadata_helper(origres, clone)

    # now obtain a subdataset in the clone, should make no difference
    assert_status('ok', clone.install('sub', result_xfm=None, return_type='list'))
    _compare_metadata_helper(origres, clone)

    # test search in search tests, not all over the place
    ## query smoke test
    assert_result_count(clone.search('mother', mode='egrep'), 1)
    assert_result_count(clone.search('(?i)MoTHER', mode='egrep'), 1)

    child_res = clone.search('child', mode='egrep')
    assert_result_count(child_res, 2)
    for r in child_res:
        if r['type'] == 'dataset':
            assert_in(
                r['query_matched']['frictionless_datapackage.name'],
                r['metadata']['frictionless_datapackage']['name'])

    ## Test 'and' for multiple search entries
    #assert_result_count(clone.search(['*child*', '*bids*']), 2)
    #assert_result_count(clone.search(['*child*', '*subsub*']), 1)
    #assert_result_count(clone.search(['*bids*', '*sub*']), 2)

    #assert_result_count(clone.search(['*', 'type:dataset']), 3)

    ##TODO update the clone or reclone to check whether saved metadata comes down the pipe


# tree puts aggregate metadata structures on two levels inside a dataset
@with_tree(tree={
    '.datalad': {
        'metadata': {
            'objects': {
                'someshasum': '{"homepage": "http://top.example.com"}'},
            'aggregate_v1.json': """\
{
    "sub/deep/some": {
        "dataset_info": "objects/someshasum"
    }
}
"""}},
    'sub': {
        '.datalad': {
            'metadata': {
                'objects': {
                    'someotherhash': '{"homepage": "http://sub.example.com"}'},
                'aggregate_v1.json': """\
{
    "deep/some": {
        "dataset_info": "objects/someotherhash"
    }
}
"""}}},
})
@with_tempfile(mkdir=True)
def test_aggregate_query(path, randompath):
    ds = Dataset(path).create(force=True)
    # no magic change to actual dataset metadata due to presence of
    # aggregated metadata
    res = ds.meta_report(reporton='datasets', on_failure='ignore')
    assert_result_count(res, 0)
    # but we can now ask for metadata of stuff that is unknown on disk
    res = ds.meta_report(op.join('sub', 'deep', 'some'), reporton='datasets')
    assert_result_count(res, 1)
    eq_({'homepage': 'http://top.example.com'}, res[0]['metadata'])
    sub = ds.create('sub', force=True)
    # when no reference dataset there is NO magic discovery of the relevant
    # dataset
    with chpwd(randompath):
        assert_raises(ValueError, meta_report,
            op.join(path, 'sub', 'deep', 'some'), reporton='datasets')
    # but inside a dataset things work
    with chpwd(ds.path):
        res = meta_report(
            op.join(path, 'sub', 'deep', 'some'),
            reporton='datasets')
        assert_result_count(res, 1)
        # the metadata in the discovered top dataset is return, not the
        # metadata in the subdataset
        eq_({'homepage': 'http://top.example.com'}, res[0]['metadata'])
    # when a reference dataset is given, it will be used as the metadata
    # provider
    res = sub.meta_report(op.join('deep', 'some'), reporton='datasets')
    assert_result_count(res, 1)
    eq_({'homepage': 'http://sub.example.com'}, res[0]['metadata'])


# this is for gh-1971
@with_tree(tree=_dataset_hierarchy_template_bids)
def test_reaggregate_with_unavailable_objects(path):
    base = Dataset(op.join(path, 'origin')).create(force=True)
    # force all metadata objects into the annex
    with open(op.join(base.path, '.datalad', '.gitattributes'), 'w') as f:
        f.write(
            '** annex.largefiles=nothing\nmetadata/objects/** annex.largefiles=anything\n')
    sub = base.create('sub', force=True)
    subsub = base.create(op.join('sub', 'subsub'), force=True)
    base.rev_save(recursive=True)
    assert_repo_status(base.path)
    # first a quick check that an unsupported 'into' mode causes an exception
    assert_raises(
        ValueError, base.meta_aggregate, recursive=True,
        into='spaceship')
    # no for real
    base.meta_aggregate(recursive=True, into='all')
    assert_repo_status(base.path)
    objpath = op.join('.datalad', 'metadata', 'objects')
    objs = list(sorted(base.repo.find(objpath)))
    # we have 3x2 metadata sets (dataset/files) under annex
    eq_(len(objs), 6)
    eq_(all(base.repo.file_has_content(objs)), True)
    # drop all object content
    base.drop(objs, check=False)
    eq_(all(base.repo.file_has_content(objs)), False)
    assert_repo_status(base.path)
    # now re-aggregate, the state hasn't changed, so the file names will
    # be the same
    base.meta_aggregate(recursive=True, into='all', force='fromscratch')
    eq_(all(base.repo.file_has_content(objs)), True)
    # and there are no new objects
    eq_(
        objs,
        list(sorted(base.repo.find(objpath)))
    )


@with_tree(tree=_dataset_hierarchy_template_bids)
@with_tempfile(mkdir=True)
def test_aggregate_with_unavailable_objects_from_subds(path, target):
    base = Dataset(op.join(path, 'origin')).create(force=True)
    # force all metadata objects into the annex
    with open(op.join(base.path, '.datalad', '.gitattributes'), 'w') as f:
        f.write(
            '** annex.largefiles=nothing\nmetadata/objects/** annex.largefiles=anything\n')
    sub = base.create('sub', force=True)
    subsub = base.create(op.join('sub', 'subsub'), force=True)
    base.rev_save(recursive=True)
    assert_repo_status(base.path)
    base.meta_aggregate(recursive=True, into='all')
    assert_repo_status(base.path)

    # now make that a subdataset of a new one, so aggregation needs to get the
    # metadata objects first:
    super = Dataset(target).create()
    super.install("base", source=base.path)
    assert_repo_status(super.path)
    clone = Dataset(op.join(super.path, "base"))
    assert_repo_status(clone.path)
    objpath = PurePosixPath('.datalad/metadata/objects')
    objs = [o for o in sorted(clone.repo.get_annexed_files(with_content_only=False))
            if objpath in PurePosixPath(o).parents]
    eq_(len(objs), 6)
    eq_(all(clone.repo.file_has_content(objs)), False)

    # now aggregate should get those metadata objects
    super.meta_aggregate(recursive=True, into='all')
    eq_(all(clone.repo.file_has_content(objs)), True)


# this is for gh-1987
@skip_if_on_windows  # create_sibling incompatible with win servers
@skip_ssh
@with_tree(tree=_dataset_hierarchy_template_bids)
def test_publish_aggregated(path):
    base = Dataset(op.join(path, 'origin')).create(force=True)
    # force all metadata objects into the annex
    with open(op.join(base.path, '.datalad', '.gitattributes'), 'w') as f:
        f.write(
            '** annex.largefiles=nothing\nmetadata/objects/** annex.largefiles=anything\n')
    base.create('sub', force=True)
    base.rev_save(recursive=True)
    assert_repo_status(base.path)
    base.meta_aggregate(recursive=True, into='all')
    assert_repo_status(base.path)

    # create sibling and publish to it
    spath = op.join(path, 'remote')
    base.create_sibling(
        name="local_target",
        sshurl="ssh://localhost",
        target_dir=spath)
    base.publish('.', to='local_target', transfer_data='all')
    remote = Dataset(spath)
    objpath = op.join('.datalad', 'metadata', 'objects')
    objs = list(sorted(base.repo.find(objpath)))
    # all object files a present in both datasets
    eq_(all(base.repo.file_has_content(objs)), True)
    eq_(all(remote.repo.file_has_content(objs)), True)
    # and we can squeeze the same metadata out
    eq_(
        [{k: v for k, v in i.items() if k not in ('path', 'refds', 'parentds')}
         for i in base.meta_report('sub')],
        [{k: v for k, v in i.items() if k not in ('path', 'refds', 'parentds')}
         for i in remote.meta_report('sub')],
    )


def _get_contained_objs(ds):
    root = ds.pathobj / '.datalad' / 'metadata' / 'objects'
    return set(f for f in ds.repo.get_indexed_files()
               if root in (ds.pathobj / PurePosixPath(f)).parents)


def _get_referenced_objs(ds):
    return set([Path(r[f]).relative_to(ds.pathobj).as_posix()
                for r in ds.meta_report(reporton='aggregates', recursive=True)
                for f in ('content_info', 'dataset_info')])


@with_tree(tree=_dataset_hierarchy_template_bids)
def test_aggregate_removal(path):
    base = Dataset(op.join(path, 'origin')).create(force=True)
    # force all metadata objects into the annex
    with open(op.join(base.path, '.datalad', '.gitattributes'), 'w') as f:
        f.write(
            '** annex.largefiles=nothing\nmetadata/objects/** annex.largefiles=anything\n')
    sub = base.create('sub', force=True)
    subsub = sub.create(op.join('subsub'), force=True)
    base.rev_save(recursive=True)
    base.meta_aggregate(recursive=True, into='all')
    assert_repo_status(base.path)
    res = base.meta_report(reporton='aggregates', recursive=True)
    assert_result_count(res, 3)
    assert_result_count(res, 1, path=subsub.path)
    # check that we only have object files that are listed in agginfo
    eq_(_get_contained_objs(base), _get_referenced_objs(base))
    # now delete the deepest subdataset to test cleanup of aggregated objects
    # in the top-level ds
    base.remove(op.join('sub', 'subsub'), check=False)
    # now aggregation has to detect that subsub is not simply missing, but gone
    # for good
    base.meta_aggregate(recursive=True, into='all')
    assert_repo_status(base.path)
    # internally consistent state
    eq_(_get_contained_objs(base), _get_referenced_objs(base))
    # info on subsub was removed at all levels
    res = base.meta_report(reporton='aggregates', recursive=True)
    assert_result_count(res, 0, path=subsub.path)
    assert_result_count(res, 2)
    res = sub.meta_report(reporton='aggregates', recursive=True)
    assert_result_count(res, 0, path=subsub.path)
    assert_result_count(res, 1)


@with_tree(tree=_dataset_hierarchy_template_bids)
def test_update_strategy(path):
    base = Dataset(op.join(path, 'origin')).create(force=True)
    # force all metadata objects into the annex
    with open(op.join(base.path, '.datalad', '.gitattributes'), 'w') as f:
        f.write(
            '** annex.largefiles=nothing\nmetadata/objects/** annex.largefiles=anything\n')
    sub = base.create('sub', force=True)
    subsub = sub.create(op.join('subsub'), force=True)
    base.rev_save(recursive=True)
    assert_repo_status(base.path)
    # we start clean
    for ds in base, sub, subsub:
        eq_(len(_get_contained_objs(ds)), 0)
    # aggregate the base dataset only, nothing below changes
    base.meta_aggregate()
    eq_(len(_get_contained_objs(base)), 2)
    for ds in sub, subsub:
        eq_(len(_get_contained_objs(ds)), 0)
    # aggregate the entire tree, but by default only updates
    # the top-level dataset with all objects, none of the leaf
    # or intermediate datasets get's touched
    base.meta_aggregate(recursive=True)
    eq_(len(_get_contained_objs(base)), 6)
    eq_(len(_get_referenced_objs(base)), 6)
    for ds in sub, subsub:
        eq_(len(_get_contained_objs(ds)), 0)
    res = base.meta_report(reporton='aggregates', recursive=True)
    assert_result_count(res, 3)
    # it is impossible to query an intermediate or leaf dataset
    # for metadata
    for ds in sub, subsub:
        assert_status(
            'impossible',
            ds.meta_report(reporton='aggregates', on_failure='ignore'))
    # get the full metadata report
    target_meta = _kill_time(base.meta_report())

    # now redo full aggregation, this time updating all
    # (intermediate) datasets
    base.meta_aggregate(recursive=True, into='all')
    eq_(len(_get_contained_objs(base)), 6)
    eq_(len(_get_contained_objs(sub)), 4)
    eq_(len(_get_contained_objs(subsub)), 2)
    # it is now OK to query an intermediate or leaf dataset
    # for metadata
    for ds in sub, subsub:
        assert_status(
            'ok',
            ds.meta_report(reporton='aggregates', on_failure='ignore'))

    # all of that has no impact on the reported metadata
    # minus the change in the refcommits
    for i in zip(target_meta, _kill_time(base.meta_report())):
        assert_dict_equal(i[0], i[1])


def _kill_time(iter):
    m = []
    for r in iter:
        # TODO why is it two of them?
        r.pop('refcommit', None)
        for k in ('@id', 'dateModified', 'version'):
            if '@graph' in r['metadata']['metalad_core']:
                for doc in r['metadata']['metalad_core']['@graph']:
                    doc.pop(k, None)
                    if 'hasPart' in doc:
                        # for shasum-based IDs
                        for i in doc['hasPart']:
                            i.pop(k, None)
            else:
                r.pop(k, None)
        m.append(r)
    return m


@with_tree({
    'this': 'that',
    'sub1': {'here': 'there'},
    'sub2': {'down': 'under'}})
def test_partial_aggregation(path):
    ds = Dataset(path).create(force=True)
    sub1 = ds.create('sub1', force=True)
    sub2 = ds.create('sub2', force=True)
    ds.rev_save(recursive=True)

    # if we aggregate a path(s) and say to recurse, we must not recurse into
    # the dataset itself and aggregate others
    ds.meta_aggregate(path='sub1', recursive=True)
    res = ds.meta_report(reporton='aggregates', recursive=True)
    assert_result_count(res, 1, path=ds.path)
    assert_result_count(res, 1, path=sub1.path)
    # so no metadata aggregates for sub2 yet
    assert_result_count(res, 0, path=sub2.path)

    ds.meta_aggregate(recursive=True)
    origsha = ds.repo.get_hexsha()
    assert_repo_status(ds.path)
    # baseline, recursive aggregation gets us something for all three datasets
    res = ds.meta_report(reporton='aggregates', recursive=True)
    assert_result_count(res, 3)
    # now let's do partial aggregation from just one subdataset
    # we should not loose information on the other datasets
    # as this would be a problem any time anything in a dataset
    # subtree is missing: not installed, too expensive to reaggregate, ...
    ds.meta_aggregate(path='sub1')
    eq_(origsha, ds.repo.get_hexsha())
    res = ds.meta_report(reporton='aggregates', recursive=True)
    assert_result_count(res, 3)
    assert_result_count(res, 1, path=sub2.path)
    # nothing changes, so no commit
    ds.meta_aggregate(path='sub1')
    eq_(origsha, ds.repo.get_hexsha())
    # and the same thing again, doesn't ruin the state either
    ds.meta_aggregate(path='sub1')
    eq_(origsha, ds.repo.get_hexsha())
    # from-scratch aggregation kills datasets that where not listed
    # note the trailing separator that indicated that path refers
    # to the content of the subdataset, not the subdataset record
    # in the superdataset
    ds.meta_aggregate(path='sub1' + op.sep, force='fromscratch')
    res = ds.meta_report(reporton='aggregates', recursive=True)
    assert_result_count(res, 1)
    assert_result_count(res, 1, path=sub1.path)
    # now reaggregated in full
    ds.meta_aggregate(recursive=True)
    # make change in sub1
    sub1.unlock('here')
    with open(op.join(sub1.path, 'here'), 'w') as f:
        f.write('fresh')
    ds.rev_save(recursive=True)
    assert_repo_status(path)
    # TODO for later
    # test --since with non-incremental
    #ds.meta_aggregate(recursive=True, since='HEAD~1', incremental=False)
    #res = ds.rev_metadata(reporton='aggregates')
    #assert_result_count(res, 3)
    #assert_result_count(res, 1, path=sub2.path)


@with_tempfile(mkdir=True)
def test_aggregate_fail(path):
    ds = Dataset(path).create()
    # we need one real piece of content
    (ds.pathobj / 'real').write_text(text_type('real'))
    ds.rev_save()
    (ds.pathobj / 'dummy').write_text(text_type('blurb'))
    assert_repo_status(ds.path, untracked=['dummy'])
    # aggregation will not fail, untracked content is simply ignored
    assert_status('ok', ds.meta_aggregate())
    # but with staged changes it does
    ds.repo.add(['dummy'])
    assert_result_count(
        ds.meta_aggregate(on_failure='ignore'),
        1,
        path=ds.path,
        type='dataset',
        status='error',
        message="dataset has pending changes",
    )


def _prep_partial_update_ds(path):
    ds, subds = make_ds_hierarchy_with_metadata(path)
    # add one more subds
    subds2 = ds.create(op.join('down', 'sub'))
    # we need one real piece of content
    # important to use a different name than the file in subds1
    # so we have two metadata objects with different hashes
    (subds2.pathobj / 'realsub2').write_text(text_type('real'))
    ds.rev_save(recursive=True)
    return ds, subds, subds2


@with_tempfile(mkdir=True)
def test_reaggregate(path):
    ds, subds1, subds2 = _prep_partial_update_ds(path)
    # the actual job
    assert_status('ok', ds.meta_aggregate(recursive=True))
    # nothing without a modification
    assert_status('notneeded', ds.meta_aggregate(recursive=True))
    # modify subds1
    (subds1.pathobj / 'new').write_text(text_type('content'))
    ds.rev_save(recursive=True)
    # go for a full re-aggregation, it should do the right thing
    # and only re-extract from subds1 and the root dataset
    # as these are the only ones with changes
    res = ds.meta_aggregate(recursive=True)
    # we should see three deletions, two for the replaced metadata blobs
    # of the modified subdataset, and one for the dataset metadata of the super
    assert_result_count(res, 3, action='delete')
    # four additions: two new blobs for the subdataset, one dataset
    # metadata blob for the root, due to a new modification date
    # and the aggregate catalog
    assert_result_count(res, 4, action='add')
    # partial reaggregation has tidied up everything nicely, so a
    # full aggregation does nothing
    good_state = ds.repo.get_hexsha()
    assert_status('notneeded', ds.meta_aggregate(recursive=True))
    # given a contraining path with also not trigger any further action
    eq_(good_state, ds.repo.get_hexsha())
    assert_status(
        'notneeded',
        ds.meta_aggregate(path='down', recursive=True)
    )
    eq_(good_state, ds.repo.get_hexsha())
    # but we can force extraction and get a selective update for this one
    # dataset only
    # not pointing to a subdataset itself, but do recursion from a subdirectory
    # downwards
    # but without an actual dataset change, and no change to an extractor's
    # output nothing will change in the dataset
    ds.meta_aggregate(path='down', recursive=True, force='extraction')
    eq_(good_state, ds.repo.get_hexsha())


sample_fmeta1 = {
    "something": "stupid",
    "complextype": {
        "entity": {
            "some": "many",
            "properties": "here",
        },
        "age": "young",
        "numbers": [3, 2, 1, 0],
    }
}
sample_fmeta2 = {
    # same as above
    "something": "stupid",
    # different complex type
    "complextype": {
        "entity": {
            "some": "few",
            "properties": "here",
        },
    }
}
custom_metadata_tree = {
    '.metadata': {
        'content': {
            'sub': {
                'one.json': jsondumps(sample_fmeta1),
                'two.json': jsondumps(sample_fmeta2),
                'three.json': jsondumps(sample_fmeta1),
            },
        },
    },
    'sub': {
        'one': '1',
        'two': '2',
        'three': '3',
    },
}


@with_tree(custom_metadata_tree)
def test_unique_values(path):
    ds = Dataset(path).create(force=True)
    ds.config.add('datalad.metadata.exclude-path', '.metadata',
                  where='dataset', reload=False)
    ds.config.add('datalad.metadata.nativetype', 'metalad_custom',
                  where='dataset')
    ds.rev_save()
    assert_repo_status(ds.path)

    # all on default
    ds.meta_aggregate()
    # all good, we get a report on this one dataset
    res = ds.meta_report(reporton='datasets')
    assert_result_count(res, 1)
    ucm = res[0]['metadata']['datalad_unique_content_properties']
    eq_(
        ucm['metalad_custom'],
        {
            # complex types do not get shmooshed together
            "complextype": [
                {
                    "age": "young",
                    "entity": {
                        "properties": "here",
                        "some": "many"
                    },
                    "numbers": [
                        3,
                        2,
                        1,
                        0
                    ]
                },
                {
                    "entity": {
                        "properties": "here",
                        "some": "few"
                    }
                }
            ],
            # simple type get unique'd as expected
            "something": [
                "stupid"
            ]
        })


# smoke test for https://github.com/datalad/datalad-revolution/issues/113
@with_tree({'subds': custom_metadata_tree})
def test_heterogenous_extractors(path):
    ds = Dataset(path).create(force=True)
    subds = ds.create('subds', force=True)
    # only the subds has 'custom' extractor enabled
    subds.config.add('datalad.metadata.exclude-path', '.metadata',
                     where='dataset', reload=False)
    subds.config.add('datalad.metadata.nativetype', 'metalad_custom',
                     where='dataset')
    ds.rev_save(recursive=True)
    assert_repo_status(ds.path)
    ds.meta_aggregate(recursive=True)
