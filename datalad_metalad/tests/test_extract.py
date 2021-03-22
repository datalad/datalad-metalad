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
from uuid import UUID

from datalad.distribution.dataset import Dataset
from datalad.api import meta_extract
from datalad.utils import chpwd

from datalad.tests.utils import (
    with_tempfile,
    with_tree,
    assert_is_not_none,
    assert_repo_status,
    assert_raises,
    assert_result_count,
    assert_in,
    assert_true,
    eq_
)

from dataladmetadatamodel.common import get_top_nodes_and_metadata_root_record
from dataladmetadatamodel.metadatapath import MetadataPath


meta_tree = {
    'sub': {
        'one': '1',
        'nothing': '2',
    },
}


@with_tempfile(mkdir=True)
def test_empty_dataset_error(path):
    # go into virgin dir to avoid detection of any dataset
    with chpwd(path):
        assert_raises(
            ValueError,
            meta_extract, extractorname="metalad_core")


@with_tempfile(mkdir=True)
def test_unknown_extractor_error(path):
    # ensure failure on unavailable metadata extractor
    with chpwd(path):
        assert_raises(
            ValueError,
            meta_extract, extractorname="bogus__")


@with_tree(meta_tree)
def test_dataset_extraction_end_to_end(path):
    ds = Dataset(path).create(force=True)
    ds.config.add(
        'datalad.metadata.exclude-path',
        '.metadata',
        where='dataset')
    ds.save()
    assert_repo_status(ds.path)

    # by default we get core and annex reports
    res = meta_extract(
        extractorname="metalad_core_dataset",
        dataset=ds)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='dataset')
    assert_result_count(res, 0, type='file')

    # Ensure that metadata was created
    tree_version_list, uuid_set, mrr = get_top_nodes_and_metadata_root_record(
        "git",
        path,
        UUID(ds.id),
        ds.repo.get_hexsha(),
        MetadataPath(""))

    assert_is_not_none(tree_version_list)
    assert_is_not_none(uuid_set)
    assert_is_not_none(mrr)

    # Check metadata
    metadata = mrr.get_dataset_level_metadata()
    assert_is_not_none(metadata)

    metadata_instances = tuple(metadata.extractor_runs())
    assert_true(len(metadata_instances) == 1)

    extractor_name, extractor_runs = metadata_instances[0]
    eq_(extractor_name, "metalad_core_dataset")

    instances = tuple(extractor_runs.get_instances())
    assert_true(len(instances), 1)
    immediate_metadata = instances[0].metadata_source.content
    
    assert_in("id", immediate_metadata)
    assert_in("refcommit", immediate_metadata)
    assert_in("path", immediate_metadata)
    assert_in("comment", immediate_metadata)

    eq_(immediate_metadata["id"], ds.id)
    eq_(immediate_metadata["refcommit"], ds.repo.get_hexsha())
    eq_(immediate_metadata["path"], ds.path)
    eq_(immediate_metadata["comment"], "test-implementation")


@with_tree(meta_tree)
def test_file_extraction_end_to_end(path):
    ds = Dataset(path).create(force=True)
    ds.config.add(
        'datalad.metadata.exclude-path',
        '.metadata',
        where='dataset')
    ds.save()
    assert_repo_status(ds.path)

    # by default we get core and annex reports
    res = meta_extract(
        extractorname="metalad_core_file",
        path="sub/one",
        dataset=ds)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='file')
    assert_result_count(res, 0, type='dataset')

    # Ensure that metadata was created
    tree_version_list, uuid_set, mrr = get_top_nodes_and_metadata_root_record(
        "git",
        path,
        UUID(ds.id),
        ds.repo.get_hexsha(),
        MetadataPath(""))

    assert_is_not_none(tree_version_list)
    assert_is_not_none(uuid_set)
    assert_is_not_none(mrr)

    # Check file level metadata
    file_tree = mrr.get_file_tree()
    assert_is_not_none(file_tree)

    assert_true("sub/one" in file_tree)
    metadata = file_tree.get_metadata(MetadataPath("sub/one"))
    metadata_instances = tuple(metadata.extractor_runs())
    assert_true(len(metadata_instances) == 1)

    extractor_name, extractor_runs = metadata_instances[0]
    eq_(extractor_name, "metalad_core_file")

    instances = tuple(extractor_runs.get_instances())
    assert_true(len(instances), 1)
    immediate_metadata = instances[0].metadata_source.content

    assert_in("@id", immediate_metadata)
    assert_in("path", immediate_metadata)
    assert_in("intra_dataset_path", immediate_metadata)
    assert_in("content_byte_size", immediate_metadata)
    assert_in("comment", immediate_metadata)

    eq_(immediate_metadata["path"], str(ds.pathobj / "sub" / "one"))
    eq_(
        MetadataPath(immediate_metadata["intra_dataset_path"]),
        MetadataPath("sub/one"))

    eq_(immediate_metadata["comment"], "test-implementation")
