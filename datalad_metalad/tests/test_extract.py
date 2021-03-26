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
from typing import Optional

from datalad.distribution.dataset import Dataset
from datalad.api import meta_extract
from datalad.utils import chpwd

from datalad.tests.utils import (
    assert_repo_status,
    assert_raises,
    assert_result_count,
    assert_in,
    eq_,
    known_failure,
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


def _check_metadata_record(metadata_record: dict,
                           dataset: Dataset,
                           extractor_name: str,
                           extractor_version: str,
                           extraction_parameter: dict,
                           path: Optional[str] = None):

    assert_in("extraction_time", metadata_record)
    eq_(metadata_record["dataset_id"], UUID(dataset.id))
    eq_(metadata_record["dataset_version"], dataset.repo.get_hexsha())
    eq_(metadata_record["extractor_version"], extractor_version)
    eq_(metadata_record["extractor_name"], extractor_name)
    eq_(metadata_record["extraction_parameter"], extraction_parameter)
    eq_(metadata_record["agent_name"], "DataLad Tester")
    eq_(metadata_record["agent_email"], "test@example.com")
    if path is not None:
        eq_(metadata_record["intra_dataset_path"], MetadataPath(path))


@with_tree(meta_tree)
def test_dataset_extraction_result(path):

    ds = Dataset(path).create(force=True)
    ds.config.add(
        'datalad.metadata.exclude-path',
        '.metadata',
        where='dataset')
    ds.save()
    assert_repo_status(ds.path)

    extractor_name = "metalad_core_dataset"
    extractor_class = get_extractor_class(extractor_name)
    extractor_version = extractor_class(None, None, None).get_version()

    res = meta_extract(
        extractorname=extractor_name,
        dataset=ds)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='dataset')
    assert_result_count(res, 0, type='file')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={})

    extracted_metadata = metadata_record["extracted_metadata"]
    eq_(extracted_metadata["id"], ds.id)
    eq_(extracted_metadata["refcommit"], ds.repo.get_hexsha())
    eq_(extracted_metadata["path"], ds.path)
    eq_(extracted_metadata["comment"], "test-implementation")


@with_tree(meta_tree)
def test_file_extraction_result(ds_path):

    ds = Dataset(ds_path).create(force=True)
    ds.config.add(
        'datalad.metadata.exclude-path',
        '.metadata',
        where='dataset')
    ds.save()
    assert_repo_status(ds.path)

    file_path = "sub/one"
    extractor_name = "metalad_core_file"
    extractor_class = get_extractor_class(extractor_name)
    extractor_version = extractor_class(None, None, None).get_version()

    res = meta_extract(
        extractorname=extractor_name,
        path=file_path,
        dataset=ds)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='file')
    assert_result_count(res, 0, type='dataset')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={},
        path=file_path)

    extracted_metadata = metadata_record["extracted_metadata"]
    assert_in("@id", extracted_metadata)
    eq_(extracted_metadata["type"], "file")
    eq_(extracted_metadata["path"], str(ds.pathobj / file_path))
    eq_(extracted_metadata["intra_dataset_path"], file_path)
    eq_(extracted_metadata["content_byte_size"], 111)
    eq_(extracted_metadata["comment"], "test-implementation")


@known_failure
@with_tree(meta_tree)
def test_legacy1_dataset_extraction_result(ds_path):

    ds = Dataset(ds_path).create(force=True)
    ds.config.add(
        'datalad.metadata.exclude-path',
        '.metadata',
        where='dataset')
    ds.save()
    assert_repo_status(ds.path)

    extractor_name = "metalad_core"
    extractor_version = "1"

    res = meta_extract(
        extractorname=extractor_name,
        dataset=ds)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='dataset')
    assert_result_count(res, 0, type='file')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={})

    extracted_metadata = metadata_record["extracted_metadata"]
    assert_in("@id", extracted_metadata)
    eq_(extracted_metadata["contentbytesize"], 1)


@with_tree(meta_tree)
def test_legacy2_dataset_extraction_result(ds_path):

    ds = Dataset(ds_path).create(force=True)
    ds.config.add(
        'datalad.metadata.exclude-path',
        '.metadata',
        where='dataset')
    ds.save()
    assert_repo_status(ds.path)

    extractor_name = "datalad_core"
    extractor_version = "un-versioned"

    res = meta_extract(
        extractorname=extractor_name,
        dataset=ds)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='dataset')
    assert_result_count(res, 0, type='file')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={})

    extracted_metadata = metadata_record["extracted_metadata"]
    assert_in("@id", extracted_metadata)


@with_tree(meta_tree)
def test_legacy1_file_extraction_result(ds_path):

    ds = Dataset(ds_path).create(force=True)
    ds.config.add(
        'datalad.metadata.exclude-path',
        '.metadata',
        where='dataset')
    ds.save()
    assert_repo_status(ds.path)

    file_path = "sub/one"
    extractor_name = "metalad_core"
    extractor_version = "1"

    res = meta_extract(
        extractorname=extractor_name,
        path=file_path,
        dataset=ds)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='file')
    assert_result_count(res, 0, type='dataset')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={},
        path=file_path)

    extracted_metadata = metadata_record["extracted_metadata"]
    assert_in("@id", extracted_metadata)
    eq_(extracted_metadata["contentbytesize"], 1)


@with_tree(meta_tree)
def test_legacy2_file_extraction_result(ds_path):

    ds = Dataset(ds_path).create(force=True)
    ds.config.add(
        'datalad.metadata.exclude-path',
        '.metadata',
        where='dataset')
    ds.save()
    assert_repo_status(ds.path)

    file_path = "sub/one"
    extractor_name = "datalad_core"
    extractor_version = "un-versioned"

    res = meta_extract(
        extractorname=extractor_name,
        path=file_path,
        dataset=ds)

    assert_result_count(res, 1)
    assert_result_count(res, 1, type='file')
    assert_result_count(res, 0, type='dataset')

    metadata_record = res[0]["metadata_record"]
    _check_metadata_record(
        metadata_record=metadata_record,
        dataset=ds,
        extractor_name=extractor_name,
        extractor_version=extractor_version,
        extraction_parameter={},
        path=file_path)

    extracted_metadata = metadata_record["extracted_metadata"]
    eq_(extracted_metadata, {})
