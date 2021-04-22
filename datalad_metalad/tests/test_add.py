# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test metadata adding"""
import json
import os
import tempfile
from pathlib import Path
from typing import List
from unittest.mock import patch
from uuid import UUID

from datalad.api import meta_add, meta_dump
from datalad.support.exceptions import IncompleteResultsError
from datalad.tests.utils import (
    assert_is_not_none,
    assert_raises,
    assert_result_count,
    assert_true,
    eq_,
    skip_if_on_windows,
    with_tempfile
)

from dataladmetadatamodel.common import get_top_nodes_and_metadata_root_record
from dataladmetadatamodel.metadatapath import MetadataPath

from .utils import create_dataset
from ..exceptions import MetadataKeyException


default_id = UUID("00010203-1011-2021-3031-404142434445")
another_id = UUID("aa010203-1011-2021-3031-404142434445")

metadata_template = {
    "extractor_name": "ex_extractor_name",
    "extractor_version": "ex_extractor_version",
    "extraction_parameter": {"parameter1": "pvalue1"},
    "extraction_time": 1111666.3333,
    "agent_name": "test_name",
    "agent_email": "test email",
    "dataset_id": str(default_id),
    "dataset_version": "000000111111111112012121212121",
    "extracted_metadata": {"info": "some metadata"}
}


additional_keys_template = {
    "root_dataset_id": str(default_id),
    "root_dataset_version": "aaaaaaa0000000000000000222222222",
    "dataset_path": "sub_0/sub_0.0/dataset_0.0.0"
}


def _assert_raise_mke_with_keys(exception_keys: List[str],
                                *args,
                                **kwargs):

    try:
        meta_add(*args, **kwargs)
        raise RuntimeError("MetadataKeyException not raised")
    except MetadataKeyException as mke:
        eq_(mke.keys, exception_keys)


@with_tempfile
def test_unknown_key_reporting(file_name):

    json.dump({
            **metadata_template,
            "type": "dataset",
            "strange_key_name": "some value"
        },
        open(file_name, "tw"))

    with patch("datalad_metalad.add.check_dataset"):
        _assert_raise_mke_with_keys(
            ["strange_key_name"],
            metadata=file_name)


@with_tempfile
def test_unknown_key_allowed(file_name):

    json.dump({
            **metadata_template,
            "type": "dataset",
            "strange_key_name": "some value"
        },
        open(file_name, "tw"))

    with \
            patch("datalad_metalad.add.add_file_metadata") as fp, \
            patch("datalad_metalad.add.add_dataset_metadata") as dp, \
            tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        meta_add(
            metadata=file_name,
            dataset=git_repo.path,
            allow_unknown=True)

        assert_true(fp.call_count == 0)
        assert_true(dp.call_count == 1)


@with_tempfile
def test_optional_keys(file_name):

    json.dump({
            **metadata_template,
            "type": "file",
            "path": "d1/d1.1./f1.1.1"
        },
        open(file_name, "tw"))

    with \
            patch("datalad_metalad.add.add_file_metadata") as fp, \
            patch("datalad_metalad.add.add_dataset_metadata") as dp, \
            tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        meta_add(
            metadata=file_name,
            dataset=git_repo.path,
            allow_unknown=True)

        assert_true(fp.call_count == 1)
        assert_true(dp.call_count == 0)


@with_tempfile
def test_incomplete_non_mandatory_key_handling(file_name):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with patch("datalad_metalad.add.check_dataset"):
        _assert_raise_mke_with_keys(
            ["root_dataset_version", "dataset_path"],
            metadata=file_name,
            additionalvalues=json.dumps({"root_dataset_id": 1}))


@with_tempfile
def test_override_key_reporting(file_name):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with patch("datalad_metalad.add.check_dataset"):
        _assert_raise_mke_with_keys(
            ["dataset_id"],
            metadata=file_name,
            additionalvalues=json.dumps(
                {"dataset_id": "a2010203-1011-2021-3031-404142434445"}))


def test_object_parameter():
    with \
            patch("datalad_metalad.add.add_file_metadata") as fp, \
            patch("datalad_metalad.add.add_dataset_metadata") as dp, \
            tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        meta_add(
            metadata={
                **metadata_template,
                "type": "file",
                "path": "d1/d1.1./f1.1.1"
            },
            dataset=git_repo.path)

        assert_true(fp.call_count == 1)
        assert_true(dp.call_count == 0)


def test_additional_values_object_parameter():
    with \
            patch("datalad_metalad.add.add_file_metadata") as fp, \
            patch("datalad_metalad.add.add_dataset_metadata") as dp, \
            tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        meta_add(
            metadata={
                **metadata_template,
                "type": "file"
            },
            additionalvalues={
                "path": "d1/d1.1./f1.1.1"
            },
            dataset=git_repo.path)

        assert_true(fp.call_count == 1)
        assert_true(dp.call_count == 0)


@with_tempfile
def test_id_mismatch_detection(file_name):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with \
            patch("datalad_metalad.add.add_file_metadata") as fp, \
            patch("datalad_metalad.add.add_dataset_metadata") as dp, \
            tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        assert_raises(
            IncompleteResultsError,
            meta_add,
            metadata=file_name,
            additionalvalues=json.dumps(
                {"dataset_id": "a1010203-1011-2021-3031-404142434445"}),
            allow_override=True,
            dataset=git_repo.path)

        assert_true(fp.call_count == 0)
        assert_true(dp.call_count == 0)


@with_tempfile
def test_id_mismatch_allowed(file_name):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with \
            patch("datalad_metalad.add.add_file_metadata") as fp, \
            patch("datalad_metalad.add.add_dataset_metadata") as dp, \
            tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        meta_add(
            metadata=file_name,
            additionalvalues=json.dumps(
                {"dataset_id": "a1010203-1011-2021-3031-404142434445"}),
            dataset=git_repo.path,
            allow_override=True,
            allow_id_mismatch=True)

        assert_true(fp.call_count == 0)
        assert_true(dp.call_count == 1)


@with_tempfile
def test_root_id_mismatch_detection(file_name):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with \
            patch("datalad_metalad.add.add_file_metadata") as fp, \
            patch("datalad_metalad.add.add_dataset_metadata") as dp, \
            tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        assert_raises(
            IncompleteResultsError,
            meta_add,
            metadata=file_name,
            additionalvalues=json.dumps(
                {
                    "root_dataset_id": str(another_id),
                    "root_dataset_version": "000000000000000000000000000",
                    "dataset_path": "a/b/c"
                }),
            allow_override=True,
            dataset=git_repo.path)

        assert_true(fp.call_count == 0)
        assert_true(dp.call_count == 0)


@with_tempfile
def test_root_id_mismatch_allowed(file_name):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with \
            patch("datalad_metalad.add.add_file_metadata") as fp, \
            patch("datalad_metalad.add.add_dataset_metadata") as dp, \
            tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        meta_add(
            metadata=file_name,
            additionalvalues=json.dumps(
                {
                    "root_dataset_id": str(another_id),
                    "root_dataset_version": "000000000000000000000000000",
                    "dataset_path": "a/b/c"
                }),
            dataset=git_repo.path,
            allow_override=True,
            allow_id_mismatch=True)

        assert_true(fp.call_count == 0)
        assert_true(dp.call_count == 1)


@with_tempfile
def test_override_key_allowed(file_name):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with \
            patch("datalad_metalad.add.add_file_metadata") as fp, \
            patch("datalad_metalad.add.add_dataset_metadata") as dp, \
            tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        meta_add(
            metadata=file_name,
            additionalvalues=json.dumps(
                {"dataset_id": str(default_id)}),
            allow_override=True,
            dataset=git_repo.path)

        assert_true(fp.call_count == 0)
        assert_true(dp.call_count == 1)


def _get_top_nodes(git_repo, dataset_id, dataset_version):
    # Ensure that metadata was created
    tree_version_list, uuid_set, mrr = \
        get_top_nodes_and_metadata_root_record(
            "git",
            git_repo.path,
            dataset_id,
            dataset_version,
            MetadataPath(""))

    assert_is_not_none(tree_version_list)
    assert_is_not_none(uuid_set)
    assert_is_not_none(mrr)

    return tree_version_list, uuid_set, mrr


def _get_metadata_content(metadata):

    assert_is_not_none(metadata)
    metadata_instances = tuple(metadata.extractor_runs())
    assert_true(len(metadata_instances) == 1)

    extractor_name, extractor_runs = metadata_instances[0]
    eq_(extractor_name, metadata_template["extractor_name"])

    instances = tuple(extractor_runs.get_instances())
    assert_true(len(instances), 1)

    return instances[0].metadata_content


@with_tempfile
def test_add_dataset_end_to_end(file_name):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        res = meta_add(metadata=file_name, dataset=git_repo.path)
        assert_result_count(res, 1)
        assert_result_count(res, 1, type='dataset')
        assert_result_count(res, 0, type='file')

        # Verify dataset level metadata was added
        tree_version_list, uuid_set, mrr = _get_top_nodes(
            git_repo,
            UUID(metadata_template["dataset_id"]),
            metadata_template["dataset_version"])

        metadata = mrr.get_dataset_level_metadata()
        metadata_content = _get_metadata_content(metadata)
        eq_(metadata_content, metadata_template["extracted_metadata"])


@with_tempfile
def test_add_file_end_to_end(file_name):

    test_path = "d_0/d_0.0/f_0.0.0"

    json.dump({
        **metadata_template,
        "type": "file",
        "path": test_path
    }, open(file_name, "tw"))

    with tempfile.TemporaryDirectory() as temp_dir:
        git_repo = create_dataset(temp_dir, default_id)

        res = meta_add(metadata=file_name, dataset=git_repo.path)
        assert_result_count(res, 1)
        assert_result_count(res, 1, type='file')
        assert_result_count(res, 0, type='dataset')

        # Verify file level metadata was added
        tree_version_list, uuid_set, mrr = _get_top_nodes(
            git_repo,
            UUID(metadata_template["dataset_id"]),
            metadata_template["dataset_version"])

        file_tree = mrr.get_file_tree()
        assert_is_not_none(file_tree)
        assert_true(test_path in file_tree)

        metadata = file_tree.get_metadata(MetadataPath(test_path))
        metadata_content = _get_metadata_content(metadata)
        eq_(metadata_content, metadata_template["extracted_metadata"])


@with_tempfile
def test_subdataset_add_dataset_end_to_end(file_name):

    json.dump({
            **{
                **metadata_template,
                "dataset_id": str(another_id)
            },
            "type": "dataset",
            **additional_keys_template
        },
        open(file_name, "tw"))

    with tempfile.TemporaryDirectory() as temp_dir:
        git_repo = create_dataset(temp_dir, default_id)

        res = meta_add(metadata=file_name, dataset=git_repo.path)
        assert_result_count(res, 1)
        assert_result_count(res, 1, type='dataset')
        assert_result_count(res, 0, type='file')

        # Verify dataset level metadata was added
        root_dataset_id = UUID(additional_keys_template["root_dataset_id"])
        root_dataset_version = additional_keys_template["root_dataset_version"]
        dataset_tree_path = MetadataPath(
            additional_keys_template["dataset_path"])

        tree_version_list, uuid_set, mrr = _get_top_nodes(
            git_repo,
            root_dataset_id,
            root_dataset_version)

        _, dataset_tree = tree_version_list.get_dataset_tree(
            root_dataset_version)

        mrr = dataset_tree.get_metadata_root_record(dataset_tree_path)
        eq_(mrr.dataset_identifier, another_id)

        metadata = mrr.get_dataset_level_metadata()
        metadata_content = _get_metadata_content(metadata)
        eq_(metadata_content, metadata_template["extracted_metadata"])


@with_tempfile
def test_subdataset_add_file_end_to_end(file_name):

    test_path = "d_1/d_1.0/f_1.0.0"

    json.dump({
        **{
            **metadata_template,
            "dataset_id": str(another_id)
        },
        **additional_keys_template,
        "type": "file",
        "path": test_path
    }, open(file_name, "tw"))

    with tempfile.TemporaryDirectory() as temp_dir:
        git_repo = create_dataset(temp_dir, default_id)

        res = meta_add(metadata=file_name, dataset=git_repo.path)
        assert_result_count(res, 1)
        assert_result_count(res, 1, type='file')
        assert_result_count(res, 0, type='dataset')

        # Verify dataset level metadata was added
        root_dataset_id = UUID(additional_keys_template["root_dataset_id"])
        root_dataset_version = additional_keys_template["root_dataset_version"]
        dataset_tree_path = MetadataPath(
            additional_keys_template["dataset_path"])

        tree_version_list, uuid_set, mrr = _get_top_nodes(
            git_repo,
            root_dataset_id,
            root_dataset_version)

        _, dataset_tree = tree_version_list.get_dataset_tree(
            root_dataset_version)

        mrr = dataset_tree.get_metadata_root_record(dataset_tree_path)
        eq_(mrr.dataset_identifier, another_id)

        file_tree = mrr.get_file_tree()
        assert_is_not_none(file_tree)
        assert_true(test_path in file_tree)

        metadata = file_tree.get_metadata(MetadataPath(test_path))
        metadata_content = _get_metadata_content(metadata)
        eq_(metadata_content, metadata_template["extracted_metadata"])


@with_tempfile
def test_current_dir_add_end_to_end(file_name):

    json.dump({
            **{
                **metadata_template,
                "dataset_id": str(another_id)
            },
            "type": "dataset",
            **additional_keys_template
        },
        open(file_name, "tw"))

    with tempfile.TemporaryDirectory() as temp_dir:
        git_repo = create_dataset(temp_dir, default_id)

        execute_directory = Path.cwd()
        os.chdir(git_repo.pathobj)

        res = meta_add(metadata=file_name)
        assert_result_count(res, 1)
        assert_result_count(res, 1, type='dataset')
        assert_result_count(res, 0, type='file')

        os.chdir(execute_directory)

        results = tuple(meta_dump(dataset=git_repo.pathobj, recursive=True))

        assert_true(len(results), 1)
        result = results[0]["metadata"]["dataset_level_metadata"]
        eq_(result["root_dataset_identifier"], str(default_id))
        eq_(result["dataset_identifier"], str(another_id))

        metadata = result["metadata"]["ex_extractor_name"][0]
        translate = {
            "extraction_agent_name": "agent_name",
            "extraction_agent_email": "agent_email",
            "extraction_result": "extracted_metadata"
        }
        for key, value in metadata.items():
            eq_(value, metadata_template[translate.get(key, key)])
