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
import time
from pathlib import Path
from typing import (
    List,
    Union,
)
from unittest.mock import (
    call,
    patch,
)
from uuid import UUID

from datalad.api import (
    meta_add,
    meta_dump,
)
from datalad.cmd import BatchedCommand
from datalad.support.exceptions import IncompleteResultsError
from datalad.tests.utils_pytest import (
    assert_dict_equal,
    assert_equal,
    assert_in,
    assert_is_not_none,
    assert_not_equal,
    assert_raises,
    assert_result_count,
    assert_true,
    chpwd,
    eq_,
    known_failure_windows,
    with_tempfile,
)

from dataladmetadatamodel.common import get_top_nodes_and_metadata_root_record
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.mappableobject import ensure_mapped

import datalad_metalad.add
from .utils import (
    create_dataset,
    create_dataset_proper,
)
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


additional_keys_unknown_template = {
    "root_dataset_id": "<unknown>",
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
def test_unknown_key_reporting(file_name=None):

    json.dump({
            **metadata_template,
            "type": "dataset",
            "strange_key_name": "some value"
        },
        open(file_name, "tw"))

    with \
            patch("datalad_metalad.add.check_dataset"), \
            patch("datalad_metalad.add.locked_backend"):

        _assert_raise_mke_with_keys(
            ["strange_key_name"],
            metadata=file_name,
            result_renderer="disabled")


@with_tempfile
def test_unknown_key_allowed(file_name=None):

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
            allow_unknown=True,
            result_renderer="disabled")

        assert_true(fp.call_count == 0)
        assert_true(dp.call_count == 1)


@with_tempfile
def test_optional_keys(file_name=None):

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
            allow_unknown=True,
            result_renderer="disabled")

        assert_true(fp.call_count == 1)
        assert_true(dp.call_count == 0)


@with_tempfile
def test_incomplete_non_mandatory_key_handling(file_name=None):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with \
            patch("datalad_metalad.add.check_dataset"), \
            patch("datalad_metalad.add.locked_backend"):

        _assert_raise_mke_with_keys(
            ["root_dataset_version"],
            metadata=file_name,
            additionalvalues=json.dumps({"root_dataset_id": 1}),
            result_renderer="disabled")


@with_tempfile
def test_override_key_reporting(file_name=None):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with \
            patch("datalad_metalad.add.check_dataset"), \
            patch("datalad_metalad.add.locked_backend"):
        _assert_raise_mke_with_keys(
            ["dataset_id"],
            metadata=file_name,
            additionalvalues=json.dumps(
                {"dataset_id": "a2010203-1011-2021-3031-404142434445"}),
            result_renderer="disabled")


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
            dataset=git_repo.path,
            result_renderer="disabled")

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
            dataset=git_repo.path,
            result_renderer="disabled")

        assert_true(fp.call_count == 1)
        assert_true(dp.call_count == 0)


@with_tempfile
def test_id_mismatch_detection(file_name=None):
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
def test_id_mismatch_allowed(file_name=None):
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
            allow_id_mismatch=True,
            result_renderer="disabled")

        assert_true(fp.call_count == 0)
        assert_true(dp.call_count == 1)


@with_tempfile
def test_root_id_mismatch_detection(file_name=None):
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
def test_root_id_mismatch_allowed(file_name=None):
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
            allow_id_mismatch=True,
            result_renderer="disabled")

        assert_true(fp.call_count == 0)
        assert_true(dp.call_count == 1)


@with_tempfile
def test_override_key_allowed(file_name=None):
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
            dataset=git_repo.path,
            result_renderer="disabled")

        assert_true(fp.call_count == 0)
        assert_true(dp.call_count == 1)


def _get_top_nodes(git_repo,
                   dataset_id,
                   dataset_version,
                   dataset_tree_path=""):

    # Ensure that metadata was created
    tree_version_list, uuid_set, mrr = \
        get_top_nodes_and_metadata_root_record(
            mapper_family="git",
            realm=git_repo.path,
            dataset_id=dataset_id,
            primary_data_version=dataset_version,
            prefix_path=MetadataPath(""),
            dataset_tree_path=MetadataPath(dataset_tree_path),
            sub_dataset_id=None,
            sub_dataset_version=None)

    assert_is_not_none(tree_version_list)
    assert_is_not_none(uuid_set)
    assert_is_not_none(mrr)

    return tree_version_list, uuid_set, mrr


def _get_metadata_content(metadata):

    assert_is_not_none(metadata)

    with ensure_mapped(metadata):
        metadata_instances = tuple(metadata.extractor_runs)
        assert_true(len(metadata_instances) == 1)

        extractor_name, extractor_runs = metadata_instances[0]
        eq_(extractor_name, metadata_template["extractor_name"])

        instances = extractor_runs.instances
        assert_true(len(instances), 1)

        return instances[0].metadata_content


@with_tempfile
def test_add_dataset_end_to_end(file_name=None):
    json.dump({
            **metadata_template,
            "type": "dataset"
        },
        open(file_name, "tw"))

    with tempfile.TemporaryDirectory() as temp_dir:

        git_repo = create_dataset(temp_dir, default_id)

        res = meta_add(
            metadata=file_name,
            dataset=git_repo.path,
            result_renderer="disabled")
        assert_result_count(res, 1)
        assert_result_count(res, 1, type='dataset')
        assert_result_count(res, 0, type='file')

        # Verify dataset level metadata was added
        tree_version_list, _, mrr = _get_top_nodes(
            git_repo,
            UUID(metadata_template["dataset_id"]),
            metadata_template["dataset_version"])

        metadata = mrr.get_dataset_level_metadata()
        metadata_content = _get_metadata_content(metadata)
        eq_(metadata_content, metadata_template["extracted_metadata"])


@with_tempfile
def test_add_file_end_to_end(file_name=None):

    test_path = "d_0/d_0.0/f_0.0.0"

    json.dump({
        **metadata_template,
        "type": "file",
        "path": test_path
    }, open(file_name, "tw"))

    with tempfile.TemporaryDirectory() as temp_dir:
        git_repo = create_dataset(temp_dir, default_id)

        res = meta_add(
            metadata=file_name,
            dataset=git_repo.path,
            result_renderer="disabled")
        assert_result_count(res, 1)
        assert_result_count(res, 1, type='file')
        assert_result_count(res, 0, type='dataset')

        # Verify file level metadata was added
        _, _, mrr = _get_top_nodes(
            git_repo,
            UUID(metadata_template["dataset_id"]),
            metadata_template["dataset_version"])

        file_tree = mrr.get_file_tree()
        assert_is_not_none(file_tree)
        assert_true(MetadataPath(test_path) in file_tree)

        metadata = file_tree.get_metadata(MetadataPath(test_path))
        metadata_content = _get_metadata_content(metadata)
        eq_(metadata_content, metadata_template["extracted_metadata"])


@with_tempfile
def test_subdataset_add_dataset_end_to_end(file_name=None):

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

        res = meta_add(
            metadata=file_name,
            dataset=git_repo.path,
            result_renderer="disabled")
        assert_result_count(res, 1)
        assert_result_count(res, 1, type='dataset')
        assert_result_count(res, 0, type='file')

        # Verify dataset level metadata was added
        root_dataset_id = UUID(additional_keys_template["root_dataset_id"])
        root_dataset_version = additional_keys_template["root_dataset_version"]
        dataset_tree_path = MetadataPath(
            additional_keys_template["dataset_path"])

        tree_version_list, _, mrr = _get_top_nodes(
            git_repo,
            root_dataset_id,
            root_dataset_version,
            additional_keys_template["dataset_path"])

        _, _, dataset_tree = tree_version_list.get_dataset_tree(
            root_dataset_version,
            MetadataPath(""))

        mrr = dataset_tree.get_metadata_root_record(dataset_tree_path)
        with ensure_mapped(mrr):
            eq_(mrr.dataset_identifier, another_id)

            metadata = mrr.get_dataset_level_metadata()
            metadata_content = _get_metadata_content(metadata)
            eq_(metadata_content, metadata_template["extracted_metadata"])


@with_tempfile
def test_subdataset_add_file_end_to_end(file_name=None):

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

        res = meta_add(
            metadata=file_name,
            dataset=git_repo.path,
            result_renderer="disabled")
        assert_result_count(res, 1)
        assert_result_count(res, 1, type='file')
        assert_result_count(res, 0, type='dataset')

        # Verify dataset level metadata was added
        root_dataset_id = UUID(additional_keys_template["root_dataset_id"])
        root_dataset_version = additional_keys_template["root_dataset_version"]
        dataset_tree_path = MetadataPath(
            additional_keys_template["dataset_path"])

        tree_version_list, _, mrr = _get_top_nodes(
            git_repo,
            root_dataset_id,
            root_dataset_version,
            additional_keys_template["dataset_path"])

        _, _, dataset_tree = tree_version_list.get_dataset_tree(
            root_dataset_version,
            MetadataPath(""))

        mrr = dataset_tree.get_metadata_root_record(dataset_tree_path)
        with ensure_mapped(mrr):
            eq_(mrr.dataset_identifier, another_id)

            file_tree = mrr.get_file_tree()
            assert_is_not_none(file_tree)
            assert_true(MetadataPath(test_path) in file_tree)

            metadata = file_tree.get_metadata(MetadataPath(test_path))
            metadata_content = _get_metadata_content(metadata)
            eq_(metadata_content, metadata_template["extracted_metadata"])


@with_tempfile
def test_current_dir_add_end_to_end(file_name=None):

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

        with chpwd(git_repo.pathobj):
            res = meta_add(metadata=file_name, result_renderer="disabled")
            assert_result_count(res, 1)
            assert_result_count(res, 1, type='dataset')
            assert_result_count(res, 0, type='file')

        results = tuple(meta_dump(dataset=git_repo.pathobj,
                                  recursive=True,
                                  result_renderer="disabled"))

        assert_true(len(results), 1)
        result = results[0]["metadata"]

        expected = {
            **metadata_template,
            **additional_keys_unknown_template,
            "type": "dataset",
            "dataset_id": str(another_id),
        }

        # Check extraction parameter result
        ep_key = "extraction_parameter"
        assert_dict_equal(expected[ep_key], result[ep_key])
        del expected[ep_key]
        del result[ep_key]

        # Check remaining result
        assert_dict_equal(result, expected)


@with_tempfile
def test_add_file_dump_end_to_end(file_name=None):

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

        import time

        start_time = time.time()
        res = meta_add(
            metadata=[],
            dataset=git_repo.path,
            result_renderer="disabled")

        start_time = time.time()
        res = meta_add(
            metadata=file_name,
            dataset=git_repo.path,
            result_renderer="disabled")

        assert_result_count(res, 1)
        assert_result_count(res, 1, type='file')
        assert_result_count(res, 0, type='dataset')

        results = tuple(meta_dump(dataset=git_repo.pathobj,
                                  recursive=True,
                                  result_renderer="disabled"))
        assert_true(len(results), 1)
        result = results[0]["metadata"]

        expected = {
            **metadata_template,
            **additional_keys_unknown_template,
            "type": "file",
            "path": test_path,
            "dataset_id": str(another_id)
        }

        # Check extraction parameter result
        ep_key = "extraction_parameter"
        assert_dict_equal(expected[ep_key], result[ep_key])
        del expected[ep_key]
        del result[ep_key]

        # Check remaining result
        assert_dict_equal(result, expected)


def _create_json_metadata_records(file_count: int,
                                  metadata_count: int) -> List:

    test_path = "d_1/d_1.0"

    return [
            {
                **{
                    **{
                        **metadata_template,
                        "extraction_parameter": {
                            "parameter1": f"pvalue{metadata_index}"
                        }
                    },
                    "dataset_id": str(another_id)
                },
                **additional_keys_template,
                "type": "file",
                "path": test_path + f"/f_1.0.{file_index}"
            }
            for file_index in range(file_count)
            for metadata_index in range(metadata_count)
    ]


def check_multi_adding(metadata: Union[str, List],
                       file_count: int,
                       metadata_count: int,
                       batch_mode: bool = False):
    with tempfile.TemporaryDirectory() as temp_dir:
        git_repo = create_dataset(temp_dir, default_id)

        import time
        start_time = time.time()
        if batch_mode:
            with \
                    patch("datalad_metalad.add._stdin_reader") as stdin_mock, \
                    patch("datalad_metalad.add.sys") as sys_mock:

                stdin_mock.return_value = iter(metadata)
                meta_add(
                    metadata="-",
                    dataset=git_repo.path,
                    batch_mode=True,
                    result_renderer="disabled")
                assert_in(
                    call.stdout.write(
                        f'{{"status": "ok", "succeeded": '
                        f'{file_count * metadata_count}, "failed": 0}}\n'),
                    sys_mock.mock_calls)
        else:
            res = meta_add(
                metadata=metadata,
                dataset=git_repo.path,
                result_renderer="disabled")
            assert_result_count(res, file_count * metadata_count)

        print(
            f"meta-add ({file_count} files, {metadata_count} records, "
            f"batched: {batch_mode}): {time.time() - start_time}s")

        if file_count * metadata_count == 0:
            return

        results = tuple(meta_dump(dataset=git_repo.pathobj,
                                  recursive=True,
                                  result_renderer="disabled"))
        assert_true(len(results), file_count * metadata_count)


def _check_file_multiple_end_to_end_test(file_count: int,
                                         metadata_count: int,
                                         file_name: str):
    json.dump(
        _create_json_metadata_records(
            file_count=file_count,
            metadata_count=metadata_count),
        open(file_name, "tw")
    )

    check_multi_adding(file_name, file_count, metadata_count)


def _check_memory_multiple_end_to_end_test(file_count: int,
                                           metadata_count: int,
                                           batch_mode: bool = False):

    json_objects = _create_json_metadata_records(
        file_count=file_count,
        metadata_count=metadata_count
    )
    check_multi_adding(json_objects, file_count, metadata_count, batch_mode)


def test_really_large_end_to_end():
    _check_memory_multiple_end_to_end_test(1000, 1)


def _perform_test_multiple_file_records_end_to_end(batch_mode: bool):
    _check_memory_multiple_end_to_end_test(0, 0, batch_mode)
    _check_memory_multiple_end_to_end_test(1, 1, batch_mode)
    _check_memory_multiple_end_to_end_test(31, 31, batch_mode)
    _check_memory_multiple_end_to_end_test(1, 1000, batch_mode)
    _check_memory_multiple_end_to_end_test(1000, 1, batch_mode)
    _check_memory_multiple_end_to_end_test(100, 100, batch_mode)


def test_add_multiple_file_records_end_to_end():
    for batch_mode in (True, False):
        _perform_test_multiple_file_records_end_to_end(batch_mode)


@with_tempfile
def test_add_multiple_metadata_records_end_to_end(file_name=None):
    _check_file_multiple_end_to_end_test(1, 1000, file_name)


@with_tempfile(mkdir=True)
def test_cache_age(temp_dir=None):
    create_dataset_proper(temp_dir)

    def slow_feed():
        json_objects = _create_json_metadata_records(file_count=3, metadata_count=3)
        for json_object in json_objects:
            yield json_object
            time.sleep(.5)

    # Ensure that maximum cache age is three
    datalad_metalad.add.max_cache_age = 2
    with \
            patch("datalad_metalad.add.flush_metadata_cache") as fc, \
            patch("datalad_metalad.add._stdin_reader") as stdin_mock:

        stdin_mock.return_value = slow_feed()
        fc.return_value = (4, 4)
        meta_add(
            metadata="-",
            dataset=temp_dir,
            allow_id_mismatch=True,
            batch_mode=True,
            result_renderer="disabled")

        assert_true(fc.call_count >= 2)


@with_tempfile(mkdir=True)
def test_batch_mode(temp_dir=None):
    create_dataset_proper(temp_dir)

    json_objects = _create_json_metadata_records(file_count=3, metadata_count=3)

    with patch("datalad_metalad.add._stdin_reader") as stdin_mock, \
         patch("datalad_metalad.add.sys") as sys_mock:

        stdin_mock.return_value = iter(json_objects)
        meta_add(
            metadata="-",
            dataset=temp_dir,
            allow_id_mismatch=True,
            batch_mode=True,
            result_renderer="disabled")

        assert_in(
            call.stdout.write(
                f'{{"status": "ok", "succeeded": {len(json_objects)}, '
                f'"failed": 0}}\n'),
            sys_mock.mock_calls)


@with_tempfile(mkdir=True)
def test_batch_mode_end_to_end(temp_dir=None):
    create_dataset_proper(temp_dir)

    json_objects = _create_json_metadata_records(file_count=3, metadata_count=3)
    bc = BatchedCommand(
        ["datalad", "meta-add", "-d", temp_dir, "--batch-mode", "-i", "-"])

    for json_object in json_objects:
        result = bc(json.dumps(json_object))
        result_object = json.loads(result)
        eq_(result_object["status"], "ok")
        eq_(result_object["action"], "meta_add")
        eq_(result_object["cached"], True)

    eq_(bc(""),
        f'{{"status": "ok", "succeeded": {len(json_objects)}, "failed": 0}}')
    bc.close()


unknown_error_lines_json = \
"""{"type": "dataset", "dataset_path": "study-95", "dataset_id": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "dataset_version": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "extraction_time": 1647789778.6307757, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_example_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 4}}
{"type": "dataset", "dataset_path": "study-95", "dataset_id": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "dataset_version": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "extraction_time": 1648565892.8312778, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 5}}
{"type": "dataset", "dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "extraction_time": 1648477352.674133, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_example_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 6}}
{"type": "dataset", "dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "extraction_time": 1648565711.668348, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 7}}
{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1647851151.4307828, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_studyminimeta", "extractor_version": "0.1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 1}}
{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1648477354.3841684, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_example_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 2}}
{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1648565713.58595, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 3}}
"""


critical_lines_json = \
"""{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1647851151.4307828, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_studyminimeta", "extractor_version": "0.1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 1}}
{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1648477354.3841684, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_example_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 2}}
{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1648565713.58595, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 3}}
{"type": "dataset", "dataset_path": "study-95", "dataset_id": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "dataset_version": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "extraction_time": 1647789778.6307757, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_example_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 4}}
{"type": "dataset", "dataset_path": "study-95", "dataset_id": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "dataset_version": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "extraction_time": 1648565892.8312778, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 5}}
{"type": "dataset", "dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "extraction_time": 1648477352.674133, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_example_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 6}}
{"type": "dataset", "dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "extraction_time": 1648565711.668348, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"test-data": 7}}
"""


@with_tempfile(mkdir=True)
def test_add_regression_1(temp_dir=None):
    create_dataset_proper(temp_dir)

    json_objects = [
        json.loads(json_string)
        for json_string in critical_lines_json.splitlines()
    ]
    for json_object in json_objects:
        for result in meta_add(dataset=temp_dir,
                               metadata=json_object,
                               allow_id_mismatch=True,
                               result_renderer="disabled"):
            eq_(result["status"], "ok")

    results = list(meta_dump(dataset=temp_dir, path="", recursive=True))
    assert_equal(len(results), len(json_objects))
    for result in results:
        eq_(result["status"], "ok")
        root_dataset_id = result["metadata"].get("root_dataset_id", None)
        assert_not_equal(root_dataset_id, "<unknown>")


@with_tempfile(mkdir=True)
def test_multi_add_regression_1(temp_dir=None):
    create_dataset_proper(temp_dir)

    json_objects = [
        json.loads(json_string)
        for json_string in critical_lines_json.splitlines()
    ]
    for result in meta_add(dataset=temp_dir,
                           metadata=json_objects,
                           allow_id_mismatch=True,
                           result_renderer="disabled"):
        eq_(result["status"], "ok")

    results = list(meta_dump(dataset=temp_dir, path="*", recursive=True))
    for result in results:
        assert_in("metadata", result)
    assert_equal(len(results), len(json_objects))
    for result in results:
        eq_(result["status"], "ok")
        root_dataset_id = result["metadata"].get("root_dataset_id", None)
        assert_not_equal(root_dataset_id, "<unknown>")


@known_failure_windows
@with_tempfile(mkdir=True)
def test_multi_add_regression_2(temp_dir=None):
    create_dataset_proper(temp_dir)

    with tempfile.NamedTemporaryFile(mode="tw") as json_input:
        json_input.write(critical_lines_json)
        json_input.flush()

        for result in meta_add(dataset=temp_dir,
                               metadata=json_input.name,
                               json_lines=True,
                               allow_id_mismatch=True,
                               result_renderer="disabled"):
            eq_(result["status"], "ok")

        results = list(meta_dump(dataset=temp_dir, path="", recursive=True))
        assert_equal(len(results), len(critical_lines_json.splitlines()))
        for result in results:
            eq_(result["status"], "ok")
            root_dataset_id = result["metadata"].get("root_dataset_id", None)
            assert_not_equal(root_dataset_id, "<unknown>")
