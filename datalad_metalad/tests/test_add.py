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
import sys
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
from datalad.tests.utils import (
    assert_dict_equal,
    assert_equal,
    assert_in,
    assert_is_not_none,
    assert_not_equal,
    assert_raises,
    assert_result_count,
    assert_true,
    eq_,
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

    with \
            patch("datalad_metalad.add.check_dataset"), \
            patch("datalad_metalad.add.locked_backend"):

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

    with \
            patch("datalad_metalad.add.check_dataset"), \
            patch("datalad_metalad.add.locked_backend"):

        _assert_raise_mke_with_keys(
            ["root_dataset_version"],
            metadata=file_name,
            additionalvalues=json.dumps({"root_dataset_id": 1}))


@with_tempfile
def test_override_key_reporting(file_name):
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
            prefix_path=MetadataPath(""),
            dataset_tree_path=MetadataPath(""))

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
        tree_version_list, _, mrr = _get_top_nodes(
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

        tree_version_list, _, mrr = _get_top_nodes(
            git_repo,
            root_dataset_id,
            root_dataset_version)

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

        tree_version_list, _, mrr = _get_top_nodes(
            git_repo,
            root_dataset_id,
            root_dataset_version)

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

        results = tuple(meta_dump(dataset=git_repo.pathobj,
                                  recursive=True,
                                  result_renderer="disabled"))

        assert_true(len(results), 1)
        result = results[0]["metadata"]

        expected = {
            **metadata_template,
            **additional_keys_template,
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
def test_add_file_dump_end_to_end(file_name):

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
        res = meta_add(metadata=[], dataset=git_repo.path)
        print(f"meta-add x 0: {time.time() - start_time} s")

        start_time = time.time()
        res = meta_add(metadata=file_name, dataset=git_repo.path)
        print(f"meta-add x 1: {time.time() - start_time} s")

        res = meta_add(metadata=file_name, dataset=git_repo.path)
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
            **additional_keys_template,
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
                meta_add(metadata="-", dataset=git_repo.path, batch_mode=True)
                assert_in(
                    call.stdout.write(
                        f'{{"status": "ok", "succeeded": '
                        f'{file_count * metadata_count}, "failed": 0}}\n'),
                    sys_mock.mock_calls)
        else:
            res = meta_add(metadata=metadata, dataset=git_repo.path)
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
def test_add_multiple_metadata_records_end_to_end(file_name: str):
    _check_file_multiple_end_to_end_test(1, 1000, file_name)


@with_tempfile(mkdir=True)
def test_cache_age(temp_dir: str):
    create_dataset_proper(temp_dir)

    def slow_feed():
        json_objects = _create_json_metadata_records(file_count=3, metadata_count=3)
        for json_object in json_objects:
            yield json_object
            time.sleep(.5)

    # Ensure that maximum cache age is three
    datalad_metalad.add.max_cache_age = 2
    with \
            patch("datalad_metalad.add.flush_cache") as fc, \
            patch("datalad_metalad.add._stdin_reader") as stdin_mock:

        stdin_mock.return_value = slow_feed()
        fc.return_value = (4, 4)
        meta_add(
            metadata="-",
            dataset=temp_dir,
            allow_id_mismatch=True,
            batch_mode=True)

        assert_true(fc.call_count >= 2)


@with_tempfile(mkdir=True)
def test_batch_mode(temp_dir: str):
    create_dataset_proper(temp_dir)

    json_objects = _create_json_metadata_records(file_count=3, metadata_count=3)

    with patch("datalad_metalad.add._stdin_reader") as stdin_mock, \
         patch("datalad_metalad.add.sys") as sys_mock:

        stdin_mock.return_value = iter(json_objects)
        meta_add(
            metadata="-",
            dataset=temp_dir,
            allow_id_mismatch=True,
            batch_mode=True)

        assert_in(
            call.stdout.write(
                f'{{"status": "ok", "succeeded": {len(json_objects)}, '
                f'"failed": 0}}\n'),
            sys_mock.mock_calls)


@with_tempfile(mkdir=True)
def test_batch_mode_end_to_end(temp_dir: str):
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
"""{"type": "dataset", "dataset_path": "study-95", "dataset_id": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "dataset_version": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "extraction_time": 1647789778.6307757, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"id": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "refcommit": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "comment": "test-implementation of core_dataset"}}
{"type": "dataset", "dataset_path": "study-95", "dataset_id": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "dataset_version": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "extraction_time": 1648565892.8312778, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/", "datalad": "http://dx.datalad.org/"}, "@graph": [{"@id": "59286713dacabfbce1cecf4c865fff5a", "@type": "agent", "name": "Christian M\u00f6nch", "email": "christian.moench@web.de"}, {"@id": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "identifier": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "@type": "Dataset", "version": "0-9-g73ad003", "dateCreated": "2020-08-12T10:32:19+02:00", "dateModified": "2020-09-18T16:28:56+02:00", "hasContributor": {"@id": "59286713dacabfbce1cecf4c865fff5a"}, "distribution": [{"name": "origin", "@id": "datalad:1513095d-1fdc-45ca-8e86-d71d26d7b4af"}]}]}}
{"type": "dataset", "dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "extraction_time": 1648477352.674133, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "refcommit": "0281546196aaddfa88ea1b5f40396a5960ed7040", "comment": "test-implementation of core_dataset"}}
{"type": "dataset", "dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "extraction_time": 1648565711.668348, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/", "datalad": "http://dx.datalad.org/"}, "@graph": [{"@id": "59286713dacabfbce1cecf4c865fff5a", "@type": "agent", "name": "Christian M\u00f6nch", "email": "christian.moench@web.de"}, {"@id": "0281546196aaddfa88ea1b5f40396a5960ed7040", "identifier": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "@type": "Dataset", "version": "0-72-g0281546", "dateCreated": "2020-08-12T13:03:58+02:00", "dateModified": "2020-09-29T13:46:46+02:00", "hasContributor": {"@id": "59286713dacabfbce1cecf4c865fff5a"}, "hasPart": [{"@id": "datalad:2c332c81f32ebe4d6bb3e59e3d895581648b7030", "@type": "Dataset", "name": "study-100", "identifier": "datalad:52142b84-dc76-11ea-98c5-7cdd908c7490"}, {"@id": "datalad:443567ac95675242b2cde9615c0cd2830104c309", "@type": "Dataset", "name": "study-101", "identifier": "datalad:5146e8d6-dc76-11ea-a2da-7cdd908c7490"}, {"@id": "datalad:934de4bb707a35b3b940536760a9f8d095e0430e", "@type": "Dataset", "name": "study-102", "identifier": "datalad:506ce096-dc76-11ea-95f0-7cdd908c7490"}, {"@id": "datalad:f222e7c8828c7345101873252efa0c56db3eec84", "@type": "Dataset", "name": "study-103", "identifier": "datalad:4fb7be64-dc76-11ea-ad07-7cdd908c7490"}, {"@id": "datalad:617610cc3db409714bd9506dda88c471b1bedd6d", "@type": "Dataset", "name": "study-104", "identifier": "datalad:4f03a794-dc76-11ea-994b-7cdd908c7490"}, {"@id": "datalad:948668c2a16a4bd67f43c7d755180a2bb6a8d3f8", "@type": "Dataset", "name": "study-106", "identifier": "datalad:4e6738a0-dc76-11ea-94dc-7cdd908c7490"}, {"@id": "datalad:44ecdff2a5c2d637282296145583281f60b9ab8a", "@type": "Dataset", "name": "study-107", "identifier": "datalad:4da8b48e-dc76-11ea-964e-7cdd908c7490"}, {"@id": "datalad:726f2e8343126771457131053b407dfcb414a2f9", "@type": "Dataset", "name": "study-108", "identifier": "datalad:4ce38894-dc76-11ea-8fef-7cdd908c7490"}, {"@id": "datalad:6988beed1ae0d570c6d94d6917570f60896215e4", "@type": "Dataset", "name": "study-109", "identifier": "datalad:4c2ee286-dc76-11ea-911b-7cdd908c7490"}, {"@id": "datalad:7fe9d0ec3bbeaf308b911646a998aab35aaf37c1", "@type": "Dataset", "name": "study-115", "identifier": "datalad:4b88f9a2-dc76-11ea-a40f-7cdd908c7490"}, {"@id": "datalad:bde639a2219d76cb919bf04974b0fa12bc387c1a", "@type": "Dataset", "name": "study-44", "identifier": "datalad:5d1d514a-dc76-11ea-a8da-7cdd908c7490"}, {"@id": "datalad:87c7f5cc16b5dc4ec1d685af45772a140109dfbf", "@type": "Dataset", "name": "study-47", "identifier": "datalad:5c62610a-dc76-11ea-af43-7cdd908c7490"}, {"@id": "datalad:e5b75a417188555b2d4cc934220d3c8496dcd6ed", "@type": "Dataset", "name": "study-74", "identifier": "datalad:5ba774b2-dc76-11ea-9186-7cdd908c7490"}, {"@id": "datalad:9d789c8a9a961d78dbc3bdff6a57d516b7a9295b", "@type": "Dataset", "name": "study-82", "identifier": "datalad:5b050b1e-dc76-11ea-88a8-7cdd908c7490"}, {"@id": "datalad:72f1521d1568c17315a1d372761e5fdf51a70601", "@type": "Dataset", "name": "study-83", "identifier": "datalad:5a5bc5e0-dc76-11ea-a9da-7cdd908c7490"}, {"@id": "datalad:565caadfa279240b63caf7d18ffe2e4a706d4090", "@type": "Dataset", "name": "study-84", "identifier": "datalad:59bff4ee-dc76-11ea-8f22-7cdd908c7490"}, {"@id": "datalad:923c8a9d8f55f08c9334ab2b2dbf6fd15317d52f", "@type": "Dataset", "name": "study-85", "identifier": "datalad:58ff8024-dc76-11ea-ad78-7cdd908c7490"}, {"@id": "datalad:1958379323b6e61467c85fc872b78c39a0685fff", "@type": "Dataset", "name": "study-90", "identifier": "datalad:58253d06-dc76-11ea-aa7e-7cdd908c7490"}, {"@id": "datalad:a94708f2ac2d05a91c3d1eda39f55f77dc838913", "@type": "Dataset", "name": "study-91", "identifier": "datalad:57796120-dc76-11ea-a8c4-7cdd908c7490"}, {"@id": "datalad:3bd3cfa689739967fa0eb76911c872b600921d85", "@type": "Dataset", "name": "study-92", "identifier": "datalad:56da392e-dc76-11ea-91f4-7cdd908c7490"}, {"@id": "datalad:b524c3740bd00bdb225ca46216d5a0f684ef4954", "@type": "Dataset", "name": "study-93", "identifier": "datalad:56370470-dc76-11ea-b64b-7cdd908c7490"}, {"@id": "datalad:73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "@type": "Dataset", "name": "study-95", "identifier": "datalad:5599d916-dc76-11ea-8d5f-7cdd908c7490"}, {"@id": "datalad:5903fb2ae6f3c83c4ae9c54e7aef70a961758ebd", "@type": "Dataset", "name": "study-96", "identifier": "datalad:54ded198-dc76-11ea-9eff-7cdd908c7490"}, {"@id": "datalad:385a7cdb4ff314b09c8b56c2b23cc157dbfc0291", "@type": "Dataset", "name": "study-97", "identifier": "datalad:542308c8-dc76-11ea-940a-7cdd908c7490"}, {"@id": "datalad:b318a98f2fbcd0e5091c5d5a9df495cf7b1b81df", "@type": "Dataset", "name": "study-98", "identifier": "datalad:535edf84-dc76-11ea-979c-7cdd908c7490"}, {"@id": "datalad:a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "@type": "Dataset", "name": "study-99", "identifier": "datalad:52b2f098-dc76-11ea-a6da-7cdd908c7490"}], "distribution": [{"name": "test_metadata", "url": "https://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:x-oauth-basic@github.com/datalad/test_metadata"}, {"name": "md-target", "url": "https://gitlab-ci-token:QMsYDo2WWz5j7kNJxa2N@jugit.fz-juelich.de/c.moench/metadata-store"}]}]}}
{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1647851151.4307828, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_studyminimeta", "extractor_version": "0.1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/"}, "@graph": [{"@id": "#study", "@type": "CreativeWork", "name": "Mueller14_EEGSZ", "keywords": ["Schizophrenia"], "accountablePerson": "r.langner@fz-juelich.de", "contributor": [{"@id": "https://schema.datalad.org/person#v.mueller@fz-juelich.de"}]}, {"@id": "https://schema.datalad.org/datalad_dataset#52b2f098-dc76-11ea-a6da-7cdd908c7490", "@type": "Dataset", "version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "name": "Mueller14_EEGSZ", "url": "http://inm7.de/datasets/data/BnB_USER/Mueller/Projects/Mueller14_EEGSZ", "author": [{"@id": "https://schema.datalad.org/person#v.mueller@fz-juelich.de"}], "description": "<this is an autogenerated description for dataset 52b2f098-dc76-11ea-a6da-7cdd908c7490, since no description was provided by the author, and because google rich-results requires the description-property in schmema.org/Dataset metadatatypes>"}, {"@id": "#publicationList", "@list": [{"@id": "#publication[0]", "@type": "ScholarlyArticle", "headline": "Modulation of affective face processing deficits in Schizophrenia by congruent emotional sounds", "datePublished": 2014, "sameAs": "10.1093/scan/nss107", "pagination": "436-444", "author": [{"@id": "https://schema.datalad.org/person#v.mueller@fz-juelich.de"}, {"@id": "https://schema.datalad.org/person#t.kellermann@fz-juelich.de"}, {"@id": "https://schema.datalad.org/person#s.c.seligman@example.com"}, {"@id": "https://schema.datalad.org/person#b.i.turetsky@example.com"}, {"@id": "https://schema.datalad.org/person#s.eickhoff@fz-juelich.de"}], "publication": {"@id": "https://schema.datalad.org/publication_event#Soc Cogn Affect Neurosci", "@type": "PublicationEvent", "name": "Soc Cogn Affect Neurosci"}, "isPartOf": {"@id": "#issue(4)", "@type": "PublicationIssue", "issue_number": 4, "isPartOf": {"@id": "#volume(9)", "@type": "PublicationVolume", "volumeNumber": 9}}}]}, {"@id": "#personList", "@list": [{"@id": "https://schema.datalad.org/person#r.langner@fz-juelich.de", "@type": "Person", "email": "r.langner@fz-juelich.de", "name": "Dr. phil.  Robert Langner", "givenName": "Robert", "familyName": "Langner", "honorificSuffix": "Dr. phil."}, {"@id": "https://schema.datalad.org/person#v.mueller@fz-juelich.de", "@type": "Person", "email": "v.mueller@fz-juelich.de", "name": "Dr. rer. med.  Veronika M\u00fcller", "givenName": "Veronika", "familyName": "M\u00fcller", "honorificSuffix": "Dr. rer. med."}, {"@id": "https://schema.datalad.org/person#t.kellermann@fz-juelich.de", "@type": "Person", "email": "t.kellermann@fz-juelich.de", "name": "Dr.  Thilo Kellermann", "givenName": "Thilo", "familyName": "Kellermann", "honorificSuffix": "Dr."}, {"@id": "https://schema.datalad.org/person#s.c.seligman@example.com", "@type": "Person", "email": "s.c.seligman@example.com", "name": " S. C. Seligman", "givenName": "S. C.", "familyName": "Seligman"}, {"@id": "https://schema.datalad.org/person#b.i.turetsky@example.com", "@type": "Person", "email": "b.i.turetsky@example.com", "name": " B. I. Turetsky", "givenName": "B. I.", "familyName": "Turetsky"}, {"@id": "https://schema.datalad.org/person#s.eickhoff@fz-juelich.de", "@type": "Person", "email": "s.eickhoff@fz-juelich.de", "name": "Prof. Dr.  Simon Eickhoff", "givenName": "Simon", "familyName": "Eickhoff", "honorificSuffix": "Prof. Dr."}]}]}}
{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1648477354.3841684, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "refcommit": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "comment": "test-implementation of core_dataset"}}
{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1648565713.58595, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/", "datalad": "http://dx.datalad.org/"}, "@graph": [{"@id": "59286713dacabfbce1cecf4c865fff5a", "@type": "agent", "name": "Christian M\u00f6nch", "email": "christian.moench@web.de"}, {"@id": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "identifier": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "@type": "Dataset", "version": "0-9-ga9ff17c", "dateCreated": "2020-08-12T10:32:14+02:00", "dateModified": "2020-09-18T16:28:58+02:00", "hasContributor": {"@id": "59286713dacabfbce1cecf4c865fff5a"}, "distribution": [{"name": "origin", "@id": "datalad:714a099d-e70f-4159-be81-32202da7573d"}]}]}}
"""


critical_lines_json = \
"""{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1647851151.4307828, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_studyminimeta", "extractor_version": "0.1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/"}, "@graph": [{"@id": "#study", "@type": "CreativeWork", "name": "Mueller14_EEGSZ", "keywords": ["Schizophrenia"], "accountablePerson": "r.langner@fz-juelich.de", "contributor": [{"@id": "https://schema.datalad.org/person#v.mueller@fz-juelich.de"}]}, {"@id": "https://schema.datalad.org/datalad_dataset#52b2f098-dc76-11ea-a6da-7cdd908c7490", "@type": "Dataset", "version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "name": "Mueller14_EEGSZ", "url": "http://inm7.de/datasets/data/BnB_USER/Mueller/Projects/Mueller14_EEGSZ", "author": [{"@id": "https://schema.datalad.org/person#v.mueller@fz-juelich.de"}], "description": "<this is an autogenerated description for dataset 52b2f098-dc76-11ea-a6da-7cdd908c7490, since no description was provided by the author, and because google rich-results requires the description-property in schmema.org/Dataset metadatatypes>"}, {"@id": "#publicationList", "@list": [{"@id": "#publication[0]", "@type": "ScholarlyArticle", "headline": "Modulation of affective face processing deficits in Schizophrenia by congruent emotional sounds", "datePublished": 2014, "sameAs": "10.1093/scan/nss107", "pagination": "436-444", "author": [{"@id": "https://schema.datalad.org/person#v.mueller@fz-juelich.de"}, {"@id": "https://schema.datalad.org/person#t.kellermann@fz-juelich.de"}, {"@id": "https://schema.datalad.org/person#s.c.seligman@example.com"}, {"@id": "https://schema.datalad.org/person#b.i.turetsky@example.com"}, {"@id": "https://schema.datalad.org/person#s.eickhoff@fz-juelich.de"}], "publication": {"@id": "https://schema.datalad.org/publication_event#Soc Cogn Affect Neurosci", "@type": "PublicationEvent", "name": "Soc Cogn Affect Neurosci"}, "isPartOf": {"@id": "#issue(4)", "@type": "PublicationIssue", "issue_number": 4, "isPartOf": {"@id": "#volume(9)", "@type": "PublicationVolume", "volumeNumber": 9}}}]}, {"@id": "#personList", "@list": [{"@id": "https://schema.datalad.org/person#r.langner@fz-juelich.de", "@type": "Person", "email": "r.langner@fz-juelich.de", "name": "Dr. phil.  Robert Langner", "givenName": "Robert", "familyName": "Langner", "honorificSuffix": "Dr. phil."}, {"@id": "https://schema.datalad.org/person#v.mueller@fz-juelich.de", "@type": "Person", "email": "v.mueller@fz-juelich.de", "name": "Dr. rer. med.  Veronika M\u00fcller", "givenName": "Veronika", "familyName": "M\u00fcller", "honorificSuffix": "Dr. rer. med."}, {"@id": "https://schema.datalad.org/person#t.kellermann@fz-juelich.de", "@type": "Person", "email": "t.kellermann@fz-juelich.de", "name": "Dr.  Thilo Kellermann", "givenName": "Thilo", "familyName": "Kellermann", "honorificSuffix": "Dr."}, {"@id": "https://schema.datalad.org/person#s.c.seligman@example.com", "@type": "Person", "email": "s.c.seligman@example.com", "name": " S. C. Seligman", "givenName": "S. C.", "familyName": "Seligman"}, {"@id": "https://schema.datalad.org/person#b.i.turetsky@example.com", "@type": "Person", "email": "b.i.turetsky@example.com", "name": " B. I. Turetsky", "givenName": "B. I.", "familyName": "Turetsky"}, {"@id": "https://schema.datalad.org/person#s.eickhoff@fz-juelich.de", "@type": "Person", "email": "s.eickhoff@fz-juelich.de", "name": "Prof. Dr.  Simon Eickhoff", "givenName": "Simon", "familyName": "Eickhoff", "honorificSuffix": "Prof. Dr."}]}]}}
{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1648477354.3841684, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "refcommit": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "comment": "test-implementation of core_dataset"}}
{"type": "dataset", "root_dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "root_dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "dataset_path": "study-99", "dataset_id": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "dataset_version": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "extraction_time": 1648565713.58595, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/", "datalad": "http://dx.datalad.org/"}, "@graph": [{"@id": "59286713dacabfbce1cecf4c865fff5a", "@type": "agent", "name": "Christian M\u00f6nch", "email": "christian.moench@web.de"}, {"@id": "a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "identifier": "52b2f098-dc76-11ea-a6da-7cdd908c7490", "@type": "Dataset", "version": "0-9-ga9ff17c", "dateCreated": "2020-08-12T10:32:14+02:00", "dateModified": "2020-09-18T16:28:58+02:00", "hasContributor": {"@id": "59286713dacabfbce1cecf4c865fff5a"}, "distribution": [{"name": "origin", "@id": "datalad:714a099d-e70f-4159-be81-32202da7573d"}]}]}}
{"type": "dataset", "dataset_path": "study-95", "dataset_id": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "dataset_version": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "extraction_time": 1647789778.6307757, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"id": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "refcommit": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "comment": "test-implementation of core_dataset"}}
{"type": "dataset", "dataset_path": "study-95", "dataset_id": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "dataset_version": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "extraction_time": 1648565892.8312778, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/", "datalad": "http://dx.datalad.org/"}, "@graph": [{"@id": "59286713dacabfbce1cecf4c865fff5a", "@type": "agent", "name": "Christian M\u00f6nch", "email": "christian.moench@web.de"}, {"@id": "73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "identifier": "5599d916-dc76-11ea-8d5f-7cdd908c7490", "@type": "Dataset", "version": "0-9-g73ad003", "dateCreated": "2020-08-12T10:32:19+02:00", "dateModified": "2020-09-18T16:28:56+02:00", "hasContributor": {"@id": "59286713dacabfbce1cecf4c865fff5a"}, "distribution": [{"name": "origin", "@id": "datalad:1513095d-1fdc-45ca-8e86-d71d26d7b4af"}]}]}}
{"type": "dataset", "dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "extraction_time": 1648477352.674133, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core_dataset", "extractor_version": "0.0.1", "extraction_parameter": {}, "extracted_metadata": {"id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "refcommit": "0281546196aaddfa88ea1b5f40396a5960ed7040", "comment": "test-implementation of core_dataset"}}
{"type": "dataset", "dataset_id": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "dataset_version": "0281546196aaddfa88ea1b5f40396a5960ed7040", "extraction_time": 1648565711.668348, "agent_name": "Christian M\u00f6nch", "agent_email": "christian.moench@web.de", "extractor_name": "metalad_core", "extractor_version": "1", "extraction_parameter": {}, "extracted_metadata": {"@context": {"@vocab": "http://schema.org/", "datalad": "http://dx.datalad.org/"}, "@graph": [{"@id": "59286713dacabfbce1cecf4c865fff5a", "@type": "agent", "name": "Christian M\u00f6nch", "email": "christian.moench@web.de"}, {"@id": "0281546196aaddfa88ea1b5f40396a5960ed7040", "identifier": "84e51a9a-dc8b-11ea-ae66-7cdd908c7490", "@type": "Dataset", "version": "0-72-g0281546", "dateCreated": "2020-08-12T13:03:58+02:00", "dateModified": "2020-09-29T13:46:46+02:00", "hasContributor": {"@id": "59286713dacabfbce1cecf4c865fff5a"}, "hasPart": [{"@id": "datalad:2c332c81f32ebe4d6bb3e59e3d895581648b7030", "@type": "Dataset", "name": "study-100", "identifier": "datalad:52142b84-dc76-11ea-98c5-7cdd908c7490"}, {"@id": "datalad:443567ac95675242b2cde9615c0cd2830104c309", "@type": "Dataset", "name": "study-101", "identifier": "datalad:5146e8d6-dc76-11ea-a2da-7cdd908c7490"}, {"@id": "datalad:934de4bb707a35b3b940536760a9f8d095e0430e", "@type": "Dataset", "name": "study-102", "identifier": "datalad:506ce096-dc76-11ea-95f0-7cdd908c7490"}, {"@id": "datalad:f222e7c8828c7345101873252efa0c56db3eec84", "@type": "Dataset", "name": "study-103", "identifier": "datalad:4fb7be64-dc76-11ea-ad07-7cdd908c7490"}, {"@id": "datalad:617610cc3db409714bd9506dda88c471b1bedd6d", "@type": "Dataset", "name": "study-104", "identifier": "datalad:4f03a794-dc76-11ea-994b-7cdd908c7490"}, {"@id": "datalad:948668c2a16a4bd67f43c7d755180a2bb6a8d3f8", "@type": "Dataset", "name": "study-106", "identifier": "datalad:4e6738a0-dc76-11ea-94dc-7cdd908c7490"}, {"@id": "datalad:44ecdff2a5c2d637282296145583281f60b9ab8a", "@type": "Dataset", "name": "study-107", "identifier": "datalad:4da8b48e-dc76-11ea-964e-7cdd908c7490"}, {"@id": "datalad:726f2e8343126771457131053b407dfcb414a2f9", "@type": "Dataset", "name": "study-108", "identifier": "datalad:4ce38894-dc76-11ea-8fef-7cdd908c7490"}, {"@id": "datalad:6988beed1ae0d570c6d94d6917570f60896215e4", "@type": "Dataset", "name": "study-109", "identifier": "datalad:4c2ee286-dc76-11ea-911b-7cdd908c7490"}, {"@id": "datalad:7fe9d0ec3bbeaf308b911646a998aab35aaf37c1", "@type": "Dataset", "name": "study-115", "identifier": "datalad:4b88f9a2-dc76-11ea-a40f-7cdd908c7490"}, {"@id": "datalad:bde639a2219d76cb919bf04974b0fa12bc387c1a", "@type": "Dataset", "name": "study-44", "identifier": "datalad:5d1d514a-dc76-11ea-a8da-7cdd908c7490"}, {"@id": "datalad:87c7f5cc16b5dc4ec1d685af45772a140109dfbf", "@type": "Dataset", "name": "study-47", "identifier": "datalad:5c62610a-dc76-11ea-af43-7cdd908c7490"}, {"@id": "datalad:e5b75a417188555b2d4cc934220d3c8496dcd6ed", "@type": "Dataset", "name": "study-74", "identifier": "datalad:5ba774b2-dc76-11ea-9186-7cdd908c7490"}, {"@id": "datalad:9d789c8a9a961d78dbc3bdff6a57d516b7a9295b", "@type": "Dataset", "name": "study-82", "identifier": "datalad:5b050b1e-dc76-11ea-88a8-7cdd908c7490"}, {"@id": "datalad:72f1521d1568c17315a1d372761e5fdf51a70601", "@type": "Dataset", "name": "study-83", "identifier": "datalad:5a5bc5e0-dc76-11ea-a9da-7cdd908c7490"}, {"@id": "datalad:565caadfa279240b63caf7d18ffe2e4a706d4090", "@type": "Dataset", "name": "study-84", "identifier": "datalad:59bff4ee-dc76-11ea-8f22-7cdd908c7490"}, {"@id": "datalad:923c8a9d8f55f08c9334ab2b2dbf6fd15317d52f", "@type": "Dataset", "name": "study-85", "identifier": "datalad:58ff8024-dc76-11ea-ad78-7cdd908c7490"}, {"@id": "datalad:1958379323b6e61467c85fc872b78c39a0685fff", "@type": "Dataset", "name": "study-90", "identifier": "datalad:58253d06-dc76-11ea-aa7e-7cdd908c7490"}, {"@id": "datalad:a94708f2ac2d05a91c3d1eda39f55f77dc838913", "@type": "Dataset", "name": "study-91", "identifier": "datalad:57796120-dc76-11ea-a8c4-7cdd908c7490"}, {"@id": "datalad:3bd3cfa689739967fa0eb76911c872b600921d85", "@type": "Dataset", "name": "study-92", "identifier": "datalad:56da392e-dc76-11ea-91f4-7cdd908c7490"}, {"@id": "datalad:b524c3740bd00bdb225ca46216d5a0f684ef4954", "@type": "Dataset", "name": "study-93", "identifier": "datalad:56370470-dc76-11ea-b64b-7cdd908c7490"}, {"@id": "datalad:73ad0039ade25bd0f6b0dbf9dd13006e3721cc38", "@type": "Dataset", "name": "study-95", "identifier": "datalad:5599d916-dc76-11ea-8d5f-7cdd908c7490"}, {"@id": "datalad:5903fb2ae6f3c83c4ae9c54e7aef70a961758ebd", "@type": "Dataset", "name": "study-96", "identifier": "datalad:54ded198-dc76-11ea-9eff-7cdd908c7490"}, {"@id": "datalad:385a7cdb4ff314b09c8b56c2b23cc157dbfc0291", "@type": "Dataset", "name": "study-97", "identifier": "datalad:542308c8-dc76-11ea-940a-7cdd908c7490"}, {"@id": "datalad:b318a98f2fbcd0e5091c5d5a9df495cf7b1b81df", "@type": "Dataset", "name": "study-98", "identifier": "datalad:535edf84-dc76-11ea-979c-7cdd908c7490"}, {"@id": "datalad:a9ff17c0c344d6dc91ead0141f2b9927f11064bc", "@type": "Dataset", "name": "study-99", "identifier": "datalad:52b2f098-dc76-11ea-a6da-7cdd908c7490"}], "distribution": [{"name": "test_metadata", "url": "https://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:x-oauth-basic@github.com/datalad/test_metadata"}, {"name": "md-target", "url": "https://gitlab-ci-token:QMsYDo2WWz5j7kNJxa2N@jugit.fz-juelich.de/c.moench/metadata-store"}]}]}}
"""


@with_tempfile(mkdir=True)
def test_add_regression_1(temp_dir: str):
    create_dataset_proper(temp_dir)

    json_objects = [
        json.loads(json_string)
        for json_string in critical_lines_json.splitlines()
    ]
    for json_object in json_objects:
        for result in meta_add(dataset=temp_dir,
                               metadata=json_object,
                               allow_id_mismatch=True):
            print(result)
            eq_(result["status"], "ok")

    results = list(meta_dump(dataset=temp_dir, path="", recursive=True))
    print(results)
    assert_equal(len(results), len(json_objects))
    for result in results:
        eq_(result["status"], "ok")
        root_dataset_id = result["metadata"].get("root_dataset_id", None)
        assert_not_equal(root_dataset_id, "<unknown>")


@with_tempfile(mkdir=True)
def test_multi_add_regression_1(temp_dir: str):
    create_dataset_proper(temp_dir)

    json_objects = [
        json.loads(json_string)
        for json_string in critical_lines_json.splitlines()
    ]
    for result in meta_add(dataset=temp_dir,
                           metadata=json_objects,
                           allow_id_mismatch=True):
        eq_(result["status"], "ok")

    results = list(meta_dump(dataset=temp_dir, path="*", recursive=True))
    for result in results:
        print(result["metadata"])
    assert_equal(len(results), len(json_objects))
    for result in results:
        eq_(result["status"], "ok")
        root_dataset_id = result["metadata"].get("root_dataset_id", None)
        assert_not_equal(root_dataset_id, "<unknown>")


@with_tempfile(mkdir=True)
def test_multi_add_regression_2(temp_dir: str):
    create_dataset_proper(temp_dir)

    with tempfile.NamedTemporaryFile(mode="tw") as json_input:
        json_input.write(critical_lines_json)

        for result in meta_add(dataset=temp_dir,
                               metadata=json_input.name,
                               json_lines=True,
                               allow_id_mismatch=True):
            eq_(result["status"], "ok")

        results = list(meta_dump(dataset=temp_dir, path="*", recursive=True))
        assert_equal(len(results), len(critical_lines_json.splitlines()))
        for result in results:
            eq_(result["status"], "ok")
            root_dataset_id = result["metadata"].get("root_dataset_id", None)
            assert_not_equal(root_dataset_id, "<unknown>")
