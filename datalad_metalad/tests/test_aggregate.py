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

import tempfile
from pathlib import Path
from typing import Optional
from unittest.mock import patch
from uuid import UUID

from datalad.api import (
    meta_aggregate,
    meta_dump,
)
from datalad.support.exceptions import InsufficientArgumentsError
from datalad.tests.utils_pytest import (
    assert_not_in,
    assert_raises,
    assert_result_count,
    eq_,
)
from .utils import (
    add_dataset_level_metadata,
    create_dataset,
)


root_id = UUID("00010203-1011-2021-3031-404142434445")
sub_0_id = UUID("a0cc0203-1011-2021-3031-404142434445")
sub_1_id = UUID("a1cc0203-1011-2021-3031-404142434445")

version_base = "000000000000000000000000000000000000000{index}"


def _check_root_elements(result_object: dict,
                         dataset_path: Optional[str],
                         root_dataset_id: Optional[str],
                         root_dataset_version: Optional[str]):

    if dataset_path is None or dataset_path == "":
        # Ensure that identical values, i.e. dataset_id and
        # dataset_version, and empty values, i.e. dataset_path, are
        # not present in the result.
        assert_not_in("root_dataset_id", result_object)
        assert_not_in("root_dataset_version", result_object)
        assert_not_in("dataset_path", result_object)
    elif dataset_path != "":
        assert_not_in("root_dataset_id", result_object)
        assert_not_in("root_dataset_version", result_object)
        eq_(result_object["dataset_path"], dataset_path)
    else:
        eq_(result_object["root_dataset_id"], root_dataset_id)
        eq_(result_object["root_dataset_version"], root_dataset_version)
        eq_(result_object["dataset_path"], dataset_path)


def test_basic_aggregation():

    with tempfile.TemporaryDirectory() as root_dataset_dir_str:
        root_dataset_dir = Path(root_dataset_dir_str)
        subdataset_0_dir = root_dataset_dir / "subdataset_0"
        subdataset_1_dir = root_dataset_dir / "subdataset_1"

        create_dataset(str(root_dataset_dir), root_id)
        create_dataset(str(subdataset_0_dir), sub_0_id)
        create_dataset(str(subdataset_1_dir), sub_1_id)

        # TODO: there is a dependency here in meta_add. We should instead
        #  use the types API to add metadata to metadata stores

        for index in range(3):
            add_dataset_level_metadata(
                metadata_store=[
                    root_dataset_dir,
                    subdataset_0_dir,
                    subdataset_1_dir
                ][index],
                dataset_id=[
                    str(root_id),
                    str(sub_0_id),
                    str(sub_1_id)
                ][index],
                dataset_version=version_base.format(index=index),
                metadata_content=f"metadata-content_{index}")

        # We have to patch "does_version_contain_version_at" because the test
        # git repos have no commits.
        with patch("datalad_metalad.aggregate.does_version_contain_version_at") as p:

            # Ensure that the sub-datasets are not copied into one another
            p.return_value = False

            result = meta_aggregate(
                str(root_dataset_dir),
                [str(subdataset_0_dir), str(subdataset_1_dir)])

            result_objects = meta_dump(
                dataset=str(root_dataset_dir),
                recursive=True,
                result_renderer="disabled")

            assert_result_count(result_objects, 3)

            zero_version = version_base.format(index=0)
            check_parameters = [
                dict(dataset_path=None, root_dataset_id=None),
                dict(dataset_path="subdataset_0", root_dataset_id=str(root_id)),
                dict(dataset_path="subdataset_1", root_dataset_id=str(root_id)),
            ]

            for index, result in enumerate(result_objects):
                result_object = result["metadata"]

                _check_root_elements(
                    result_object=result_object,
                    root_dataset_version=zero_version,
                    **(check_parameters[index]))

                eq_(result_object["dataset_id"], [
                    str(root_id),
                    str(sub_0_id),
                    str(sub_1_id),
                ][index])

                eq_(
                    result_object["dataset_version"], [
                        version_base.format(index=0),
                        version_base.format(index=1),
                        version_base.format(index=2),
                    ][index])

                eq_(result_object["extractor_name"], "test_dataset")
                eq_(result_object["extracted_metadata"]["content"], [
                    "metadata-content_0",
                    "metadata-content_1",
                    "metadata-content_2",
                ][index])

            # Test a second aggregation
            result = meta_aggregate(
                str(root_dataset_dir),
                [str(subdataset_0_dir), str(subdataset_1_dir)])

            result_objects = meta_dump(
                dataset=str(root_dataset_dir),
                recursive=True,
                result_renderer="disabled")

            assert_result_count(result_objects, 3)


def test_missing_metadata_stores():

    with tempfile.TemporaryDirectory() as root:
        root_dataset_dir = Path(root)
        subdataset_0_dir = root_dataset_dir / "subdataset_0"
        subdataset_1_dir = root_dataset_dir / "subdataset_1"

        create_dataset(str(root_dataset_dir), root_id)
        create_dataset(str(subdataset_0_dir), sub_0_id)
        create_dataset(str(subdataset_1_dir), sub_1_id)

        assert_raises(
            InsufficientArgumentsError,
            meta_aggregate,
            str(root_dataset_dir),
            [str(subdataset_0_dir), str(subdataset_1_dir)])


def test_basic_aggregation_into_empty_store():

    with tempfile.TemporaryDirectory() as root_dataset_dir_str:
        root_dataset_dir = Path(root_dataset_dir_str)
        subdataset_0_dir = root_dataset_dir / "subdataset_0"
        subdataset_1_dir = root_dataset_dir / "subdataset_1"

        create_dataset(str(root_dataset_dir), root_id)
        create_dataset(str(subdataset_0_dir), sub_0_id)
        create_dataset(str(subdataset_1_dir), sub_1_id)

        # TODO: this is more an end-to-end test, since we depend
        #  on meta_add. We should instead use the datalad API to add
        #  metadata to metadata stores

        for index in range(2):
            add_dataset_level_metadata(
                metadata_store=[
                    subdataset_0_dir,
                    subdataset_1_dir
                ][index],
                dataset_id=[
                    str(sub_0_id),
                    str(sub_1_id)
                ][index],
                dataset_version=version_base.format(index=index),
                metadata_content=f"metadata-content_{index}")

        # We have to patch "does_version_contain_version_at" because the test
        # git repos have no commits and we want the code to assume that the
        # sub-datasets are at the given path in the given version.
        with patch("datalad_metalad.aggregate.does_version_contain_version_at") as p:

            # Ensure that the sub-datasets are not copied into one another
            p.return_value = False

            meta_aggregate(
                str(root_dataset_dir),
                [str(subdataset_0_dir), str(subdataset_1_dir)])

            result_objects = meta_dump(
                dataset=str(root_dataset_dir),
                recursive=True,
                result_renderer="disabled")

            assert_result_count(result_objects, 2)

            a_version = version_base.format(index="a")
            check_parameters = [
                dict(dataset_path="subdataset_0", root_dataset_id="<unknown>"),
                dict(dataset_path="subdataset_1", root_dataset_id="<unknown>"),
            ]

            for index, result in enumerate(result_objects):

                result_object = result["metadata"]

                _check_root_elements(
                    result_object=result_object,
                    root_dataset_version=a_version,
                    **(check_parameters[index]))

                eq_(result_object["dataset_id"], [
                        str(sub_0_id),
                        str(sub_1_id),
                    ][index])

                eq_(
                    result_object["dataset_version"], [
                        version_base.format(index=0),
                        version_base.format(index=1),
                    ][index])

                eq_(result_object["extractor_name"], "test_dataset")
                eq_(result_object["extracted_metadata"]["content"], [
                    "metadata-content_0",
                    "metadata-content_1",
                ][index])
