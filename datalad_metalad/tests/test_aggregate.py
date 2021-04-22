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
from unittest.mock import patch
from uuid import UUID

from datalad.api import meta_add, meta_aggregate, meta_dump
from datalad.support.exceptions import InsufficientArgumentsError
from datalad.tests.utils import assert_raises, assert_result_count, eq_

from .utils import add_dataset_level_metadata, create_dataset


root_id = UUID("00010203-1011-2021-3031-404142434445")
sub_0_id = UUID("a0cc0203-1011-2021-3031-404142434445")
sub_1_id = UUID("a1cc0203-1011-2021-3031-404142434445")


def test_basic_aggregation():

    with tempfile.TemporaryDirectory() as root_dataset_dir_str:
        root_dataset_dir = Path(root_dataset_dir_str)
        subdataset_0_dir = root_dataset_dir / "subdataset_0"
        subdataset_1_dir = root_dataset_dir / "subdataset_1"

        create_dataset(root_dataset_dir, root_id)
        create_dataset(subdataset_0_dir, sub_0_id)
        create_dataset(subdataset_1_dir, sub_1_id)

        # TODO: there is a dependency here in meta_add. We should instead
        #  use the model API to add metadata to metadata stores

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
                dataset_version=f"000000000000000000000000000000000000000{index}",
                metadata_content=f"metadata-content_{index}")

        # We have to patch "get_root_version_for_subset_version" because
        # the test git repos have no commits.
        with patch("datalad_metalad.aggregate.get_root_version_for_subset_version") as p:

            # Ensure that the root version is found
            p.return_value = ["0000000000000000000000000000000000000000"]

            result = meta_aggregate(
                str(root_dataset_dir),
                [
                    "subdataset_0", str(subdataset_0_dir),
                    "subdataset_1", str(subdataset_1_dir)
                ])

            result_objects = meta_dump(
                dataset=str(root_dataset_dir),
                recursive=True)

            assert_result_count(result_objects, 3)
            for index, result in enumerate(result_objects):
                result_object = result["metadata"]["dataset_level_metadata"]
                eq_(result_object["root_dataset_identifier"], str(root_id))

                eq_(result_object["root_dataset_version"],
                    "0000000000000000000000000000000000000000")

                eq_(result_object["dataset_identifier"], [
                    str(root_id),
                    str(sub_0_id),
                    str(sub_1_id)
                ][index])

                eq_(result_object["dataset_version"],
                    f"000000000000000000000000000000000000000{index}")

                eq_(result_object["dataset_path"], [
                    "",
                    "subdataset_0",
                    "subdataset_1"
                ][index])

                metadata_content = result_object["metadata"]["test_dataset"][0]

                eq_(metadata_content["extraction_result"]["content"],
                    f"metadata-content_{index}")


def test_missing_metadata_stores():

    with tempfile.TemporaryDirectory() as root:
        root_dataset_dir = Path(root)
        subdataset_0_dir = root_dataset_dir / "subdataset_0"
        subdataset_1_dir = root_dataset_dir / "subdataset_1"

        create_dataset(root_dataset_dir, root_id)
        create_dataset(subdataset_0_dir, sub_0_id)
        create_dataset(subdataset_1_dir, sub_1_id)

        assert_raises(
            InsufficientArgumentsError,
            meta_aggregate,
            str(root_dataset_dir),
            [
                "subdataset_0", str(subdataset_0_dir),
                "subdataset_1", str(subdataset_1_dir)
            ])


def test_basic_aggregation_into_empty_store():

    with tempfile.TemporaryDirectory() as root_dataset_dir_str:
        root_dataset_dir = Path(root_dataset_dir_str)
        subdataset_0_dir = root_dataset_dir / "subdataset_0"
        subdataset_1_dir = root_dataset_dir / "subdataset_1"

        create_dataset(root_dataset_dir, root_id)
        create_dataset(subdataset_0_dir, sub_0_id)
        create_dataset(subdataset_1_dir, sub_1_id)

        # TODO: this is more an end-to-end test, since we depend
        #  on meta_add. We should instead use the model API to add
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
                dataset_version=f"000000000000000000000000000000000000000{index}",
                metadata_content=f"metadata-content_{index}")

        # We have to patch "get_root_version_for_subset_version" because
        # the test git repos have no commits.
        with patch("datalad_metalad.aggregate.get_root_version_for_subset_version") as p:

            # Ensure that the root version is found
            p.return_value = ["0000000000000000000000000000000000000aaa"]

            meta_aggregate(
                str(root_dataset_dir),
                [
                    "subdataset_0", str(subdataset_0_dir),
                    "subdataset_1", str(subdataset_1_dir)
                ])

            result_objects = meta_dump(
                dataset=str(root_dataset_dir),
                recursive=True)

            assert_result_count(result_objects, 2)
            for index, result in enumerate(result_objects):
                result_object = result["metadata"]["dataset_level_metadata"]
                eq_(result_object["root_dataset_identifier"], "<unknown>")
                eq_(
                    result_object["root_dataset_version"],
                    "0000000000000000000000000000000000000aaa")

                eq_(result_object["dataset_identifier"],
                    [
                        str(sub_0_id),
                        str(sub_1_id)
                    ][index])

                eq_(result_object["dataset_version"],
                    f"000000000000000000000000000000000000000{index}")

                eq_(result_object["dataset_path"], [
                    "subdataset_0",
                    "subdataset_1"
                ][index])

                metadata_content = result_object["metadata"]["test_dataset"][0]

                eq_(metadata_content["extraction_result"]["content"],
                    f"metadata-content_{index}")
