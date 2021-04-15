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

from datalad.api import meta_add, meta_aggregate, meta_dump
from datalad.support.exceptions import InsufficientArgumentsError
from datalad.support.gitrepo import GitRepo
from datalad.tests.utils import assert_raises, assert_result_count, eq_


def _add_dataset_level_metadata(metadata_store: Path,
                                dataset_id: str,
                                dataset_version: str,
                                metadata_content: str):
    meta_add(
        {
            "type": "dataset",
            "extractor_name": "test_dataset",
            "extractor_version": "1.0",
            "extraction_parameter": {},
            "extraction_time": "1000.1",
            "agent_name": "test_aggregate",
            "agent_email": "test@test.aggregate",
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "extracted_metadata": {
                "content": metadata_content
            }
        },
        str(metadata_store))


def test_basic_aggregation():

    with tempfile.TemporaryDirectory() as root_dataset_dir_str:
        root_dataset_dir = Path(root_dataset_dir_str)
        subdataset_0_dir = root_dataset_dir / "subdataset_0"
        subdataset_1_dir = root_dataset_dir / "subdataset_1"

        root_git_repo = GitRepo(root_dataset_dir)
        subdataset_0_repo = GitRepo(subdataset_0_dir)
        subdataset_1_repo = GitRepo(subdataset_1_dir)

        # TODO: there is a dependency here in meta_add. We should instead
        #  use the model API to add metadata to metadata stores

        for index in range(3):
            _add_dataset_level_metadata(
                [
                    root_dataset_dir,
                    subdataset_0_dir,
                    subdataset_1_dir
                ][index],
                f"00000000-0000-0000-0000-00000000000{index}",
                f"000000000000000000000000000000000000000{index}",
                f"metadata-content_{index}")

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
                metadata_store=str(root_dataset_dir),
                recursive=True)

            assert_result_count(result_objects, 3)
            for index, result in enumerate(result_objects):
                result_object = result["metadata"]["dataset_level_metadata"]
                eq_(result_object["root_dataset_identifier"],
                    "00000000-0000-0000-0000-000000000000")

                eq_(result_object["root_dataset_version"],
                    "0000000000000000000000000000000000000000")

                eq_(result_object["dataset_identifier"],
                    f"00000000-0000-0000-0000-00000000000{index}")

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

        root_git_repo = GitRepo(root_dataset_dir)
        subdataset_0_dir.mkdir()
        subdataset_1_dir.mkdir()

        assert_raises(
            InsufficientArgumentsError,
            meta_aggregate,
            str(root_dataset_dir),
            [
                "subdataset_0", str(subdataset_0_dir),
                "subdataset_1", str(subdataset_1_dir)
            ])
