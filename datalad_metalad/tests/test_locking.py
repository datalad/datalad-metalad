import concurrent.futures
import os
import tempfile
from typing import List
from unittest.mock import patch
from unittest import SkipTest
from uuid import uuid4

from datalad.api import (
    meta_add,
    meta_dump,
)
from datalad.support.gitrepo import GitRepo
from datalad.tests.utils_pytest import (
    assert_in,
    eq_,
    skip_if,
)

from .utils import create_dataset
from ..metadatatypes import JSONType


dataset_id = uuid4()


meta_data_pattern = {
    "type": "file",
    "extractor_name": "ex_extractor_name",
    "extractor_version": "ex_extractor_version",
    "extraction_parameter": {"parameter1": "pvalue1"},
    "extraction_time": 1111666.3333,
    "agent_name": "test_name",
    "agent_email": "test email",
    "dataset_id": str(dataset_id),
    "dataset_version": "000000111111111112012121212121",
    "extracted_metadata": {"info": "some metadata"}
}


def get_metadata(index: int):
    return {
        **meta_data_pattern,
        "path": f"a/b/{index}",
        "extraction_time": 1000 + index,
    }


def call_meta_add(locked: bool,
                  *args,
                  **kwargs) -> List:

    if locked:
        return list(meta_add(*args, **kwargs))
    else:
        with patch("datalad_metalad.add.locked_backend"):
            return list(meta_add(*args, **kwargs))


def perform_concurrent_adds(locked: bool,
                            git_repo: GitRepo,
                            count: int):

    with concurrent.futures.ProcessPoolExecutor() as executor:
        running = set([
            executor.submit(
                call_meta_add,
                locked=locked,
                metadata=get_metadata(index),
                dataset=git_repo.path,
                result_renderer="disabled"
            )
            for index in range(count)
        ])
        concurrent.futures.wait(running)


def get_all_metadata_records(git_repo: GitRepo) -> List[JSONType]:
    res = meta_dump(
        dataset=git_repo.path,
        path="*",
        recursive=True,
        result_renderer="disabled")
    return list(res)


def verify_locking_adds(git_repo: GitRepo, test_process_number: int):
    perform_concurrent_adds(True, git_repo, test_process_number)
    metadata_records = get_all_metadata_records(git_repo)
    eq_(len(metadata_records), test_process_number)


@skip_if(cond='GITHUB_WORKFLOW' in os.environ)
def test_meta_add_locking_impact_end_to_end():

    test_process_number = 100

    with tempfile.TemporaryDirectory() as temp_dir:
        git_repo = create_dataset(temp_dir, dataset_id)

        perform_concurrent_adds(False, git_repo, test_process_number)

        metadata_records = get_all_metadata_records(git_repo)
        if len(metadata_records) == test_process_number:
            raise SkipTest(
                "cannot trigger race condition, "
                "leaving meta-add locking test")

    with tempfile.TemporaryDirectory() as temp_dir:
        git_repo = create_dataset(temp_dir, dataset_id)
        verify_locking_adds(git_repo, test_process_number)


@skip_if(cond='GITHUB_WORKFLOW' in os.environ)
def test_meta_add_locking_end_to_end():

    test_process_number = 100

    with tempfile.TemporaryDirectory() as temp_dir:
        git_repo = create_dataset(temp_dir, dataset_id)
        verify_locking_adds(git_repo, test_process_number)


def test_multiple_adds():
    # This is a regression test. Ensure that newly added records do not
    # overwrite old records
    with tempfile.TemporaryDirectory() as temp_dir:
        git_repo = create_dataset(temp_dir, dataset_id)
        meta_add(
            dataset=git_repo.path,
            metadata=get_metadata(0),
            result_renderer="disabled")
        meta_add(
            dataset=git_repo.path,
            metadata=get_metadata(1),
            result_renderer="disabled")
        metadata_records = get_all_metadata_records(git_repo)
        eq_(len(metadata_records), 2)
        metadata = [mr["metadata"] for mr in metadata_records]
        assert_in(get_metadata(0), metadata)
        assert_in(get_metadata(1), metadata)
