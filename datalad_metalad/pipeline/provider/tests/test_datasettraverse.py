import os
from pathlib import Path

from datalad.tests.utils import (
    assert_equal,
    with_tempfile,
)

from ....tests.utils import create_dataset_proper
from ..datasettraverse import DatasetTraverser


@with_tempfile(mkdir=True)
def test_relative_top_level_dir(temp_dir: str):
    relative_path_str = "./some/path/dataset_0"

    dataset_path = Path(temp_dir) / relative_path_str
    dataset_path.mkdir(parents=True)
    create_dataset_proper(dataset_path)

    old_path = Path.cwd()
    os.chdir(str(temp_dir))
    traverser = DatasetTraverser(
        top_level_dir=relative_path_str,
        item_type="both"
    )
    assert_equal(traverser.fs_base_path, dataset_path)

    # prevent teardown error due to modified working directory
    os.chdir(str(old_path))
