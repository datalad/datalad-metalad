from pathlib import Path
from typing import Optional

from datalad.tests.utils import (
    assert_equal,
    chpwd,
    with_tempfile,
)

from ....tests.utils import create_dataset_proper
from ..datasettraverse import DatasetTraverser


@with_tempfile(mkdir=True)
def test_relative_and_unresolved_top_level_dir(temp_dir: Optional[str] = None):
    relative_path_str = "./some/path/dataset_0"
    unresolved_path = "./some/path/../path/dataset_0"

    dataset_path = Path(temp_dir) / relative_path_str
    dataset_path.mkdir(parents=True)
    create_dataset_proper(dataset_path)

    with chpwd(str(temp_dir)):
        # check relative paths
        traverser = DatasetTraverser(
            top_level_dir=relative_path_str,
            item_type="both"
        )
        assert_equal(traverser.fs_base_path, dataset_path)

        # check unresolved paths
        traverser = DatasetTraverser(
            top_level_dir=Path(temp_dir) / unresolved_path,
            item_type="both"
        )

        tuple(traverser.next_object())
        assert_equal(traverser.fs_base_path, dataset_path.resolve())
