from pathlib import Path

from datalad.tests.utils import (
    assert_repo_status,
    assert_raises,
    assert_result_count,
    assert_in,
    eq_,
    known_failure_windows,
    with_tempfile,
    with_tree
)


from ..export import get_dir_for


def test_get_dir_for():
    dirs, file = get_dir_for("aaabbccddd", [3, 2, 2])
    eq_(dirs, Path("aaa") / "bb" / "cc")
    eq_(file, Path("ddd"))

    assert_raises(ValueError, get_dir_for, "ab", [1, 1])
