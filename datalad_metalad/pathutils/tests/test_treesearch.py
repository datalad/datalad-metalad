import unittest
from typing import List, Tuple

from dataladmetadatamodel.filetree import FileTree
from datalad_metalad.pathutils.treesearch import MatchRecord, TreeSearch


class TestJoin(unittest.TestCase):
    def test_empty_join(self):
        self.assertEqual(
            TreeSearch._join("", ""),
            ""
        )

    def test_empty_leading(self):
        self.assertEqual(
            TreeSearch._join("", "", "a"),
            "a"
        )

    def test_common(self):
        self.assertEqual(
            TreeSearch._join("", "", "a", "b"),
            "a/b"
        )

    def test_multiple_dash(self):
        self.assertEqual(
            TreeSearch._join("/", "a//", "b"),
            "/a/b"
        )

    def test_intermediate_root(self):
        self.assertEqual(
            TreeSearch._join("/", "a//", "/b"),
            "/b"
        )


class TestTreeSearchBase(unittest.TestCase):
    @staticmethod
    def create_tree_search_from_paths(path_list: List[str]) -> TreeSearch:
        tree = FileTree("", "")
        for path in path_list:
            tree.add_file(path)
        return TreeSearch(tree)

    def setUp(self) -> None:
        self.path_list = [
            ".datalad_metadata",
            "s1/s1.1/d1.1.1/.datalad_metadata",
            "s1/s1.2/d1.2.1/.datalad_metadata",
            "s2/d2.1/.datalad_metadata",
            "d3/.datalad_metadata",
            "d3/some_file",
        ]
        self.tree_search = self.create_tree_search_from_paths(self.path_list)

    def assertSameElements(self, list_a: List, list_b: List):
        l_a = list_a[:]
        l_b = list_b[:]
        while len(l_a) > 0:
            e = l_a.pop()
            self.assertIn(e, l_b)
            l_b.remove(e)
        self.assertTrue(len(l_b) == 0)

    def assertPathsInResult(self, matches: List[MatchRecord], paths: List[str]):
        self.assertSameElements(
            list(map(lambda match_record: match_record.path, matches)),
            paths)


class TestTreeSearchMatching(TestTreeSearchBase):
    @staticmethod
    def _show_result(found, failed):
        if found:
            print("\n".join(
                map(lambda record: f"{record.path}\t{'f' if record.node.is_leaf_node() else 'd'}", found)))
        if failed:
            print("\n".join(map(lambda n: f"no such dataset or directory: {n}", failed)))
        return found, failed

    def _test_pattern(self, pattern_list: List[str], expected_matches: List[str]):
        found, failed = self.tree_search.get_matching_paths(pattern_list)
        self.assertPathsInResult(found, expected_matches)
        self.assertListEqual(failed, [])

    def _test_pattern_rec(self, pattern_list: List[str], expected_matches: List[str]):
        found, failed = self.tree_search.get_matching_paths_recursive(pattern_list)
        self.assertPathsInResult(found, expected_matches)
        self.assertListEqual(failed, [])

    def test_auto_list_root_on(self):
        found, failed = self.tree_search.get_matching_paths([""], auto_list_root=True)
        self.assertPathsInResult(
            found,
            [
                ".datalad_metadata",
                "s1",
                "s2",
                "d3"
            ]
        )
        self.assertListEqual(failed, [])

    def test_auto_list_root_off(self):
        found, failed = self.tree_search.get_matching_paths([""], auto_list_root=False)
        self.assertListEqual(found, [])
        self.assertListEqual(failed, [])

    def test_pattern_1(self):
        self._test_pattern(
            ["*"],
            [
                ".datalad_metadata",
                "s1",
                "s2",
                "d3"])

    def test_pattern_2(self):
        self._test_pattern(["s*"], ["s1", "s2"])

    def test_pattern_3(self):
        self._test_pattern(["s*/*"], ["s1/s1.1", "s1/s1.2", "s2/d2.1"])

    def test_pattern_4(self):
        self._test_pattern(["d3/*"], ["d3/.datalad_metadata", "d3/some_file"])

    def test_pattern_5(self):
        self._test_pattern(["*/s*"], ["s1/s1.1", "s1/s1.2", "d3/some_file"])

    def test_pattern_6(self):
        found, failed = self.tree_search.get_matching_paths(["s*/xxx"])
        self.assertListEqual(found, [])
        self.assertListEqual(failed, ["s*/xxx"])

    def test_pattern_7(self):
        found, failed = self.tree_search.get_matching_paths(["see"])
        self.assertListEqual(found, [])
        self.assertListEqual(failed, ["see"])

    def test_recursive_list_1(self):
        self._test_pattern_rec([""], self.path_list)

    def test_recursive_list_2(self):
        self._test_pattern_rec(["d3"], ["d3/.datalad_metadata", "d3/some_file"])


if __name__ == '__main__':
    unittest.main()
