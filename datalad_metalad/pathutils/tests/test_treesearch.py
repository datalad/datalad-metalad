import unittest
from typing import (
    Any,
    List
)

from dataladmetadatamodel.metadata import Metadata
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.datasettree import datalad_root_record_name
from dataladmetadatamodel.mtreenode import MTreeNode

from ..treesearch import (
    MatchRecord,
    TreeSearch
)


class TestTreeSearchBase(unittest.TestCase):
    @staticmethod
    def create_tree_search_from_paths(path_list: List[MetadataPath],
                                      leaf_class: Any
                                      ) -> TreeSearch:
        tree = MTreeNode(leaf_class)
        for path in path_list:
            tree.add_child_at(leaf_class(), path)
        return TreeSearch(tree)

    def setUp(self) -> None:
        self.path_list = [
            MetadataPath(datalad_root_record_name),
            MetadataPath("s1/s1.1/d1.1.1") / datalad_root_record_name,
            MetadataPath("s1/s1.2/d1.2.1") / datalad_root_record_name,
            MetadataPath("s2/d2.1") / datalad_root_record_name,
            MetadataPath("d3") / datalad_root_record_name,
            MetadataPath("d3/some_file"),
            MetadataPath("dataset_0.0") / datalad_root_record_name,
            MetadataPath("dataset_0.0/dataset_0.0.0") / datalad_root_record_name,
            MetadataPath("dataset_0.0/dataset_0.0.1") / datalad_root_record_name,
            MetadataPath("dataset_0.0/dataset_0.0.2") / datalad_root_record_name,
            MetadataPath("dataset_0.1") / datalad_root_record_name,
            MetadataPath("dataset_0.1/dataset_0.1.0") / datalad_root_record_name,
            MetadataPath("dataset_0.1/dataset_0.1.1") / datalad_root_record_name,
            MetadataPath("dataset_0.1/dataset_0.1.2") / datalad_root_record_name
        ]

        self.tree_search = self.create_tree_search_from_paths(
            self.path_list,
            Metadata)

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
                map(lambda record:
                    f"{record.path}\t{'f' if record.node.is_leaf_node() else 'd'}",
                    found)))
        if failed:
            print("\n".join(
                map(lambda n: f"no such dataset or directory: {n}", failed)))
        return found, failed

    def _test_pattern(self,
                      pattern_list: List[str],
                      expected_matches: List[str],
                      tree_search: TreeSearch,
                      recursive: bool = False):

        found, failed = tree_search.get_matching_paths(
            pattern_list=pattern_list,
            recursive=recursive)
        self.assertPathsInResult(found, expected_matches)
        self.assertListEqual(failed, [])

    def test_root(self):
        found, failed = self.tree_search.get_matching_paths(
            pattern_list=[""],
            recursive=False)

        # Only root and top-level directories should be reported
        self.assertPathsInResult(
            found,
            [MetadataPath("")])
        self.assertListEqual(failed, [])

    def test_root_elements(self):
        found, failed = self.tree_search.get_matching_paths(
            pattern_list=["*"],
            recursive=False)

        # Only top-level directories should be reported
        self.assertPathsInResult(
            found,
            [
                MetadataPath(datalad_root_record_name),
                MetadataPath("s1"),
                MetadataPath("s2"),
                MetadataPath("d3"),
                MetadataPath("dataset_0.0"),
                MetadataPath("dataset_0.1")
            ])
        self.assertListEqual(failed, [])

    def test_single_dir(self):
        found, failed = self.tree_search.get_matching_paths(
            pattern_list=["dataset_0.1"],
            recursive=False)

        # Only root and top-level directories should be reported
        self.assertPathsInResult(
            found,
            [
                MetadataPath("dataset_0.1"),
            ])
        self.assertListEqual(failed, [])

    def test_multiple_dir(self):
        found, failed = self.tree_search.get_matching_paths(
            pattern_list=["dataset_0.?"],
            recursive=False)

        # Only root and top-level directories should be reported
        self.assertPathsInResult(
            found,
            [
                MetadataPath("dataset_0.0"),
                MetadataPath("dataset_0.1")
            ])
        self.assertListEqual(failed, [])

    def test_list_root_recursive(self):
        found, failed = self.tree_search.get_matching_paths(
            pattern_list=[""],
            recursive=True)

        self.assertPathsInResult(
            found,
            self.path_list)
        self.assertListEqual(failed, [])

    def test_pattern_1(self):
        self._test_pattern(
            ["*"],
            [
                MetadataPath(datalad_root_record_name),
                MetadataPath("s1"),
                MetadataPath("s2"),
                MetadataPath("d3"),
                MetadataPath("dataset_0.0"),
                MetadataPath("dataset_0.1")
            ],
            self.tree_search)

    def test_root_dataset_recursive(self):
        self._test_pattern(
            [""],
            self.path_list,
            self.tree_search,
            recursive=True)

    def test_pattern_2(self):
        self._test_pattern(
            ["s*"],
            [
                MetadataPath("s1"),
                MetadataPath("s2")
            ],
            self.tree_search)

    def test_pattern_3(self):
        self._test_pattern(
            ["s*/*"],
            [
                MetadataPath("s1/s1.1"),
                MetadataPath("s1/s1.2"),
                MetadataPath("s2/d2.1")
            ],
            self.tree_search)

    def test_pattern_4(self):
        self._test_pattern(
            ["d3/*"],
            [
                MetadataPath("d3") / datalad_root_record_name,
                MetadataPath("d3/some_file")
            ],
            self.tree_search)

    def test_pattern_5(self):
        self._test_pattern(
            ["*/s*"],
            [
                MetadataPath("s1/s1.1"),
                MetadataPath("s1/s1.2"),
                MetadataPath("d3/some_file")
            ],
            self.tree_search)

    def test_pattern_6(self):
        found, failed = self.tree_search.get_matching_paths(["s*/xxx"], False)
        self.assertListEqual(found, [])
        self.assertListEqual(failed, [MetadataPath("s*/xxx")])

    def test_pattern_7(self):
        found, failed = self.tree_search.get_matching_paths(["see"], False)
        self.assertListEqual(found, [])
        self.assertListEqual(failed, [MetadataPath("see")])

    def test_recursive_list_2(self):
        self._test_pattern(
            pattern_list=["d3"],
            expected_matches=[
                MetadataPath("d3") / datalad_root_record_name,
                MetadataPath("d3/some_file")],
            tree_search=self.tree_search,
            recursive=True)


if __name__ == '__main__':
    unittest.main()
