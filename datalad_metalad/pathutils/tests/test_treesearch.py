import unittest
from typing import Callable, List

from dataladmetadatamodel.datasettree import DatasetTree
from dataladmetadatamodel.filetree import FileTree
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.treenode import TreeNode

from ..treesearch import MatchRecord, TreeSearch


def file_report_matcher(tree_node: TreeNode) -> bool:
    return len(tree_node.child_nodes) == 0


def dataset_report_matcher(tree_node: TreeNode) -> bool:
    return tree_node.value.startswith("test_dataset:")


class TestTreeSearchBase(unittest.TestCase):
    @staticmethod
    def create_tree_search_from_paths(path_list: List[MetadataPath],
                                      report_matcher: Callable
                                      ) -> TreeSearch:
        tree = FileTree("git", "")
        for path in path_list:
            tree.add_file(path)
        return TreeSearch(tree, report_matcher)

    @staticmethod
    def create_dataset_tree_search_from_paths(path_list: List[MetadataPath],
                                              report_matcher: Callable
                                              ) -> TreeSearch:

        tree = DatasetTree("git", "")
        for path in path_list:
            tree.add_dataset(path, f"test_dataset:{path}")
        return TreeSearch(tree, report_matcher)

    def setUp(self) -> None:
        self.file_path_list = [
            MetadataPath(".datalad_metadata"),
            MetadataPath("s1/s1.1/d1.1.1/.datalad_metadata"),
            MetadataPath("s1/s1.2/d1.2.1/.datalad_metadata"),
            MetadataPath("s2/d2.1/.datalad_metadata"),
            MetadataPath("d3/.datalad_metadata"),
            MetadataPath("d3/some_file")]

        self.file_tree_search = self.create_tree_search_from_paths(
            self.file_path_list,
            file_report_matcher)

        self.plain_tree_search = self.create_tree_search_from_paths(
            self.file_path_list,
            lambda x: True)

        self.dataset_path_list = [
            MetadataPath(""),
            MetadataPath("dataset_0.0"),
            MetadataPath("dataset_0.0/dataset_0.0.0"),
            MetadataPath("dataset_0.0/dataset_0.0.1"),
            MetadataPath("dataset_0.0/dataset_0.0.2"),
            MetadataPath("dataset_0.1"),
            MetadataPath("dataset_0.1/dataset_0.1.0"),
            MetadataPath("dataset_0.1/dataset_0.1.1"),
            MetadataPath("dataset_0.1/dataset_0.1.2"),
        ]
        self.dataset_tree_search = self.create_dataset_tree_search_from_paths(
            self.dataset_path_list,
            dataset_report_matcher)

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

    def _test_pattern(self,
                      pattern_list: List[str],
                      expected_matches: List[str],
                      tree_search: TreeSearch,
                      recursive: bool = False,
                      auto_list_dirs: bool = True):

        found, failed = tree_search.get_matching_paths(
            pattern_list=pattern_list,
            recursive=recursive,
            auto_list_dirs=auto_list_dirs)
        self.assertPathsInResult(found, expected_matches)
        self.assertListEqual(failed, [])

    def test_auto_list_dirs_on(self):
        found, failed = self.file_tree_search.get_matching_paths(
            pattern_list=[""],
            recursive=False,
            auto_list_dirs=True)

        # Only top-level files should be reported
        self.assertPathsInResult(found, [MetadataPath(".datalad_metadata")])
        self.assertListEqual(failed, [])

    def test_auto_list_dirs_on_recursive(self):
        found, failed = self.file_tree_search.get_matching_paths(
            pattern_list=[""],
            recursive=True,
            auto_list_dirs=True)

        self.assertPathsInResult(
            found,
            [
                MetadataPath(".datalad_metadata"),
                MetadataPath("s1/s1.1/d1.1.1/.datalad_metadata"),
                MetadataPath("s1/s1.2/d1.2.1/.datalad_metadata"),
                MetadataPath("s2/d2.1/.datalad_metadata"),
                MetadataPath("d3/.datalad_metadata"),
                MetadataPath("d3/some_file")
            ])

        self.assertListEqual(failed, [])

    def test_auto_list_dirs_on_recursive_plain(self):
        found, failed = self.plain_tree_search.get_matching_paths(
            pattern_list=[""],
            recursive=True,
            auto_list_dirs=True)

        self.assertPathsInResult(
            found,
            [
                MetadataPath(".datalad_metadata"),
                MetadataPath("s1"),
                MetadataPath("s1/s1.1"),
                MetadataPath("s1/s1.1/d1.1.1"),
                MetadataPath("s1/s1.2"),
                MetadataPath("s1/s1.2/d1.2.1"),
                MetadataPath("s1/s1.1/d1.1.1/.datalad_metadata"),
                MetadataPath("s1/s1.2/d1.2.1/.datalad_metadata"),
                MetadataPath("s2"),
                MetadataPath("s2/d2.1"),
                MetadataPath("s2/d2.1/.datalad_metadata"),
                MetadataPath("d3"),
                MetadataPath("d3/.datalad_metadata"),
                MetadataPath("d3/some_file")
            ])

        self.assertListEqual(failed, [])

    def test_auto_list_dirs_off(self):
        # Expect no records, since only root is specified and is not reported
        found, failed = self.file_tree_search.get_matching_paths(
            [""],
            False,
            auto_list_dirs=False)

        self.assertListEqual(found, [])
        self.assertListEqual(failed, [])

    def test_root_dataset(self):
        self._test_pattern(
            [""],
            [
                MetadataPath(""),
            ],
            self.dataset_tree_search,
            auto_list_dirs=False)

    def test_autolist_dirs_dataset_on(self):
        self._test_pattern(
            [""],
            [
                MetadataPath("dataset_0.0"),
                MetadataPath("dataset_0.1")
            ],
            self.dataset_tree_search,
            auto_list_dirs=True)

    def test_pattern_1(self):
        self._test_pattern(
            ["*"],
            [MetadataPath(".datalad_metadata"),
                MetadataPath("s1"),
                MetadataPath("s2"),
                MetadataPath("d3")],
            self.plain_tree_search)

    def test_root_dataset_recursive(self):
        self._test_pattern(
            [""],
            [
                MetadataPath(""),
                MetadataPath("dataset_0.0"),
                MetadataPath("dataset_0.0/dataset_0.0.0"),
                MetadataPath("dataset_0.0/dataset_0.0.1"),
                MetadataPath("dataset_0.0/dataset_0.0.2"),
                MetadataPath("dataset_0.1"),
                MetadataPath("dataset_0.1/dataset_0.1.0"),
                MetadataPath("dataset_0.1/dataset_0.1.1"),
                MetadataPath("dataset_0.1/dataset_0.1.2"),
            ],
            self.dataset_tree_search,
            recursive=True,
            auto_list_dirs=False)

    def test_pattern_2(self):
        self._test_pattern(
            ["s*"],
            [
                MetadataPath("s1"),
                MetadataPath("s2")
            ],
            self.plain_tree_search)

    def test_pattern_3(self):
        self._test_pattern(
            ["s*/*"],
            [
                MetadataPath("s1/s1.1"),
                MetadataPath("s1/s1.2"),
                MetadataPath("s2/d2.1")
            ],
            self.plain_tree_search)

    def test_pattern_4(self):
        self._test_pattern(
            ["d3/*"],
            [
                MetadataPath("d3/.datalad_metadata"),
                MetadataPath("d3/some_file")
            ],
            self.file_tree_search)

    def test_pattern_5(self):
        self._test_pattern(
            ["*/s*"],
            [
                MetadataPath("s1/s1.1"),
                MetadataPath("s1/s1.2"),
                MetadataPath("d3/some_file")
            ],
            self.plain_tree_search)

    def test_pattern_6(self):
        found, failed = self.file_tree_search.get_matching_paths(["s*/xxx"], False)
        self.assertListEqual(found, [])
        self.assertListEqual(failed, [MetadataPath("s*/xxx")])

    def test_pattern_7(self):
        found, failed = self.file_tree_search.get_matching_paths(["see"], False)
        self.assertListEqual(found, [])
        self.assertListEqual(failed, [MetadataPath("see")])

    def test_recursive_list_1(self):
        self._test_pattern(
            pattern_list=[""],
            expected_matches=self.file_path_list,
            tree_search=self.file_tree_search,
            recursive=True,
            auto_list_dirs=False)

    def test_recursive_list_2(self):
        self._test_pattern(
            pattern_list=["d3"],
            expected_matches=[
                MetadataPath("d3/.datalad_metadata"),
                MetadataPath("d3/some_file")],
            tree_search=self.file_tree_search,
            recursive=True,
            auto_list_dirs=False)


if __name__ == '__main__':
    unittest.main()
