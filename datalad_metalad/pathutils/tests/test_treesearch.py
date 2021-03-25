import unittest
from typing import List

from dataladmetadatamodel.datasettree import DatasetTree
from dataladmetadatamodel.filetree import FileTree
from dataladmetadatamodel.metadatapath import MetadataPath

from ..treesearch import MatchRecord, TreeSearch


class TestTreeSearchBase(unittest.TestCase):
    @staticmethod
    def create_tree_search_from_paths(path_list: List[MetadataPath]
                                      ) -> TreeSearch:

        tree = FileTree("", "")
        for path in path_list:
            tree.add_file(path)
        return TreeSearch(tree)

    @staticmethod
    def create_dataset_tree_search_from_paths(path_list: List[MetadataPath]
                                              ) -> TreeSearch:

        tree = DatasetTree("", "")
        for path in path_list:
            tree.add_dataset(path, f"test:{path}")
        return TreeSearch(tree)

    def setUp(self) -> None:
        self.path_list = [
            MetadataPath(".datalad_metadata"),
            MetadataPath("s1/s1.1/d1.1.1/.datalad_metadata"),
            MetadataPath("s1/s1.2/d1.2.1/.datalad_metadata"),
            MetadataPath("s2/d2.1/.datalad_metadata"),
            MetadataPath("d3/.datalad_metadata"),
            MetadataPath("d3/some_file")]
        self.tree_search = self.create_tree_search_from_paths(self.path_list)

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
            self.dataset_path_list)

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
                      use_dataset_tree: bool = False):

        tree_search = (
            self.dataset_tree_search
            if use_dataset_tree is True
            else self.tree_search
        )

        found, failed = tree_search.get_matching_paths(pattern_list, False)
        self.assertPathsInResult(found, expected_matches)
        self.assertListEqual(failed, [])

    def _test_pattern_rec(self, pattern_list: List[str], expected_matches: List[str]):
        found, failed = self.tree_search.get_matching_paths(pattern_list, True)
        self.assertPathsInResult(found, expected_matches)
        self.assertListEqual(failed, [])

    def test_auto_list_root_on(self):
        found, failed = self.tree_search.get_matching_paths(
            [""],
            False,
            auto_list_root=True)

        self.assertPathsInResult(
            found,
            [
                MetadataPath(".datalad_metadata"),
                MetadataPath("s1"),
                MetadataPath("s2"),
                MetadataPath("d3")])

        self.assertListEqual(failed, [])

    def test_auto_list_root_off(self):
        """ Expect a single root record for non-autolist root search """
        found, failed = self.tree_search.get_matching_paths(
            [""],
            False,
            auto_list_root=False)

        self.assertListEqual(
            found,
            [MatchRecord(MetadataPath(""), self.tree_search.tree)])
        self.assertListEqual(failed, [])

    def test_root_dataset(self):
        self._test_pattern(
            ["*"],
            [
                MetadataPath(""),
                MetadataPath("dataset_0.0"),
                MetadataPath("dataset_0.1")
            ],
            use_dataset_tree=True)

    def test_pattern_1(self):
        self._test_pattern(
            ["*"],
            [
                MetadataPath(".datalad_metadata"),
                MetadataPath("s1"),
                MetadataPath("s2"),
                MetadataPath("d3")])

    def test_pattern_2(self):
        self._test_pattern(["s*"], [MetadataPath("s1"), MetadataPath("s2")])

    def test_pattern_3(self):
        self._test_pattern(
            ["s*/*"],
            [
                MetadataPath("s1/s1.1"),
                MetadataPath("s1/s1.2"),
                MetadataPath("s2/d2.1")])

    def test_pattern_4(self):
        self._test_pattern(
            ["d3/*"],
            [
                MetadataPath("d3/.datalad_metadata"),
                MetadataPath("d3/some_file")])

    def test_pattern_5(self):
        self._test_pattern(
            ["*/s*"],
            [
                MetadataPath("s1/s1.1"),
                MetadataPath("s1/s1.2"),
                MetadataPath("d3/some_file")])

    def test_pattern_6(self):
        found, failed = self.tree_search.get_matching_paths(["s*/xxx"], False)
        self.assertListEqual(found, [])
        self.assertListEqual(failed, [MetadataPath("s*/xxx")])

    def test_pattern_7(self):
        found, failed = self.tree_search.get_matching_paths(["see"], False)
        self.assertListEqual(found, [])
        self.assertListEqual(failed, [MetadataPath("see")])

    def test_recursive_list_1(self):
        self._test_pattern_rec([""], self.path_list)

    def test_recursive_list_2(self):
        self._test_pattern_rec(
            ["d3"],
            [
                MetadataPath("d3/.datalad_metadata"),
                MetadataPath("d3/some_file")])


if __name__ == '__main__':
    unittest.main()
