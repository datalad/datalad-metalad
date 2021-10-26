import unittest
from typing import (
    Any,
    List,
)

from dataladmetadatamodel.datasettree import datalad_root_record_name
from dataladmetadatamodel.metadata import Metadata
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.mtreenode import MTreeNode

from ..mtreesearch import MTreeSearch


class TestMTreeSearchBase(unittest.TestCase):
    @staticmethod
    def create_mtree_search_from_paths(path_list: List[MetadataPath],
                                       leaf_class: Any
                                       ) -> MTreeSearch:
        tree = MTreeNode(leaf_class)
        for path in path_list:
            tree.add_child_at(leaf_class(), path)
        return MTreeSearch(tree)

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
        self.mtree_search = self.create_mtree_search_from_paths(
            self.path_list,
            Metadata)


class TestTreeSearchMatching(TestMTreeSearchBase):
    def test_root(self):
        result = list(
            self.mtree_search.search_pattern(
                pattern=MetadataPath("")))
        self.assertEqual(
            result[0],
            (MetadataPath(""), self.mtree_search.mtree))

    def test_globbing(self):
        results = list(
            self.mtree_search.search_pattern(
                pattern=MetadataPath("*/dataset_0*")))
        for expected_path in [
                MetadataPath("dataset_0.0/dataset_0.0.0"),
                MetadataPath("dataset_0.0/dataset_0.0.1"),
                MetadataPath("dataset_0.0/dataset_0.0.2"),
                MetadataPath("dataset_0.1/dataset_0.1.0"),
                MetadataPath("dataset_0.1/dataset_0.1.1"),
                MetadataPath("dataset_0.1/dataset_0.1.2")]:
            self.assertIn(expected_path, [result[0] for result in results])
