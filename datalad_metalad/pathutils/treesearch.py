from fnmatch import fnmatchcase
from typing import List, Tuple, Union

import dataclasses

from dataladmetadatamodel.datasettree import DatasetTree
from dataladmetadatamodel.filetree import FileTree
from dataladmetadatamodel.metadatapath import MetadataPath


@dataclasses.dataclass
class MatchRecord:
    path: MetadataPath
    node: FileTree

    def __eq__(self, other) -> bool:
        return (
            self.path == other.path
            and self.node == other.node)


class TreeSearch:
    """
    Search through FileTrees. File Tree elements
    are always separated by "/". The root is
    identified by an empty string, i.e. "".
    """
    def __init__(self, tree: Union[DatasetTree, FileTree]):
        self.tree = tree

    def get_matching_paths(self,
                           pattern_list: List[str],
                           recursive: bool,
                           auto_list_root: bool = True
                           ) -> Tuple[List[MatchRecord], List[MetadataPath]]:
        """
        Get all metadata paths that are matching the patterns in
        pattern_list.

        - Leading "/" are removed from patterns, since metadata
          paths are not absolute.

        - Empty pattern-specifications, i.e. '', are interpreted
          as root-dataset or root-file-tree nodes.
        """
        pattern_elements_list = [
            MetadataPath(pattern)
            for pattern in set(pattern_list)
        ]
        matching, failed = self._get_matching_nodes(
            pattern_elements_list,
            auto_list_root)

        if recursive:
            matching = self._list_recursive(matching[:])
        return matching, failed

    def _get_matching_nodes(self,
                            pattern_list: List[MetadataPath],
                            auto_list_root: bool
                            ) -> Tuple[List[MatchRecord], List[MetadataPath]]:

        match_records: List[MatchRecord] = []
        failed_patterns: List[MetadataPath] = []

        for pattern in pattern_list:
            if pattern.parts in (("*",), ("",)):

                # TODO: fix this case and combine with the next
                # Special cases in which the root node is added
                match_records.extend(self._get_root_nodes(auto_list_root))

            elif pattern.parts == ():
                match_records.extend(self._get_root_nodes(auto_list_root))

            else:
                matching_path_records = self._search_matches(
                    pattern.parts,
                    self.tree,
                    MetadataPath(""))

                if matching_path_records:
                    match_records.extend(matching_path_records)
                else:
                    failed_patterns.append(pattern)

        return match_records, failed_patterns

    def _get_root_nodes(self,
                        auto_list_root: bool
                        ) -> List[MatchRecord]:
        return (
            [
                MatchRecord(MetadataPath(name), child_node)
                for name, child_node in self.tree.child_nodes.items()
            ]
            if auto_list_root is True
            else [MatchRecord(MetadataPath(""), self.tree)])

    def _search_matches(self,
                        pattern_parts: Tuple[str],
                        tree: FileTree,
                        accumulated_path: MetadataPath
                        ) -> List[MatchRecord]:

        if not pattern_parts:
            return [MatchRecord(MetadataPath(accumulated_path), tree)]

        match_records = []
        for name, sub_tree in tree.child_nodes.items():
            if fnmatchcase(name, pattern_parts[0]):
                match_records.extend(
                    self._search_matches(
                        pattern_parts[1:],
                        sub_tree,
                        accumulated_path / name))

        return match_records

    def _list_recursive(self,
                        starting_points: List[MatchRecord]
                        ) -> List[MatchRecord]:

        return [
            record
            for starting_point in starting_points
            for record in self._rec_list_recursive(
                starting_point.node,
                starting_point.path)]

    def _rec_list_recursive(self,
                            starting_point: FileTree,
                            starting_point_path: MetadataPath
                            ) -> List[MatchRecord]:

        if starting_point.is_leaf_node():
            return [MatchRecord(starting_point_path, starting_point)]

        result = []
        for node_name, sub_tree in starting_point.child_nodes.items():
            sub_tree_path = starting_point_path / node_name
            result.extend(
                self._rec_list_recursive(sub_tree, sub_tree_path))

        return result
