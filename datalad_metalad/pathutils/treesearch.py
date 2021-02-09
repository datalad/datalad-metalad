from fnmatch import fnmatchcase
from typing import List, Tuple

import dataclasses

from dataladmetadatamodel.filetree import FileTree


@dataclasses.dataclass
class MatchRecord:
    path: str
    node: FileTree

    def __eq__(self, other) -> bool:
        return (
            self.path == other.path
            and self.node == other.node)


class TreeSearch:
    """
    Search through FileTrees. File Tree elements
    are always separated by "/". The root is
    identified by an empty string, and -for now-
    also by "/".
    """
    def __init__(self, file_tree: FileTree):
        self.file_tree = file_tree

    def get_matching_paths(self,
                           pattern_list: List[str],
                           recursive: bool,
                           auto_list_root: bool = True
                           ) -> Tuple[List[MatchRecord], List[str]]:
        """
        Get all paths that are matching the patterns in
        pattern_list.

        - Leading "/" are removed from paths.

        - Empty paths specifications are interpreted as
          root-dataset or root-file-tree nodes.
        """
        path_elements_list = [
            pattern.lstrip("/").split("/")
            for pattern in set(pattern_list)
        ]
        matching, failed = self._get_matching_nodes(
            path_elements_list,
            auto_list_root)

        if recursive:
            matching = self._list_recursive(matching[:])
        return matching, failed

    def _get_matching_nodes(self,
                            path_elements_list: List[List[str]],
                            auto_list_root: bool
                            ) -> Tuple[List[MatchRecord], List[str]]:

        match_records: List[MatchRecord] = []
        failed_patterns: List[str] = []

        for path_elements in path_elements_list:
            if path_elements == [""]:
                match_records.extend(self._get_root_nodes(auto_list_root))

            else:
                path_matches = self._search_matches(path_elements, self.file_tree, "")
                if path_matches:
                    match_records.extend(path_matches)
                else:
                    failed_patterns.append("/".join(path_elements))

        return match_records, failed_patterns

    def _get_root_nodes(self,
                        auto_list_root: bool
                        ) -> List[MatchRecord]:
        return (
            [
                MatchRecord(name, child_node)
                for name, child_node in self.file_tree.child_nodes.items()
            ]
            if auto_list_root is True
            else [MatchRecord("", self.file_tree.child_nodes[""])])  # TODO: the root node should be the root node

    def _search_matches(self,
                        path_elements: List[str],
                        tree: FileTree,
                        path_name: str
                        ) -> List[MatchRecord]:

        if not path_elements:
            return [MatchRecord(path_name, tree)]

        match_records = []
        for name, sub_tree in tree.child_nodes.items():
            if fnmatchcase(name, path_elements[0]):
                match_records.extend(
                    self._search_matches(
                        path_elements[1:],
                        sub_tree,
                        self._join(path_name, name)))

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
                            starting_point_path: str
                            ) -> List[MatchRecord]:

        if starting_point.is_leaf_node():
            return [MatchRecord(starting_point_path, starting_point)]

        result = []
        for node_name, sub_tree in starting_point.child_nodes.items():
            sub_tree_path = starting_point_path + "/" + node_name
            result.extend(
                self._rec_list_recursive(sub_tree, sub_tree_path))

        return result

    @staticmethod
    def _join(*paths):
        result = ""
        for path in paths:
            if path.startswith("/"):
                result = path
            else:
                result = result.rstrip("/") + ("/" if result else "") + path.rstrip("/")
        return result
