import dataclasses
from fnmatch import fnmatchcase
from typing import Any, List, Tuple

from dataladmetadatamodel.mappableobject import MappableObject
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.mtreenode import MTreeNode


@dataclasses.dataclass
class MatchRecord:
    path: MetadataPath
    node: MappableObject

    def __eq__(self, other) -> bool:
        return (
            self.path == other.path
            and self.node == other.node)


class TreeSearch:
    """
    Search through MTrees. Tree elements
    are always separated by "/". The root is
    identified by an empty string, i.e. "".
    """
    def __init__(self,
                 tree: MTreeNode
                 ):
        assert isinstance(tree, MTreeNode)
        self.tree = tree
        self.mapped_objects = []

    def get_matching_paths(self,
                           pattern_list: List[str],
                           recursive: bool
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
        matching, failed = self._get_matching_nodes(pattern_elements_list)

        if recursive:
            matching = self._list_recursive(matching[:])
        return matching, failed

    def _get_matching_nodes(self,
                            pattern_list: List[MetadataPath],
                            ) -> Tuple[List[MatchRecord], List[MetadataPath]]:

        match_records: List[MatchRecord] = []
        failed_patterns: List[MetadataPath] = []

        for pattern in pattern_list:
            if pattern.parts == ():
                match_records.extend(self._get_root_nodes())
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

    def _get_root_nodes(self) -> List[MatchRecord]:
        return [MatchRecord(MetadataPath(""), self.tree)]

    def _search_matches(self,
                        pattern_parts: Tuple[str],
                        tree: MTreeNode,
                        accumulated_path: MetadataPath
                        ) -> List[MatchRecord]:

        if not isinstance(tree, MTreeNode):
            if not pattern_parts:
                tree.ensure_mapped()
                return [MatchRecord(MetadataPath(accumulated_path), tree)]
            return []

        if tree.ensure_mapped():
            self.mapped_objects.append(tree)

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

        record_list = [
            record
            for starting_point in starting_points
            for record in self._rec_list_recursive(
                starting_point.node,
                starting_point.path)]

        # remove duplicates which might stem from
        # starting points that are contained in other
        # starting points, e.g. "a/d" is contained in "a".
        result = []
        name_set = set()
        for record in record_list:
            if str(record.path) not in name_set:
                name_set.add(str(record.path))
                result.append(record)
        return result

    def _rec_list_recursive(self,
                            starting_point: Any,
                            starting_point_path: MetadataPath
                            ) -> List[MatchRecord]:

        if starting_point is None:
            return []

        if starting_point.ensure_mapped():
            self.mapped_objects.append(starting_point)

        if not isinstance(starting_point, MTreeNode):
            return [MatchRecord(starting_point_path, starting_point)]

        result = [
            MatchRecord(starting_point_path / path, mappable_object)
            for path, mappable_object in starting_point.get_paths_recursive()
        ]
        return result

    def purge_mapped_objects(self):
        for obj in self.mapped_objects:
            obj.purge()
