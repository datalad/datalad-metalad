"""
Search through MTreeNode based trees.

This implementation tries to keep memory usage low by:
 - using generators
 - purging MTreeNode-objects, that are not needed anymore

"""
# TODO: unify recursive and non-recursive calls
import enum
import fnmatch
from collections import deque
from dataclasses import dataclass
from typing import (
    Generator,
    Optional,
    Tuple,
    Union,
)

from dataladmetadatamodel.mappableobject import (
    MappableObject,
    ensure_mapped,
)
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.mtreenode import MTreeNode


root_path = MetadataPath("")


@dataclass(frozen=True)
class StackItem:
    item_path: MetadataPath
    item_level: int
    node: Union[MTreeNode, MappableObject]
    needs_purge: bool


@dataclass(frozen=True)
class SearchResult:
    element_path: MetadataPath
    mtree_node: MTreeNode
    possible_contained_element_path: MetadataPath


class TraversalOrder(enum.Enum):
    depth_first_search = 0
    breadth_first_search = 1


class MatchType(enum.Enum):
    full_match = 0
    item_match = 1


class MTreeSearch:
    def __init__(self,
                 mtree: MTreeNode):
        self.mtree = mtree

    def search_pattern(self,
                       pattern: MetadataPath,
                       recursive: bool = False,
                       traversal_order: TraversalOrder = TraversalOrder.depth_first_search,
                       item_indicator: Optional[str] = None,
                       ) -> Generator[Tuple[MetadataPath, MTreeNode, Optional[MetadataPath]], None, None]:

        if recursive is True:
            generator_function = self._search_pattern_recursive
        else:
            generator_function = self._search_pattern
        yield from generator_function(pattern, traversal_order, item_indicator)

    def _search_pattern(self,
                        pattern: MetadataPath,
                        traversal_order: TraversalOrder = TraversalOrder.depth_first_search,
                        item_indicator: Optional[str] = None,
                        ) -> Generator[Tuple[MetadataPath, MTreeNode, Optional[MetadataPath]], None, None]:
        """
        Search the tree und yield nodes that match the pattern.

        Parameters
        ----------
        pattern: file name with shell-style wildcards
        traversal_order: specify whether to use depth-first-order
                         or breadth-first-order in search
        item_indicator: a string that indicates that the current
                        mtree-node is an item in an enclosing context,
                        for example: ".datalad_metadata-root-record"
                        could indicate a dataset-node.

        Returns:
        -------

        Yields a 3-tuple, which describes a full-match, or an item-match.

        A full-match is a tree-node whose path matches the complete pattern.

        An item-match is a tree-node that is an item-node, i.e. it has an item
        indicator as child, which is matched by a prefix of the pattern.
        Item-matches are only generated, when item_indicator is not None.

        In a full-match the first element is the MetadataPath of the matched
        node, the second element is the matched node, and the third
        element is always None.

        In an item match, the first element is the MetadataPath of the
        item-node, the second element is the item node, and the third
        element is a MetadataPath containing the remaining pattern.
        """

        pattern_elements = pattern.parts

        to_process = deque([
            StackItem(
                MetadataPath(""),
                0,
                self.mtree,
                False)])

        while to_process:
            if traversal_order == TraversalOrder.depth_first_search:
                current_item = to_process.pop()
            else:
                current_item = to_process.popleft()

            with ensure_mapped(current_item.node):

                # If the current item level is equal to the number of
                # pattern elements, i.e. all pattern element were matched
                # earlier, the current item is a valid match.
                if len(pattern_elements) == current_item.item_level:
                    yield current_item.item_path, current_item.node, None

                    # There will be no further matches below the
                    # current item, because the pattern elements are
                    # exhausted. Go to the next item.
                    continue

                # Check for item-node, if item indicator is not None
                if item_indicator is not None:
                    if isinstance(current_item.node, MTreeNode):
                        if item_indicator in current_item.node.child_nodes:
                            yield current_item.item_path, current_item.node, MetadataPath(
                                    *pattern_elements[current_item.item_level:])

                # There is at least one more pattern element, try to
                # match it against the current nodes children.
                if not isinstance(current_item.node, MTreeNode):
                    # If the current node has no children, we cannot
                    # match anything and go to the next item
                    continue

                # Check whether the current pattern matches any children,
                # if it does, add the children to `to_process`.
                for child_name, child_mtree in current_item.node.child_nodes.items():
                    if fnmatch.fnmatch(child_name, pattern_elements[current_item.item_level]):
                        # If we have an item indicator, do not append the item
                        # indicator node
                        if item_indicator is None or item_indicator != child_name:
                            to_process.append(
                                StackItem(
                                    current_item.item_path / child_name,
                                    current_item.item_level + 1,
                                    child_mtree,
                                    child_mtree.ensure_mapped()
                                )
                            )

    def _search_pattern_recursive(self,
                                  pattern: MetadataPath,
                                  traversal_order: TraversalOrder = TraversalOrder.depth_first_search,
                                  item_indicator: Optional[str] = None,
                                  ) -> Generator[Tuple[MetadataPath, MTreeNode, Optional[MetadataPath]], None, None]:
        """
        Find nodes that match the given pattern and list all nodes
        recursively from them

        See search_pattern for a description of the parameters and result
        elements
        """
        for result in self._search_pattern(pattern,
                                           traversal_order,
                                           item_indicator):
            if result[2] is not None:
                # Do not recursively list item-matches.
                yield result
            else:
                yield from self._list_recursive(result[0],
                                                result[1],
                                                traversal_order,
                                                item_indicator)

    def _list_recursive(self,
                        start_path: MetadataPath,
                        start_node: MTreeNode,
                        traversal_order: TraversalOrder = TraversalOrder.depth_first_search,
                        item_indicator: Optional[str] = None,
                        ):

        to_process = deque([
            StackItem(
                start_path,
                0,
                start_node,
                False)])

        while to_process:
            if traversal_order == TraversalOrder.depth_first_search:
                current_item = to_process.pop()
            else:
                current_item = to_process.popleft()

            with ensure_mapped(current_item.node):
                # Check for item-node, if item indicator is not None
                if isinstance(current_item.node, MTreeNode):
                    if item_indicator is not None:
                        if item_indicator in current_item.node.child_nodes:
                            yield current_item.item_path, current_item.node, None

                    for child_name, child_node in current_item.node.child_nodes.items():
                        # If we have an item indicator, do not append the item
                        # indicator node
                        if item_indicator is None or item_indicator != child_name:
                            to_process.append(
                                StackItem(
                                    current_item.item_path / child_name,
                                    current_item.item_level + 1,
                                    child_node,
                                    False
                                )
                            )
                else:
                    if item_indicator is None:
                        # If we are at a leaf and there is no item_indicator,
                        # yield the leave.
                        yield current_item.item_path, current_item.node, None
