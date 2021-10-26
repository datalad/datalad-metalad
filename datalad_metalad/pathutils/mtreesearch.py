"""
Search through mtrees. Either for leave nodes
or for nodes with a specific child.

Try to keep memory low:
 use generators
 unmap what is not needed anymore
"""
import fnmatch
from dataclasses import dataclass
from typing import (
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

from dataladmetadatamodel.mappableobject import MappableObject
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.mtreenode import MTreeNode


root_path = MetadataPath("")


@dataclass(frozen=True)
class StackItem:
    item_path: MetadataPath
    item_level: int
    node: Union[MTreeNode, MappableObject]
    needs_purge: bool


class MTreeSearch:
    def __init__(self,
                 mtree: MTreeNode):
        self.mtree = mtree

    def search_pattern(self,
                       pattern: MetadataPath,
                       recursive: bool = False,
                       required_child: Optional[str] = None,
                       ) -> Generator:
        """
        Search the tree. If required child is None,
        return leaves that match the pattern or are
        reached by recursion from a matching pattern.
        If required child is not None, return nodes
        that match a pattern and contain the required
        child, are are reached by recursion from a
        matching pattern and contain the required child.

        Parameters
        ----------
        pattern
        recursive
        required_child

        Returns
        -------

        """

        pattern_elements = pattern.parts

        to_process = [
            StackItem(
                MetadataPath(""),
                0,
                self.mtree,
                self.mtree.ensure_mapped())]

        while to_process:
            current_item = to_process.pop()

            # If the current item level is equal to the number of
            # pattern elements, i.e. all pattern element were matched
            # earlier, the current item is a valid match.
            if len(pattern_elements) == current_item.item_level:
                yield current_item.item_path, current_item.node

                # There will be no further matches below the
                # current item, because the pattern elements are
                # exhausted. Go to the next item.
                continue

            # There is at least one more pattern element, try to
            # match it against the current nodes children.
            if not isinstance(current_item.node, MTreeNode):
                # If the current node has no children, we cannot
                # match anything and go to the next item
                continue

            # Check whether the current pattern matches any children,
            # if it does, add the children to `to_process`.
            for child_name in current_item.node.child_nodes.keys():
                if fnmatch.fnmatch(child_name, pattern_elements[current_item.item_level]):
                    child_mtree = current_item.node.get_child(child_name)
                    to_process.append(
                        StackItem(
                            current_item.item_path / child_name,
                            current_item.item_level + 1,
                            child_mtree,
                            child_mtree.ensure_mapped()
                        )
                    )

            # We are done with this node. Purge it, if it was
            # not present in memory before this search.
            if current_item.needs_purge:
                current_item.node.purge()
