from .node import Node


class _NodeSet(set):
    def __repr__(self):
        if not self:
            return "set()"
        return "{" + ",\n ".join(str(n) for n in sorted(self)) + "}"


class Tree:
    def __init__(self):
        self.__nodes: dict = {}  # child_id -> Node

    def __call__(self) -> set:
        return _NodeSet(self.__nodes.values())

    def __getitem__(self, key) -> Node | None:
        return self.__nodes.get(key, None)

    def _is_ancestor(self, ancestor_id, node_id) -> bool:
        """Returns True if ancestor_id is an ancestor of node_id in the current tree.
            This method will be used to detect cycles in move operation."""
        current_id = node_id
        while current_id is not None:
            node = self.__nodes.get(current_id)
            if node is None:
                break
            current_id = node.parent
            if current_id == ancestor_id:
                return True
        return False

    def move(self, new_node: Node) -> None:
        child_id = new_node.child
        new_parent_id = new_node.parent
        existing = self.__nodes.get(child_id) # true if this node already exists in the tree

        if existing is None:
            self.__nodes[child_id] = new_node
            return

        # ignore the operation if the new parent is a descendant of the child (cycle!)
        if new_parent_id is not None and (
            new_parent_id == child_id or self._is_ancestor(child_id, new_parent_id)
        ):
            return

        self.__nodes[child_id] = new_node

    def __str__(self) -> str:
        return str(self())