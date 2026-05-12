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
        
        """
        ERROR 1 CONTINUED: Due to ERROR 1 mentioned, if the current tree had a cycle, is_ancestor was looping forever.
        We fixed ERROR 1, but just to be sure, we also created a 'visited set' inside this function for additional cycle check.
        """

        visited = set()
        current_id = node_id
        while current_id is not None:
            if current_id in visited:
                break  # cycle!!!
            visited.add(current_id)
            node = self.__nodes.get(current_id)
            if node is None:
                break
            current_id = node.parent
            if current_id == ancestor_id:
                return True
        return False

    def can_move(self, new_node: Node) -> bool:
        child_id = new_node.child
        new_parent_id = new_node.parent

        if new_parent_id is not None and (
            new_parent_id == child_id or self._is_ancestor(child_id, new_parent_id)
        ):
            return False

        return True

    def move(self, new_node: Node) -> None:
        if not self.can_move(new_node):
            return

        child_id = new_node.child
        self.__nodes[child_id] = new_node

    def __str__(self) -> str:
        return str(self())
