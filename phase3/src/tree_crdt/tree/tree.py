from .node import Node


class Tree:
  """The tree data structure underlying the Tree CRDT.

  Phase 3 reverts to the single-valued tree of Phase 1: each child ID
  maps to at most one `Node`. The multi-value semantics of Phase 2
  (and the associated Move-Wins resolution) are no longer needed
  because the Phase 3 total order on vector timestamps disambiguates
  concurrent operations on the same child at the `Replica` layer
  before they reach the `Tree`.

  See the Phase 3 PDF, sections "Reverting to a Single-Valued Tree"
  and "The Tree Class", for the full contracts.

  The choice of how to STORE the child-to-Node association internally
  is yours: a dict mapping child IDs to Nodes is the natural choice,
  but anything that meets the method contracts below is acceptable.
  The EXTERNAL return types of `__call__`, `__getitem__`, and
  `get_active` ARE constrained (they are part of the test-facing API).
  """

  def __init__(self):
    """Construct an empty tree."""
    # TODO: initialise an empty container for the child-to-Node mapping.
    raise NotImplementedError("TODO: implement Tree.__init__")

  def __call__(self, deleted=False):
    """Return the current tree state as a `set[Node]`.

    If `deleted` is False (default), nodes whose `status` is "deleted"
    are filtered out. If `deleted` is True, they are included.
    """
    # TODO
    raise NotImplementedError("TODO: implement Tree.__call__")

  def __getitem__(self, key):
    """Return the `Node` associated with `key`, or `None` if unknown.

    Returns a single Node (not a set) because the tree is
    single-valued in Phase 3.
    """
    # TODO
    raise NotImplementedError("TODO: implement Tree.__getitem__")

  def get_active(self, key):
    """Return the `Node` at `key` iff it is reachable from the root and not deleted.

    Otherwise return None. "Reachable" means the node's parent chain
    eventually reaches a node with `parent is None` without cycling
    and without dangling. Implementations can use a simple
    parent-pointer walk; the multi-value BFS of Phase 2 is no longer
    necessary.
    """
    # TODO
    raise NotImplementedError("TODO: implement Tree.get_active")

  def move(self, parent, metadata, child):
    """Apply a Move (or Delete, depending on metadata["status"]).

    The Phase 3 contract is simpler than Phase 2's: there is no
    producer-ID or producer-timestamp argument, and no Move-Wins
    resolution block. Concurrent Move/Delete pairs on the same child
    are ordered by the total order at the Replica layer, so by the
    time this method is called the operation is already in its final
    position and either overwrites the existing version or is a
    no-op.

    Required behaviour:

      - If `metadata["status"] == "deleted"`, this is a Delete: the
        existing node at `child` (if any) is marked deleted by
        replacing it with a Node carrying the new metadata.
      - Otherwise it is a Move: a new Node is inserted at `child`.
        Reject the move if it would create a cycle (a node cannot be
        moved to its own descendant) or if it would point a node at
        itself.
      - "Status" is required in `metadata`; if it is missing, the
        method should be a no-op.
    """
    # TODO
    raise NotImplementedError("TODO: implement Tree.move")

  def __str__(self):
    # TODO
    raise NotImplementedError("TODO: implement Tree.__str__")
