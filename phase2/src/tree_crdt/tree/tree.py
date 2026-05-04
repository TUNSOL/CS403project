from copy import deepcopy

from ..clock import VectorClock
from .node import Node


class Tree:
  """The tree data structure underlying the Tree CRDT.

  In Phase 2, each child ID is associated with a set of versions of that
  node, rather than a single triple as in Phase 1. Each version is a
  Node carrying the (i, t, p, m, c) quintuple from the operation that
  produced it. Under a Lamport clock that set will contain at most one
  element, so the Phase 1 behaviour is recovered as a degenerate
  special case.

  The choice of how to STORE this association internally is yours,
  exactly as in Phase 1: a dictionary mapping IDs to sets, a list of
  Node objects you index on demand, or any other container is
  acceptable, as long as the methods below behave according to their
  contracts. Note that the EXTERNAL return types of __call__ and
  __getitem__ ARE constrained (they are part of the test-facing API);
  see the Phase 2 PDF, Section "The Tree Class", for the full contract.
  """

  def __init__(self):
    """Construct an empty tree."""
    self.__nodes: dict[int, set[Node]] = {}

  def __call__(self, deleted: bool = False):
    """Return the current tree state as a list of frozensets, sorted by child ID.

    If `deleted` is False (default), versions whose status is "deleted"
    are filtered out of each frozenset. If `deleted` is True, they are
    included. frozenset is used (rather than set) because the caller may
    need the result to be hashable.
    """
    result = []
    for key in sorted(self.__nodes):
      versions = self.__nodes[key] if deleted else self.get_active(key)
      result.append(frozenset(versions))
    return result

  def __getitem__(self, key):
    """Return the set of versions associated with `key`, or the empty set if unknown.

    The return type is a set[Node]; if you store versions internally in
    a different container, convert on the fly here.
    """
    return set(self.__nodes.get(key, set()))

  def __iter__(self):
    """Iterate over the child IDs known to the tree."""
    return iter(sorted(self.__nodes))

  @staticmethod
  def __is_deleted_metadata(metadata: dict) -> bool:
    return metadata.get("status", "active") == "deleted"

  @staticmethod
  def __is_deleted_node(node: Node) -> bool:
    return Tree.__is_deleted_metadata(node.metadata)

  @staticmethod
  def __normalize_timestamp(timestamp):
    return VectorClock._normalize_timestamp(timestamp)

  @staticmethod
  def __timestamp_lt(lhs, rhs, lhs_replica=None, rhs_replica=None) -> bool:
    lhs = Tree.__normalize_timestamp(lhs)
    rhs = Tree.__normalize_timestamp(rhs)

    if isinstance(lhs, int) and isinstance(rhs, int):
      return (lhs, lhs_replica if lhs_replica is not None else -1) < (
        rhs,
        rhs_replica if rhs_replica is not None else -1,
      )

    return VectorClock.timestamp_lt(lhs, rhs)

  @staticmethod
  def __timestamp_le(lhs, rhs, lhs_replica=None, rhs_replica=None) -> bool:
    lhs = Tree.__normalize_timestamp(lhs)
    rhs = Tree.__normalize_timestamp(rhs)

    if isinstance(lhs, int) and isinstance(rhs, int):
      return (lhs, lhs_replica if lhs_replica is not None else -1) <= (
        rhs,
        rhs_replica if rhs_replica is not None else -1,
      )

    return VectorClock.timestamp_le(lhs, rhs)

  @staticmethod
  def __timestamp_concurrent(lhs, rhs) -> bool:
    return VectorClock.timestamp_concurrent(
      Tree.__normalize_timestamp(lhs),
      Tree.__normalize_timestamp(rhs),
    )

  def __has_path_to(self, start_id, target_id) -> bool:
    if start_id is None:
      return False

    stack = [start_id]
    visited = set()

    while stack:
      current = stack.pop()
      if current == target_id:
        return True
      if current in visited:
        continue
      visited.add(current)

      for version in self.__nodes.get(current, set()):
        if version.parent is not None:
          stack.append(version.parent)

    return False

  def get_active(self, key):
    """Return the subset of `[key]` consisting of versions that are alive.

    A version is alive iff it is not a tombstone (status != "deleted")
    AND it is not orphaned. A version is orphaned if it has no path to
    the root through ancestors whose multi-value sets contain at least
    one alive version. See the PDF section "Orphaned Nodes" for details.
    """
    memo: dict[Node, bool] = {}
    visiting: set[Node] = set()

    def has_live_root_path(version: Node) -> bool:
      if self.__is_deleted_node(version):
        return False

      if version in memo:
        return memo[version]

      if version in visiting:
        return False

      if version.parent is None:
        memo[version] = True
        return True

      parent_versions = self.__nodes.get(version.parent, set())
      if not parent_versions:
        memo[version] = False
        return False

      visiting.add(version)
      alive = any(has_live_root_path(parent_version) for parent_version in parent_versions)
      visiting.remove(version)
      memo[version] = alive
      return alive

    return {version for version in self.__nodes.get(key, set()) if has_live_root_path(version)}

  def move(self, replica_id=None, timestamp=None, parent=None, metadata=None, child=None):
    """Apply the Move operation (i, t, p, m, c) to the tree.

    The required behaviour is:

      1. Reject the operation if it would create a cycle (child == parent,
         or `parent` is currently a descendant of `child` through any
         version in any multi-value set on the path; use a DFS).

      2. Otherwise, build the candidate Node and resolve it pairwise
         against every existing version in the multi-value set for `child`:

         - Vector (both timestamps are dict): pairwise compare with
           VectorClock.timestamp_le / timestamp_lt / timestamp_concurrent; the
           older version is removed, the newer-existing version causes
           the candidate to be discarded, and concurrent versions trigger
           Move-Wins (see PDF Section "Multi-Value Tree State and the
           Move-Wins Concurrency Semantics"):

             * incoming Delete vs. existing alive  --> discard candidate
             * incoming alive  vs. existing Delete --> remove existing

      3. Remove the marked-for-removal versions, add the candidate
         (unless discarded), write the updated multi-value set back.
    """
    if isinstance(replica_id, Node):
      values = replica_id()
      if len(values) == 5:
        replica_id, timestamp, parent, metadata, child = values
      else:
        parent, metadata, child = values
        replica_id = -1
        timestamp = 0

    if metadata is None:
      metadata = {}

    metadata = deepcopy(metadata)
    metadata.setdefault("status", "active")
    timestamp = self.__normalize_timestamp(deepcopy(timestamp))

    if child == parent:
      return None

    if parent is not None and self.__has_path_to(parent, child):
      return None

    candidate = Node(i=replica_id, t=timestamp, p=parent, m=metadata, c=child)
    versions = set(self.__nodes.get(child, set()))
    to_remove: set[Node] = set()
    discard_candidate = False

    for existing in versions:
      if self.__timestamp_lt(
        existing.timestamp,
        candidate.timestamp,
        existing.replica_id,
        candidate.replica_id,
      ):
        to_remove.add(existing)
        continue

      if self.__timestamp_le(
        candidate.timestamp,
        existing.timestamp,
        candidate.replica_id,
        existing.replica_id,
      ):
        discard_candidate = True
        break

      if self.__timestamp_concurrent(existing.timestamp, candidate.timestamp):
        candidate_deleted = self.__is_deleted_node(candidate)
        existing_deleted = self.__is_deleted_node(existing)

        if candidate_deleted and not existing_deleted:
          discard_candidate = True
          break

        if not candidate_deleted and existing_deleted:
          to_remove.add(existing)

    if discard_candidate:
      return None

    versions.difference_update(to_remove)
    versions.add(candidate)
    self.__nodes[child] = versions
    return None

  def __str__(self):
    return str(self())

  def __repr__(self):
    return str(self)
