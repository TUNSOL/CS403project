import copy
import threading

from .clock import Clock, LamportClock, VectorClock
from .payload import MovePayload
from .tree import Tree


class Replica:
  """A replica in the Tree CRDT system.

  In Phase 2, the Replica gains:

    - A `num_replicas` constructor argument that
      switches the replica's clock from LamportClock to VectorClock.
    - A peer-progress map (peer_id -> last seen timestamp) used to compute
      the causal-stability threshold for log compaction.
    - A second Tree instance, the SNAPSHOT, holding the cumulative effect
      of log entries that are already known to be causally stable.
    - A new resolution path inside __apply_move (the Move-Wins case)
      that may flip the "applied" flag of a past log entry and rebuild
      the tree from the snapshot.

  The Replica object is shared between the main and listener threads of
  its process; access must remain thread-safe (e.g. via threading.RLock).

  See the Phase 2 PDF, Section "The Replica Class", for the full contract.
  """

  def __init__(self, id, host, main_base, listener_base, num_replicas=None):
    """Construct a replica.
    The clock is a VectorClock(id, num_replicas).
    """
    self.__id = id
    self.__num_replicas = num_replicas
    self.__clock: Clock = (
      VectorClock(id, num_replicas)
      if num_replicas is not None
      else LamportClock(id)
    )
    self.__tree = Tree()
    self.__tree_snapshot = Tree()
    self.__op_log: list[tuple[int, object, object | None, object | None, dict, object]] = []
    self.__last_timestamps: dict[int, object] = {}
    self.__zmq_main_addr = f"tcp://{host}:{main_base + id}"
    self.__zmq_listener_addr = f"tcp://{host}:{listener_base + id}"
    self.__lock = threading.RLock()

  # ---------------------------------------------------------------------
  # Public read-only accessors
  # ---------------------------------------------------------------------

  @property
  def id(self):
    """Return the replica ID."""
    return self.__id

  @property
  def clock(self):
    """Return a deep copy of the clock."""
    with self.__lock:
      return copy.deepcopy(self.__clock)

  @property
  def tree(self):
    """Return a deep copy of the active tree."""
    with self.__lock:
      return copy.deepcopy(self.__tree)
  
  @property
  def tree_snapshot(self):
    """Return a deep copy of the tree snapshot."""
    with self.__lock:
      return copy.deepcopy(self.__tree_snapshot)

  @property
  def log(self):
    """Return a deep copy of the operation log."""
    with self.__lock:
      return copy.deepcopy(self.__op_log)

  @property
  def last_timestamps(self):
    """Return a deep copy of the per-peer most-recent-timestamp map."""
    with self.__lock:
      return copy.deepcopy(self.__last_timestamps)

  @property
  def main_addr(self):
    """Return the ZeroMQ bind address for the main thread."""
    return self.__zmq_main_addr

  @property
  def listener_addr(self):
    """Return the ZeroMQ bind address for the listener thread."""
    return self.__zmq_listener_addr

  # ---------------------------------------------------------------------
  # Clock helpers
  # ---------------------------------------------------------------------

  def current_timestamp(self):
    """Return the current value of the clock's timestamp."""
    with self.__lock:
      return copy.deepcopy(self.__clock.timestamp)

  def tick_clock(self, received):
    """Advance the clock by calling clock.update(received), thread-safely.

    Pass `received=None` for a local event, or the received timestamp for
    a remote event. Returns the new timestamp.
    """
    with self.__lock:
      self.__clock.update(self.__normalize_timestamp(received))
      return copy.deepcopy(self.__clock.timestamp)

  # ---------------------------------------------------------------------
  # Peer-progress bookkeeping (Phase 2)
  # ---------------------------------------------------------------------

  def record_last_timestamp(self, replica_id, last_timestamp):
    """Record that peer `replica_id` was most recently seen at `last_timestamp`.

    Called by the listener-side of the receive path whenever a peer
    advertises its progress (see PDF Section "Tracking Peer Progress").
    """
    with self.__lock:
      normalized = self.__normalize_timestamp(last_timestamp)
      current = self.__last_timestamps.get(replica_id)

      if current is None:
        self.__last_timestamps[replica_id] = copy.deepcopy(normalized)
        return

      if isinstance(current, int) and isinstance(normalized, int):
        self.__last_timestamps[replica_id] = max(current, normalized)
        return

      if isinstance(current, dict) and isinstance(normalized, dict):
        keys = set(current.keys()) | set(normalized.keys())
        self.__last_timestamps[replica_id] = {
          key: max(current.get(key, 0), normalized.get(key, 0))
          for key in keys
        }
        return

      self.__last_timestamps[replica_id] = copy.deepcopy(normalized)

  def get_peer_timestamp(self, peer_id):
    """Return the most recent timestamp recorded for peer `peer_id`.

    Contract:
      - If `peer_id` has been registered via `record_last_timestamp`,
        return the recorded value (deep-copy-safe: the caller must not
        be able to mutate internal state through the returned value).
      - If `peer_id` is unknown, return the identity element of the
        clock's order: all-zeros vectorn(with the same key set as the local clock) 
        for a vector-clock replica.
      - The method is read-only and must be thread-safe.
    """
    with self.__lock:
      if peer_id in self.__last_timestamps:
        return copy.deepcopy(self.__last_timestamps[peer_id])

      return self.__zero_timestamp()

  # ---------------------------------------------------------------------
  # Public apply paths
  # ---------------------------------------------------------------------

  def apply_local_move(self, parent, metadata, child):
    """Generate a local Move operation, apply it, and return the payload.

    The metadata dict MUST contain "status": "active" or "deleted".
    The library will set "applied" inside __apply_move.
    """
    with self.__lock:
      timestamp = self.tick_clock(None)
      op = MovePayload(
        i=self.__id,
        t=timestamp,
        p=parent,
        m=copy.deepcopy(metadata),
        c=child,
      )
      self.__apply_move(op)
      return op

  def apply_remote_move(self, op):
    """Apply a Move operation received from a peer."""
    with self.__lock:
      self.tick_clock(op.timestamp)
      self.__apply_move(op)

  def apply_move(self, op):
    """Phase 1-compatible apply path for an already timestamped operation."""
    with self.__lock:
      self.__apply_move(op)

  def finalize(self):
    """Rebuild the active tree from the stable snapshot and remaining log."""
    with self.__lock:
      self.__rebuild_tree_from_snapshot()

  # ---------------------------------------------------------------------
  # Internal: ordering, conflict detection, apply, undo/do/redo
  # ---------------------------------------------------------------------
  #
  # The structure below is the recommended decomposition; you may rename,
  # reorganise, or merge methods as you see fit. The PDF describes the
  # required SEMANTICS of each step.

  def __is_in_order(self, op):
    """Return True iff `op` can be appended to the log without disturbing it.

    Vector : op is in order iff op does NOT strictly happen-before the
             last log entry. (Concurrent ops are in order; they become
             multi-value peers.)
    """
    if not self.__op_log:
      return True

    last_entry = self.__op_log[-1]
    return not self.__op_happens_before_entry(op, last_entry)

  def __get_concurrent_conflicts(self, op):
    """Return log entries that conflict with `op` (vector clock case).

    Two ops conflict iff their timestamps are concurrent AND they target
    the same child ID. Used to drive the Move-Wins path of __apply_move.
    """
    return [self.__op_log[index] for index in self.__get_concurrent_conflict_indices(op)]

  def __find_insertion_point(self, op):
    """Return the index in op_log at which `op` should be inserted.

    Vector : insert before the first entry whose timestamp is >= op.timestamp
             under the partial order; concurrent entries are skipped past
             so that they remain peers rather than being undone/redone.
    """
    for index, entry in enumerate(self.__op_log):
      if self.__op_happens_before_entry(op, entry):
        return index

      if self.__timestamps_equal(op.timestamp, entry[1]) and op.id < entry[0]:
        return index

    return len(self.__op_log)

  def __apply_move(self, op):
    """The central apply method.

    Required behaviour (see PDF Section "The __apply_move(op) method"):

      1. If using vector clocks, detect log conflicts. For each conflict:
           - incoming Delete vs. existing alive   --> mark op as
             "applied": False, append to log, RETURN (no checkpointing).
           - incoming alive  vs. existing Delete  --> flip the existing
             entry's "applied" flag to False; remember to rebuild.

      2. If no flag was flipped AND op is in order: append + apply to tree.

      3. Else, run the appropriate recovery sequence:
           - undo-do-redo (Phase 1, but skipping entries with applied=False),
             OR
           - log-insertion-rollback-redo: insert op, restore tree from
             snapshot, redo all entries currently in the log whose
             applied=True.

      4. End by attempting log compaction (call __compact_log).
    """
    with self.__lock:
      op = self.__normalized_payload(op)

      if any(self.__entry_matches_payload(entry, op) for entry in self.__op_log):
        return

      op_metadata = self.__operation_metadata(op, applied=True)
      incoming_deleted = self.__is_deleted_metadata(op_metadata)
      flipped_past_entry = False

      for index in self.__get_concurrent_conflict_indices(op):
        replica_id, timestamp, old_parent, new_parent, metadata, child = self.__op_log[index]
        existing_deleted = self.__is_deleted_metadata(metadata)
        existing_applied = metadata.get("applied", True)

        if incoming_deleted and not existing_deleted and existing_applied:
          loser_metadata = self.__operation_metadata(op, applied=False)
          insertion_point = self.__find_insertion_point(op)
          self.__op_log.insert(
              insertion_point,
              self.__make_log_entry(op, loser_metadata, self.__current_parent(self.__tree, op.child))
          )
          return

        if not incoming_deleted and existing_deleted and existing_applied:
          updated_metadata = copy.deepcopy(metadata)
          updated_metadata["applied"] = False
          self.__op_log[index] = (
            replica_id,
            timestamp,
            old_parent,
            new_parent,
            updated_metadata,
            child,
          )
          flipped_past_entry = True

      if not flipped_past_entry and self.__is_in_order(op):
        entry = self.__make_log_entry(
          op,
          op_metadata,
          self.__current_parent(self.__tree, op.child),
        )
        self.__op_log.append(entry)
        self.__apply_entry_to_tree(len(self.__op_log) - 1, self.__tree, update_old_parent=False)
        self.__compact_log()
        return

      insertion_point = self.__find_insertion_point(op)
      self.__op_log.insert(
        insertion_point,
        self.__make_log_entry(op, op_metadata, None),
      )
      self.__rebuild_tree_from_snapshot()
      self.__compact_log()

  # ---------------------------------------------------------------------
  # Internal: checkpointing (Phase 2)
  # ---------------------------------------------------------------------

  def __min_timestamp(self):
    """Return the causal-stability threshold (min over peer timestamps)."""
    if isinstance(self.__clock.timestamp, int):
      if self.__num_replicas is not None:
        peer_ids = [
          peer_id
          for peer_id in range(self.__num_replicas)
          if peer_id != self.__id
        ]
        if not peer_ids:
          return 0
        values = [self.get_peer_timestamp(peer_id) for peer_id in peer_ids]
      else:
        if not self.__last_timestamps:
          return 0
        values = list(self.__last_timestamps.values())
      return min(values) if values else 0

    local = self.__clock.timestamp
    replica_count = self.__num_replicas if self.__num_replicas is not None else len(local)
    peer_ids = [
      replica_id
      for replica_id in range(replica_count)
      if replica_id != self.__id
    ]

    if not peer_ids:
      return {key: 0 for key in local}

    timestamps = [self.get_peer_timestamp(replica_id) for replica_id in peer_ids]

    return {
      key: min(timestamp.get(key, 0) for timestamp in timestamps if isinstance(timestamp, dict))
      for key in local
    }

  def __compact_log(self):
    """Compact the operation log up to the causal-stability threshold.

    For each log entry whose timestamp is strictly less than the
    threshold, fold its effect (if applied=True) into the snapshot tree
    and drop it from the active log.
    """
    threshold = self.__min_timestamp()
    compact_count = 0

    for entry in self.__op_log:
      if not self.__timestamp_is_stable(entry[1], threshold):
        break

      if entry[4].get("applied", True):
        replica_id, timestamp, _, new_parent, metadata, child = entry
        self.__tree_snapshot.move(replica_id, timestamp, new_parent, copy.deepcopy(metadata), child)

      compact_count += 1

    if compact_count:
      del self.__op_log[:compact_count]

  # ---------------------------------------------------------------------
  # Internal: undo / do / redo helpers
  # ---------------------------------------------------------------------

  def __undo_operations(self, ops):
    """Rebuild the tree without the given log entries.

    A reasonable implementation is to restore from the snapshot and
    redo every other log entry whose applied=True.
    """
    skipped = list(ops)
    self.__tree = copy.deepcopy(self.__tree_snapshot)
    for index, entry in enumerate(self.__op_log):
      if entry in skipped:
        continue
      if entry[4].get("applied", True):
        self.__apply_entry_to_tree(index, self.__tree, update_old_parent=True)

  def __do_operation(self, op, *args, **kwargs):
    """Insert `op` into the log and (if applied=True) apply it to the tree."""
    op = self.__normalized_payload(op)
    metadata = self.__operation_metadata(op, applied=kwargs.get("applied", True))
    insertion_point = kwargs.get("insertion_point", len(self.__op_log))
    self.__op_log.insert(
      insertion_point,
      self.__make_log_entry(op, metadata, self.__current_parent(self.__tree, op.child)),
    )
    if metadata.get("applied", True):
      self.__apply_entry_to_tree(insertion_point, self.__tree, update_old_parent=False)

  def __redo_operations(self, ops):
    """Re-apply each entry in `ops` (whose applied=True) to the tree."""
    for entry in ops:
      if not entry[4].get("applied", True):
        continue
      replica_id, timestamp, _, new_parent, metadata, child = entry
      self.__tree.move(replica_id, timestamp, new_parent, copy.deepcopy(metadata), child)

  # ---------------------------------------------------------------------
  # String representations
  # ---------------------------------------------------------------------

  def __str__(self):
    return f"ID: {self.id}, Timestamp: {self.current_timestamp()}"

  def __repr__(self):
    return str(self)

  @staticmethod
  def __normalize_timestamp(timestamp):
    return VectorClock._normalize_timestamp(timestamp)

  def __zero_timestamp(self):
    timestamp = self.__clock.timestamp
    if isinstance(timestamp, dict):
      return {key: 0 for key in timestamp}
    return 0

  def __normalized_payload(self, op):
    return MovePayload(
      i=op.id,
      t=self.__normalize_timestamp(copy.deepcopy(op.timestamp)),
      p=op.parent,
      m=copy.deepcopy(op.metadata),
      c=op.child,
    )

  def __operation_metadata(self, op, applied: bool) -> dict:
    metadata = copy.deepcopy(op.metadata)
    metadata.pop("last_ts", None)
    metadata.setdefault("status", "active")
    metadata["applied"] = applied
    return metadata

  @staticmethod
  def __is_deleted_metadata(metadata: dict) -> bool:
    return metadata.get("status", "active") == "deleted"

  @staticmethod
  def __timestamp_lt(lhs, rhs, lhs_replica=None, rhs_replica=None) -> bool:
    lhs = Replica.__normalize_timestamp(lhs)
    rhs = Replica.__normalize_timestamp(rhs)

    if isinstance(lhs, int) and isinstance(rhs, int):
      return (lhs, lhs_replica if lhs_replica is not None else -1) < (
        rhs,
        rhs_replica if rhs_replica is not None else -1,
      )

    return VectorClock.timestamp_lt(lhs, rhs)

  @staticmethod
  def __timestamps_equal(lhs, rhs) -> bool:
    return VectorClock.timestamp_eq(
      Replica.__normalize_timestamp(lhs),
      Replica.__normalize_timestamp(rhs),
    )

  @staticmethod
  def __timestamps_concurrent(lhs, rhs) -> bool:
    return VectorClock.timestamp_concurrent(
      Replica.__normalize_timestamp(lhs),
      Replica.__normalize_timestamp(rhs),
    )

  @staticmethod
  def __timestamp_is_stable(timestamp, threshold) -> bool:
    timestamp = Replica.__normalize_timestamp(timestamp)
    threshold = Replica.__normalize_timestamp(threshold)

    if isinstance(timestamp, int) and isinstance(threshold, int):
      return timestamp < threshold

    return VectorClock.timestamp_lt(timestamp, threshold)

  def __op_happens_before_entry(self, op, entry) -> bool:
    return self.__timestamp_lt(op.timestamp, entry[1], op.id, entry[0])

  def __entry_matches_payload(self, entry, op) -> bool:
    return (
      entry[0] == op.id
      and self.__timestamps_equal(entry[1], op.timestamp)
      and entry[3] == op.parent
      and entry[5] == op.child
    )

  def __get_concurrent_conflict_indices(self, op):
    return [
      index
      for index, entry in enumerate(self.__op_log)
      if entry[5] == op.child and self.__timestamps_concurrent(entry[1], op.timestamp)
    ]

  @staticmethod
  def __timestamp_sort_key(timestamp):
    timestamp = Replica.__normalize_timestamp(timestamp)
    if isinstance(timestamp, dict):
      return (1, tuple(sorted(timestamp.items())))
    return (0, timestamp)

  def __current_parent(self, tree: Tree, child):
    versions = tree.get_active(child)
    if not versions:
      versions = {node for node in tree[child] if not self.__is_deleted_metadata(node.metadata)}
    if not versions:
      versions = tree[child]
    if not versions:
      return None

    selected = sorted(
      versions,
      key=lambda node: (
        self.__timestamp_sort_key(node.timestamp),
        node.replica_id,
        -1 if node.parent is None else node.parent,
      ),
    )[-1]
    return selected.parent

  def __make_log_entry(self, op, metadata, old_parent):
    return (
      op.id,
      self.__normalize_timestamp(copy.deepcopy(op.timestamp)),
      old_parent,
      op.parent,
      copy.deepcopy(metadata),
      op.child,
    )

  def __apply_entry_to_tree(self, index: int, tree: Tree, update_old_parent: bool) -> None:
    replica_id, timestamp, old_parent, new_parent, metadata, child = self.__op_log[index]
    actual_old_parent = self.__current_parent(tree, child)

    if update_old_parent:
      self.__op_log[index] = (
        replica_id,
        timestamp,
        actual_old_parent,
        new_parent,
        metadata,
        child,
      )

    tree.move(replica_id, timestamp, new_parent, copy.deepcopy(metadata), child)

  def __rebuild_tree_from_snapshot(self) -> None:
    self.__tree = copy.deepcopy(self.__tree_snapshot)
    for index, entry in enumerate(self.__op_log):
      if entry[4].get("applied", True):
        self.__apply_entry_to_tree(index, self.__tree, update_old_parent=True)
