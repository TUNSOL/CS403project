import copy
import json
import threading
import time

from .clock import DeliveryClock, VectorClock
from .payload import MovePayload
from .raft import GrpcRaftServer, GrpcTransport, RaftNode, Role
from .tree import Tree


class Replica:
  def __init__(
    self,
    id,
    host,
    main_base,
    listener_base,
    num_replicas,
    raft_base,
    peer_hosts,
    op_limits,
  ):
    self.__id = int(id)
    self.__num_replicas = int(num_replicas)
    self.__op_clock = VectorClock(self.__id, self.__num_replicas)
    self.__checkpoint_clock = DeliveryClock(self.__id, self.__num_replicas)
    self.__tree = Tree()
    self.__tree_snapshot = Tree()
    self.__op_log: list[tuple[int, dict[int, int], int | None, dict | None, int | None, dict, int]] = []

    self.__log_offset = 0
    self.__safe_index_abs = 0
    self.__last_sent_safe_index = 0
    self.__raft_committed_safe_index = 0
    self.__safe_index_enabled = bool(op_limits)
    self.__op_limits = {
      replica_id: int(op_limits.get(replica_id, 0))
      for replica_id in range(self.__num_replicas)
    }

    self.__zmq_main_addr = f"tcp://{host}:{main_base + self.__id}"
    self.__zmq_listener_addr = f"tcp://{host}:{listener_base + self.__id}"
    self.__lock = threading.RLock()

    self.__commit_lock = threading.Lock()
    self.__pending_commits: list[tuple[int, int]] = []
    self.__raft_shutdown = threading.Event()
    self.__raft_node = None
    self.__raft_server = None
    self.__raft_transport = None
    self.__raft_thread = None

    self.__raft_enabled = self.__safe_index_enabled
    if self.__raft_enabled:
      self.__raft_node = RaftNode(
        node_id=self.__id,
        peers=list(range(self.__num_replicas)),
        apply=self.__apply_raft_command,
      )
      self.__raft_node.set_snapshot_provider(self.__snapshot_bytes)
      self.__raft_node.set_apply_snapshot_handler(self.__apply_snapshot_bytes)

      self.__raft_server = GrpcRaftServer(
        self.__raft_node,
        f"{host}:{raft_base + self.__id}",
      )
      self.__raft_server.start()

      peer_addresses = {
        peer_id: f"{peer_hosts[peer_id]}:{raft_base + peer_id}"
        for peer_id in range(self.__num_replicas)
        if peer_id != self.__id
      }
      self.__raft_transport = GrpcTransport(
        node=self.__raft_node,
        peer_addresses=peer_addresses,
      )
      self.__raft_node.set_transport(self.__raft_transport)

      self.__raft_thread = threading.Thread(
        target=self.__run_raft,
        name=f"Replica-{self.__id}-raft",
        daemon=True,
      )
      self.__raft_thread.start()

  @property
  def id(self):
    return self.__id

  @property
  def op_clock(self):
    with self.__lock:
      return copy.deepcopy(self.__op_clock)

  @property
  def checkpoint_clock(self):
    with self.__lock:
      return copy.deepcopy(self.__checkpoint_clock)

  @property
  def tree(self):
    with self.__lock:
      return copy.deepcopy(self.__tree)

  @property
  def tree_snapshot(self):
    with self.__lock:
      return copy.deepcopy(self.__tree_snapshot)

  @property
  def log(self):
    with self.__lock:
      return copy.deepcopy(self.__op_log)

  @property
  def raft_log(self):
    if self.__raft_node is None:
      return []
    return copy.deepcopy(self.__raft_node.log.entries)

  @property
  def log_offset(self):
    with self.__lock:
      return self.__log_offset

  @property
  def safe_index(self):
    with self.__lock:
      return self.__safe_index_abs

  @property
  def main_addr(self):
    return self.__zmq_main_addr

  @property
  def listener_addr(self):
    return self.__zmq_listener_addr

  def current_timestamp(self):
    with self.__lock:
      return copy.deepcopy(self.__op_clock.timestamp)

  def tick_op_clock(self, received):
    with self.__lock:
      self.__op_clock.update(self.__normalize_timestamp(received))
      return copy.deepcopy(self.__op_clock.timestamp)

  def tick_checkpoint_clock(self, source):
    with self.__lock:
      self.__checkpoint_clock.update(source)
      return copy.deepcopy(self.__checkpoint_clock.timestamp)

  def apply_local_move(self, parent, metadata, child):
    with self.__lock:
      timestamp = self.tick_op_clock(None)
      payload = MovePayload(
        i=self.__id,
        t=copy.deepcopy(timestamp),
        p=parent,
        m=self.__operation_metadata(metadata),
        c=child,
      )
      self.__apply_move(payload)
      return payload

  def apply_remote_move(self, op):
    with self.__lock:
      op = self.__normalized_payload(op)
      self.tick_op_clock(op.timestamp)
      self.__apply_move(op)

  def flush_snapshot(self, timeout_seconds=30.0):
    deadline = time.time() + timeout_seconds
    expected_total = sum(self.__op_limits.values())

    while time.time() <= deadline:
      with self.__lock:
        self.__try_advance_safe_index()
        self.__drain_pending_commits()
        if self.__log_offset >= expected_total:
          return True
      time.sleep(0.05)

    return False

  def close_raft_channels(self):
    self.__raft_shutdown.set()
    if self.__raft_thread is not None:
      self.__raft_thread.join(timeout=2.0)
    if self.__raft_transport is not None:
      self.__raft_transport.close()

  def close_raft_server(self):
    if self.__raft_server is None:
      return
    stop_handle = self.__raft_server.stop(grace=1.0)
    if hasattr(stop_handle, "wait"):
      stop_handle.wait(timeout=2.0)

  def __run_raft(self):
    last_tick = time.monotonic()
    while not self.__raft_shutdown.is_set():
      now = time.monotonic()
      elapsed_ms = max(1, int((now - last_tick) * 1000))
      last_tick = now

      if self.__raft_node is not None:
        self.__raft_node.tick(elapsed_ms)

      with self.__lock:
        self.__drain_pending_commits()
        self.__try_advance_safe_index()

        if (
          self.__raft_node is not None
          and self.__raft_node.role == Role.LEADER
          and self.__safe_index_abs > self.__last_sent_safe_index
        ):
          command = {"op": "safe_index", "value": self.__safe_index_abs}
          raft_index = self.__raft_node.client_append(command)
          if raft_index is not None:
            self.__last_sent_safe_index = self.__safe_index_abs
            self.__raft_node.set_limit_index(raft_index)

      time.sleep(0.01)

  def __apply_raft_command(self, command, raft_log_index):
    if not isinstance(command, dict) or command.get("op") != "safe_index":
      return
    with self.__commit_lock:
      self.__pending_commits.append((int(command.get("value", 0)), int(raft_log_index)))

  def __drain_pending_commits(self):
    with self.__commit_lock:
      pending = list(self.__pending_commits)
      self.__pending_commits.clear()

    for safe_index_abs, raft_log_index in pending:
      self.__apply_safe_index_commit(safe_index_abs, raft_log_index)

  def __apply_safe_index_commit(self, safe_index_abs, raft_log_index):
    self.__raft_committed_safe_index = max(
      self.__raft_committed_safe_index,
      int(safe_index_abs),
    )
    self.__compact_log(raft_log_index)

  def __compact_log(self, raft_log_index):
    drop_target = min(self.__raft_committed_safe_index, self.__safe_index_abs)
    drop_count = max(0, drop_target - self.__log_offset)
    if drop_count:
      del self.__op_log[:drop_count]
      self.__log_offset += drop_count

    if self.__raft_node is not None:
      self.__raft_node.log.compact(int(raft_log_index))

  def __apply_move(self, op):
    op = self.__normalized_payload(op)
    if any(self.__entry_matches_payload(entry, op) for entry in self.__op_log):
      return

    insertion_point = self.__find_insertion_point(op)
    self.__op_log.insert(
      insertion_point,
      self.__make_log_entry(op, None, None),
    )
    self.__rebuild_tree_from_snapshot()
    self.__try_advance_safe_index()

  def __is_in_order(self, op):
    return self.__find_insertion_point(op) == len(self.__op_log)

  def __find_insertion_point(self, op):
    for index, entry in enumerate(self.__op_log):
      if self.__op_precedes_entry(op, entry):
        return index
    return len(self.__op_log)

  def __undo_operations(self, ops):
    skipped = list(ops)
    self.__tree = copy.deepcopy(self.__tree_snapshot)
    start_pos = max(0, self.__safe_index_abs - self.__log_offset)
    for index in range(start_pos, len(self.__op_log)):
      entry = self.__op_log[index]
      if entry in skipped:
        continue
      self.__apply_entry_to_tree(index, self.__tree, update_old_state=True)

  def __do_operation(self, op, old_p, old_m, p, m, c, insert_pos=None):
    entry = (
      op.id,
      self.__normalize_timestamp(copy.deepcopy(op.timestamp)),
      old_p,
      copy.deepcopy(old_m),
      p,
      self.__operation_metadata(m),
      c,
    )
    if insert_pos is None:
      self.__op_log.append(entry)
    else:
      self.__op_log.insert(insert_pos, entry)
    self.__tree.move(p, copy.deepcopy(entry[5]), c)

  def __redo_operations(self, ops):
    for entry in ops:
      _, _, _, _, parent, metadata, child = entry
      self.__tree.move(parent, copy.deepcopy(metadata), child)

  def __try_advance_safe_index(self):
    if not self.__safe_index_enabled:
      return

    while True:
      start_pos = self.__safe_index_abs - self.__log_offset
      if start_pos < 0:
        start_pos = 0
      if start_pos >= len(self.__op_log):
        return

      checkpoint = self.__checkpoint_clock.timestamp
      suffix = self.__op_log[start_pos:]

      for replica_id in range(self.__num_replicas):
        if checkpoint.get(replica_id, 0) >= self.__op_limits.get(replica_id, 0):
          continue
        if not any(
          entry[0] == replica_id
          and entry[1].get(replica_id, 0) == checkpoint.get(replica_id, 0) + 1
          for entry in suffix
        ):
          return

      candidate = self.__op_log[start_pos]
      replica_id, timestamp, _, _, parent, metadata, child = candidate
      if checkpoint.get(replica_id, 0) >= self.__op_limits.get(replica_id, 0):
        return

      exceeded = [
        key
        for key in sorted(checkpoint)
        if timestamp.get(key, 0) > checkpoint.get(key, 0)
      ]

      if len(exceeded) != 1:
        return
      if exceeded[0] != replica_id:
        return
      if timestamp.get(replica_id, 0) != checkpoint.get(replica_id, 0) + 1:
        return

      for right_id in range(replica_id + 1, self.__num_replicas):
        if checkpoint.get(right_id, 0) >= self.__op_limits.get(right_id, 0):
          continue
        if not any(
          entry[1].get(right_id, 0) == checkpoint.get(right_id, 0) + 1
          for entry in suffix
        ):
          return

      self.__tree_snapshot.move(parent, copy.deepcopy(metadata), child)
      self.__checkpoint_clock.update(replica_id)
      self.__safe_index_abs += 1

  def __str__(self):
    return f"ID: {self.id}, Timestamp: {self.current_timestamp()}"

  def __repr__(self):
    return str(self)

  @staticmethod
  def __normalize_timestamp(timestamp):
    normalized = VectorClock._normalize_timestamp(timestamp)
    if isinstance(normalized, dict):
      return {int(key): int(value) for key, value in normalized.items()}
    return normalized

  def __normalized_payload(self, op):
    return MovePayload(
      i=op.id,
      t=self.__normalize_timestamp(copy.deepcopy(op.timestamp)),
      p=op.parent,
      m=self.__operation_metadata(op.metadata),
      c=op.child,
    )

  @staticmethod
  def __operation_metadata(metadata):
    result = copy.deepcopy(metadata) if metadata is not None else {}
    result.pop("applied", None)
    result.pop("last_ts", None)
    result.setdefault("status", "active")
    return result

  @staticmethod
  def __is_deleted_metadata(metadata):
    return metadata.get("status") == "deleted"

  def __current_state(self, tree, child):
    node = tree[child]
    if node is None:
      return None, None
    return node.parent, copy.deepcopy(node.metadata)

  def __make_log_entry(self, op, old_parent, old_metadata):
    return (
      op.id,
      self.__normalize_timestamp(copy.deepcopy(op.timestamp)),
      old_parent,
      copy.deepcopy(old_metadata),
      op.parent,
      self.__operation_metadata(op.metadata),
      op.child,
    )

  def __apply_entry_to_tree(self, index, tree, update_old_state):
    replica_id, timestamp, old_parent, old_metadata, parent, metadata, child = self.__op_log[index]
    actual_old_parent, actual_old_metadata = self.__current_state(tree, child)

    if update_old_state:
      self.__op_log[index] = (
        replica_id,
        timestamp,
        actual_old_parent,
        actual_old_metadata,
        parent,
        metadata,
        child,
      )

    tree.move(parent, copy.deepcopy(metadata), child)

  def __rebuild_tree_from_snapshot(self):
    self.__tree = copy.deepcopy(self.__tree_snapshot)
    start_pos = max(0, self.__safe_index_abs - self.__log_offset)
    for index in range(start_pos, len(self.__op_log)):
      self.__apply_entry_to_tree(index, self.__tree, update_old_state=True)

  def __op_precedes_entry(self, op, entry):
    if VectorClock.timestamp_lt(op.timestamp, entry[1]):
      return True
    if VectorClock.timestamp_eq(op.timestamp, entry[1]):
      return op.id < entry[0]
    return False

  def __entry_matches_payload(self, entry, op):
    return (
      entry[0] == op.id
      and VectorClock.timestamp_eq(entry[1], op.timestamp)
      and entry[4] == op.parent
      and entry[6] == op.child
      and entry[5] == self.__operation_metadata(op.metadata)
    )

  def __snapshot_bytes(self, _raft_index):
    with self.__lock:
      payload = {
        "log_offset": self.__log_offset,
        "checkpoint": self.__checkpoint_clock.timestamp,
        "nodes": [
          {
            "parent": node.parent,
            "metadata": node.metadata,
            "child": node.child,
          }
          for node in self.__tree_snapshot(deleted=True)
        ],
      }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

  def __apply_snapshot_bytes(self, data, last_included_index):
    if not data:
      return

    payload = json.loads(data.decode("utf-8"))
    snapshot = Tree()
    for node in payload.get("nodes", []):
      snapshot.move(node.get("parent"), node.get("metadata", {}), node.get("child"))

    with self.__lock:
      incoming_offset = int(payload.get("log_offset", last_included_index))
      if incoming_offset < self.__log_offset:
        return
      
      # FIX: trim op-log entries that are now below the snapshot frontier
      drop_count = incoming_offset - self.__log_offset
      if drop_count > 0:
          del self.__op_log[:drop_count]

      self.__tree_snapshot = snapshot
      self.__checkpoint_clock.set_timestamp(payload.get("checkpoint", {}))
      self.__log_offset = incoming_offset
      self.__safe_index_abs = max(self.__safe_index_abs, incoming_offset)
      self.__rebuild_tree_from_snapshot()
