import copy
import threading

from .clock.lamport import LamportClock
from .tree.node import Node
from .payload import MovePayload
from .tree import Tree

class Replica:
  def __init__(self, id: int, host: str, main_base: int, listener_base: int):
    self.__id = id
    self.__clock = LamportClock(id)
    self.__tree = Tree()
    self.__op_log: list[tuple[int, int, int | None, int | None, dict, int]] = []
    #                         (replica_id, timestamp, old_parent, new_parent, metadata, child)
    self.__zmq_main_addr = f"tcp://{host}:{main_base + id}"
    self.__zmq_listener_addr = f"tcp://{host}:{listener_base + id}"
    self.__lock = threading.RLock()

  @property
  def id(self) -> int:
    return self.__id

  @property
  def clock(self) -> LamportClock:
    with self.__lock:
      return copy.deepcopy(self.__clock)

  @property
  def tree(self) -> Tree:
    with self.__lock:
      return copy.deepcopy(self.__tree)

  @property
  def log(self) -> list[tuple[int, int, int | None, int | None, dict, int]]:
    with self.__lock:
      return copy.deepcopy(self.__op_log)

  @property
  def main_addr(self) -> str:
    return self.__zmq_main_addr

  @property
  def listener_addr(self) -> str:
    return self.__zmq_listener_addr

  def current_timestamp(self) -> int:
    with self.__lock:
      return self.__clock.timestamp

  def tick_clock(self, received: int | None) -> int:
    with self.__lock:
      self.__clock.update(received)
      return self.__clock.timestamp

  def _entry_matches_payload(
    self,
    entry: tuple[int, int, int | None, int | None, dict, int],
    op: MovePayload,
  ) -> bool:
    return (
      entry[0] == op.id
      and entry[1] == op.timestamp
      and entry[3] == op.parent
      and entry[4] == op.metadata
      and entry[5] == op.child
    )

  def apply_move(self, op: MovePayload) -> None:
    with self.__lock:
      # skip if this exact operation is already in the log
      if any(self._entry_matches_payload(entry, op) for entry in self.__op_log):
        return

      # The log is always kept sorted by (timestamp, replica_id).
      # Scan forward to find the insertion point k for the new operation.
      new_key = (op.timestamp, op.id)
      k = len(self.__op_log)
      for i, entry in enumerate(self.__op_log):
        if (entry[1], entry[0]) >= new_key:
          k = i
          break

      # UNDO: restore each entry from the end back to position k, in reverse order.
      # Each entry stores old_parent so we can roll the node back to where it was
      # before that operation was applied.
      for entry in reversed(self.__op_log[k:]):
        _, _, old_parent, _, metadata, child = entry
        self.__tree.move(Node(p=old_parent, m=copy.deepcopy(metadata), c=child))

        """
        ERROR 2: Previously, even when Tree.move() rejects a move operation due to cycle creation, the log
        still recorded this operation as applied. Then during undo-do-redo, program tried to apply something 
        that never actually changed the tree, corrupting the old_parent chain for subsequent operations.

        Now, if the move operation is NOT applied, we do not add it to log.
        """

      # DO: apply the new operation and insert its log entry at position k.
      existing = self.__tree[op.child]
      old_parent_of_new = existing.parent if existing is not None else None
      self.__tree.move(Node(p=op.parent, m=copy.deepcopy(op.metadata), c=op.child))

      # Detect rejection by comparing tree state after the call.
      # If the node's parent was not updated to op.parent, the move was rejected (cycle).
      node_after = self.__tree[op.child]
      applied = (node_after is not None and node_after.parent == op.parent)

      if not applied:
        # Move is rejected (cycle). Re-apply the undone operations; do not add to log.
        for i in range(k, len(self.__op_log)):
          _, _, _, new_parent, metadata, child = self.__op_log[i]
          self.__tree.move(Node(p=new_parent, m=copy.deepcopy(metadata), c=child))
        return

      self.__op_log.insert(
        k,
        (op.id, op.timestamp, old_parent_of_new, op.parent, copy.deepcopy(op.metadata), op.child),
      )

      # REDO: reapply every entry after position k in forward order.
      # old_parent is updated in the log to reflect the tree state just before each redo.
      for i in range(k + 1, len(self.__op_log)):
        replica_id, timestamp, _, new_parent, metadata, child = self.__op_log[i]
        existing = self.__tree[child]
        actual_old_parent = existing.parent if existing is not None else None
        self.__tree.move(Node(p=new_parent, m=copy.deepcopy(metadata), c=child))
        self.__op_log[i] = (replica_id, timestamp, actual_old_parent, new_parent, metadata, child)

  
  def apply_local_move(self, parent, metadata: dict, child: int) -> MovePayload:
    with self.__lock:
      timestamp = self.tick_clock(None)
      op = MovePayload(
        i=self.__id,
        t=timestamp,
        p=parent,
        m=copy.deepcopy(metadata),
        c=child,
      )
      self.apply_move(op)
      return op

  def apply_remote_move(self, op: MovePayload) -> None:
    with self.__lock:
      self.tick_clock(op.timestamp)
      self.apply_move(op)

  def __str__(self) -> str:
    return f"ID: {self.id}, Timestamp: {self.current_timestamp()}"
