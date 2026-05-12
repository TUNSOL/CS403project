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
    self.__all_ops: list[tuple[int, int, int | None, dict, int]] = []
    #                         (replica_id, timestamp, new_parent, metadata, child)
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
    entry: tuple[int, int, int | None, dict, int],
    op: MovePayload,
  ) -> bool:
    return (
      entry[0] == op.id
      and entry[1] == op.timestamp
      and entry[2] == op.parent
      and entry[3] == op.metadata
      and entry[4] == op.child
    )

  def _rebuild_from_all_ops(self) -> None:
    self.__tree = Tree()
    self.__op_log = []

    for replica_id, timestamp, new_parent, metadata, child in self.__all_ops:
      new_node = Node(p=new_parent, m=copy.deepcopy(metadata), c=child)
      existing = self.__tree[child]
      old_parent = existing.parent if existing is not None else None

      if not self.__tree.can_move(new_node):
        continue

      self.__tree.move(new_node)
      self.__op_log.append(
        (replica_id, timestamp, old_parent, new_parent, copy.deepcopy(metadata), child)
      )

  def apply_move(self, op: MovePayload) -> None:
    with self.__lock:
      # skip if this exact operation is already in the log
      if any(self._entry_matches_payload(entry, op) for entry in self.__all_ops):
        return

      # The log is always kept sorted by (timestamp, replica_id).
      # Scan forward to find the insertion point k for the new operation.
      new_key = (op.timestamp, op.id)
      k = len(self.__all_ops)
      for i, entry in enumerate(self.__all_ops):
        if (entry[1], entry[0]) >= new_key:
          k = i
          break

      self.__all_ops.insert(
        k,
        (op.id, op.timestamp, op.parent, copy.deepcopy(op.metadata), op.child),
      )
      self._rebuild_from_all_ops()

  
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
