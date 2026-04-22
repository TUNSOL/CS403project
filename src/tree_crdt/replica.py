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

  def _rebuild_state_from_log(
    self,
    ordered_entries: list[tuple[int, int, int | None, int | None, dict, int]],
  ) -> None:
    # Replaying the log in total order yields the same result as undo/do/redo,
    # while keeping the implementation compact and deterministic.
    rebuilt_tree = Tree()
    rebuilt_log: list[tuple[int, int, int | None, int | None, dict, int]] = []

    for replica_id, timestamp, _, parent, metadata, child in ordered_entries:
      existing = rebuilt_tree[child]
      old_parent = existing.parent if existing is not None else None

      rebuilt_tree.move(Node(p=parent, m=copy.deepcopy(metadata), c=child))
      rebuilt_log.append(
        (replica_id, timestamp, old_parent, parent, copy.deepcopy(metadata), child)
      )

    self.__tree = rebuilt_tree
    self.__op_log = rebuilt_log

  def apply_move(self, op: MovePayload) -> None:
    with self.__lock:
      if any(self._entry_matches_payload(entry, op) for entry in self.__op_log):
        return

      pending_entries = copy.deepcopy(self.__op_log)
      pending_entries.append(
        (op.id, op.timestamp, None, op.parent, copy.deepcopy(op.metadata), op.child)
      )
      pending_entries.sort(key=lambda entry: (entry[1], entry[0]))

      self._rebuild_state_from_log(pending_entries)

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
