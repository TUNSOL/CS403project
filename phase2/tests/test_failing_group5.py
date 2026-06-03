import unittest
from typing import Any, cast

from tree_crdt.replica import Replica
from tree_crdt.payload import MovePayload

class FailingTests(unittest.TestCase):
  def _new_replica(self, num_replicas: int) -> Replica:
    return Replica(id=0, host="127.0.0.1", main_base=5555, listener_base=6666, num_replicas=num_replicas)

  def _remote_timestamp(self, num_replicas: int, t: int, sender: int = 0) -> int | dict[int, int]:  
    return {i: (t if i == sender else 0) for i in range(num_replicas)}
  
  def _run_complex_reparenting(self, num_replicas: int) -> None:
    replica = self._new_replica(num_replicas)
    replica.apply_remote_move(MovePayload(i=0, t=self._remote_timestamp(num_replicas, 1), p=None, m={"status": "active"}, c=1))
    replica.apply_remote_move(MovePayload(i=0, t=self._remote_timestamp(num_replicas, 2), p=1,    m={"status": "active"}, c=2))

    initial_nodes = len(replica.tree())

    replica.apply_remote_move(MovePayload(i=0, t=self._remote_timestamp(num_replicas, 3), p=None, m={"status": "active"}, c=2))
    replica.apply_remote_move(MovePayload(i=0, t=self._remote_timestamp(num_replicas, 4), p=1,    m={"status": "active"}, c=2))

    self.assertEqual(len(replica.tree()), initial_nodes)
    self.assertEqual(len(replica.log), 4)

  def test_complex_reparenting_scenario(self):
    self._run_complex_reparenting(num_replicas=1)

  def test_min_timestamp_multiple_peers(self):
    replica = Replica(id=0, host="127.0.0.1", main_base=5555, listener_base=6666, num_replicas=3)
    replica.tick_clock(None)
    replica.record_last_timestamp(1, {0: 1, 1: 5, 2: 0})
    replica.record_last_timestamp(2, {0: 2, 1: 3, 2: 4})

    min_ts = cast(Any, replica)._Replica__min_timestamp()
    self.assertEqual(min_ts, {0: 1, 1: 3, 2: 0})

if __name__ == "__main__":
  unittest.main()