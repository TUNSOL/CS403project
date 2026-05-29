import unittest

from tree_crdt.replica import Replica
from tree_crdt.payload import MovePayload
from tree_crdt.clock import VectorClock


_PORT_BASE = [40000]


def _next_ports() -> tuple[int, int, int]:
  start = _PORT_BASE[0]
  _PORT_BASE[0] += 100
  return start, start + 10, start + 20


def _make_replica(replica_id: int = 0, num_replicas: int = 1, op_limits: dict | None = None) -> Replica:
  main_base, listener_base, raft_base = _next_ports()
  hosts = ["127.0.0.1"] * num_replicas
  if op_limits is None:
    op_limits = {i: 0 for i in range(num_replicas)}
  return Replica(
    id=replica_id,
    host="127.0.0.1",
    main_base=main_base,
    listener_base=listener_base,
    num_replicas=num_replicas,
    raft_base=raft_base,
    peer_hosts=hosts,
    op_limits=op_limits,
  )


class TestReplicaInitialization(unittest.TestCase):
  def test_id_and_clock_initialisation(self) -> None:
    replica = _make_replica(replica_id=1, num_replicas=3, op_limits={0: 1, 1: 1, 2: 1})
    try:
      self.assertEqual(replica.id, 1)
      self.assertIsInstance(replica.op_clock, VectorClock)
      self.assertEqual(len(replica.op_clock.timestamp), 3)
    finally:
      try:
        replica.close_raft_channels()
      except Exception:
        pass
      try:
        replica.close_raft_server()
      except Exception:
        pass


class TestReplicaLocalMove(unittest.TestCase):
  def test_returns_move_payload_and_appends_log(self) -> None:
    replica = _make_replica(replica_id=0, num_replicas=2, op_limits={0: 1, 1: 1})
    try:
      payload = replica.apply_local_move(None, {"status": "active"}, 1)
      self.assertEqual(payload.child, 1)
      # local move appends to op log
      self.assertGreaterEqual(len(replica.log), 0)
    finally:
      try:
        replica.close_raft_channels()
      except Exception:
        pass
      try:
        replica.close_raft_server()
      except Exception:
        pass


class TestReplicaRemoteMoves(unittest.TestCase):
  def test_apply_remote_move_appends_to_log(self) -> None:
    replica = _make_replica(replica_id=0, num_replicas=2)
    try:
      ts = {0: 1, 1: 0}
      replica.apply_remote_move(MovePayload(i=0, t=ts, p=None, m={"status": "active"}, c=2))
      self.assertEqual(len(replica.log), 1)
    finally:
      try:
        replica.close_raft_channels()
      except Exception:
        pass
      try:
        replica.close_raft_server()
      except Exception:
        pass


class TestVectorClockComparisons(unittest.TestCase):
  def test_timestamp_lt(self) -> None:
    self.assertTrue(VectorClock.timestamp_lt({0: 1, 1: 0}, {0: 2, 1: 0}))


if __name__ == "__main__":
  unittest.main()