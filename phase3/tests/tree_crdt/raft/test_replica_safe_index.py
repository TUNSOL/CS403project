import unittest

from tree_crdt.payload import MovePayload
from tree_crdt.replica import Replica


class TestReplicaSafeIndex(unittest.TestCase):
  def _make_replica(self, op_limits=None) -> Replica:
    if isinstance(op_limits, list):
      op_limits_dict = {i: v for i, v in enumerate(op_limits)}
    else:
      op_limits_dict = op_limits or {}

    replica = Replica(
      id=0,
      host="127.0.0.1",
      main_base=5500,
      listener_base=6500,
      num_replicas=2,
      raft_base=7000,
      peer_hosts=["127.0.0.1", "127.0.0.1"],
      op_limits=op_limits_dict,
    )
    self.addCleanup(replica.close_raft_channels)
    self.addCleanup(replica.close_raft_server)
    return replica

  def test_safe_index_advances_when_limits_reached(self) -> None:
    replica = self._make_replica(op_limits=[10, 0])
    move = MovePayload(
      i=0,
      t={0: 1, 1: 0},
      p=None,
      m={"status": "active"},
      c=1,
    )

    replica.apply_remote_move(move)

    self.assertEqual(replica.safe_index, 1)
    self.assertEqual(replica.log_offset, 0)
    self.assertEqual(len(replica.log), 1)

    replica._Replica__apply_raft_command({"op": "safe_index", "value": 1}, 1)

    replica._Replica__drain_pending_commits()

    self.assertEqual(replica.log_offset, 1)
    self.assertEqual(len(replica.log), 0)


if __name__ == "__main__":
  unittest.main()
