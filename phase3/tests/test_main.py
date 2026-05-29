import unittest
from unittest.mock import MagicMock, patch

import main
from tree_crdt import Replica
from tree_crdt.clock import VectorClock


class TestMainHelpers(unittest.TestCase):
  """A small subset of helpers to expose to students."""

  def test_validate_ip_accepts_valid_addresses(self):
    self.assertTrue(main.validate_ip("127.0.0.1"))
    self.assertTrue(main.validate_ip("0.0.0.0"))

  def test_get_move_generator_defaults_to_hierarchical(self):
    self.assertIs(main.get_move_generator("unknown"), main.generate_hierarchical_move)


class TestMainProcessOrchestration(unittest.TestCase):
  @patch("main.multiprocessing.Process")
  def test_main_starts_and_joins_all_processes(self, process_cls: MagicMock) -> None:
    first = MagicMock()
    first.is_alive.return_value = False
    process_cls.return_value = first

    hosts = ["127.0.0.1"]
    main.main(
      num_replicas=1,
      tree_config="hierarchical",
      hosts=hosts,
      main_base=5000,
      listener_base=6000,
      raft_base=7000,
      num_op_messages=[1],
    )

    first.start.assert_called_once()
    first.join.assert_called_once_with()


class TestMainVectorClockConfiguration(unittest.TestCase):
  def test_replica_created_with_vector_clock(self):
    replica = Replica(
      id=0,
      host="127.0.0.1",
      main_base=5000,
      listener_base=6000,
      num_replicas=3,
      raft_base=7000,
      peer_hosts=["127.0.0.1"] * 3,
      op_limits={},
    )

    self.assertIsInstance(replica.op_clock, VectorClock)
    self.assertEqual(replica.op_clock.timestamp, {0: 0, 1: 0, 2: 0})


if __name__ == "__main__":
  unittest.main()
