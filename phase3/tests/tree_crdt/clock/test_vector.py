import unittest

from tree_crdt.clock import VectorClock


class TestVectorClock(unittest.TestCase):
  def setUp(self):
    self.clock = VectorClock(id=1, max_id=3)

  def test_initial_state(self):
    self.assertEqual(self.clock.id, 1)
    self.assertEqual(self.clock.timestamp, {0: 0, 1: 0, 2: 0})

  def test_update_local_increments_owner_field(self):
    self.clock.update(None)
    self.assertEqual(self.clock.timestamp, {0: 0, 1: 1, 2: 0})

  def test_timestamp_lt_by_sum(self):
    self.assertTrue(VectorClock.timestamp_lt({0: 1, 1: 0}, {0: 1, 1: 1}))


if __name__ == '__main__':
  unittest.main()
