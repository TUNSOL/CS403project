import unittest

from tree_crdt.clock import DeliveryClock


class TestDeliveryClock(unittest.TestCase):
  def setUp(self):
    self.clock = DeliveryClock(id=1, max_id=3)

  def test_initial_state(self):
    self.assertEqual(self.clock.id, 1)
    self.assertEqual(self.clock.timestamp, {0: 0, 1: 0, 2: 0})

  def test_update_with_source_increments_that_field(self):
    self.clock.update(0)
    self.assertEqual(self.clock.timestamp, {0: 1, 1: 0, 2: 0})
    self.clock.update(2)
    self.assertEqual(self.clock.timestamp, {0: 1, 1: 0, 2: 1})


if __name__ == "__main__":
  unittest.main()