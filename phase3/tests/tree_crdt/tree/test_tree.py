import unittest

from tree_crdt.tree import Tree


class TestTree(unittest.TestCase):
  def setUp(self):
    self.tree = Tree()

  def test_tree_initialization(self):
    self.assertEqual(self.tree(), set())
    self.assertEqual(self.tree(deleted=True), set())

  def test_add_single_node(self):
    self.tree.move(None, {"id": 1, "status": "active"}, 1)
    node = self.tree[1]
    self.assertIsNotNone(node)
    self.assertEqual(node.child, 1)


if __name__ == "__main__":
  unittest.main()
