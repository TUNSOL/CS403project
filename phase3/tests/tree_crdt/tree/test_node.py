import unittest

from tree_crdt.tree import Node


class TestNode(unittest.TestCase):
  def setUp(self):
    self.node1 = Node(None, {"key": "value", "status": "active"}, 1)
    self.node2 = Node(1, {"key": "value", "status": "active"}, 2)
    self.node3 = Node(1, {"key": "value", "status": "active"}, 2)
    self.node4 = Node(1, {"different": "metadata", "status": "active"}, 2)

  def test_node_initialization_and_call(self):
    node = Node(5, {"test": "data", "status": "active"}, 10)
    self.assertEqual(node(), (5, {"test": "data", "status": "active"}, 10))
    self.assertEqual(node.parent, 5)
    self.assertEqual(node.metadata, {"test": "data", "status": "active"})
    self.assertEqual(node.child, 10)
    self.assertEqual(node.status, "active")

  def test_node_with_none_parent(self):
    node = Node(None, {"status": "active"}, 1)
    self.assertIsNone(node.parent)
    self.assertEqual(node.child, 1)

  def test_node_str(self):
    expected = "Node(p=None,m={'key': 'value', 'status': 'active'},c=1)"
    self.assertEqual(str(self.node1), expected)

  def test_node_equality_same_attributes(self):
    self.assertEqual(self.node2, self.node3)

  def test_node_equality_different_metadata(self):
    self.assertNotEqual(self.node2, self.node4)

  def test_node_equality_different_parents(self):
    node_a = Node(1, {"key": "value", "status": "active"}, 2)
    node_b = Node(2, {"key": "value", "status": "active"}, 2)
    self.assertNotEqual(node_a, node_b)

  def test_node_equality_different_child_ids(self):
    node_a = Node(1, {"key": "value", "status": "active"}, 2)
    node_b = Node(1, {"key": "value", "status": "active"}, 3)
    self.assertNotEqual(node_a, node_b)

  def test_node_inequality(self):
    self.assertTrue(self.node2 != self.node4)

  def test_node_empty_metadata(self):
    node = Node(5, {"status": "active"}, 10)
    self.assertEqual(node.metadata, {"status": "active"})

  def test_node_complex_metadata(self):
    metadata = {"nested": {"level": 2}, "list": [1, 2, 3], "string": "value", "status": "active"}
    node = Node(1, metadata, 2)
    self.assertEqual(node.metadata, metadata)

  def test_node_large_child_id(self):
    node = Node(1, {"status": "active"}, 999999)
    self.assertEqual(node.child, 999999)

  def test_node_equality_reflexive(self):
    self.assertEqual(self.node1, self.node1)

  def test_node_equality_same_values(self):
    node_copy = Node(None, {"key": "value", "status": "active"}, 1)
    self.assertEqual(self.node1, node_copy)

  def test_node_equality_non_node(self):
    self.assertNotEqual(self.node1, "not a node")
    self.assertNotEqual(self.node1, None)

  def test_node_status_deleted(self):
    node = Node(None, {"status": "deleted"}, 1)
    self.assertEqual(node.status, "deleted")

  def test_node_hashable(self):
    self.assertIsInstance(hash(self.node1), int)

  def test_node_equal_nodes_same_hash(self):
    node_copy = Node(None, {"key": "value", "status": "active"}, 1)
    self.assertEqual(hash(self.node1), hash(node_copy))

  def test_node_in_frozenset(self):
    s = frozenset([self.node2, self.node3])
    self.assertEqual(len(s), 1)

  def test_node_equality_with_nested_metadata(self):
    metadata = {"nested": {"a": 1, "b": [2, 3]}, "status": "active"}
    node_a = Node(1, metadata, 2)
    node_b = Node(1, {"status": "active", "nested": {"b": [2, 3], "a": 1}}, 2)
    self.assertEqual(node_a, node_b)


if __name__ == "__main__":
  unittest.main()
