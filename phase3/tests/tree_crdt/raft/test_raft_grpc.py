import random
import time
import unittest

from tree_crdt.raft import GrpcTransport, GrpcRaftServer, RaftNode, Role


class TestRaftGrpc(unittest.TestCase):
  def _drive_time(self, nodes, steps: int = 30, tick_ms: int = 50) -> None:
    for _ in range(steps):
      for node in nodes:
        node.tick(tick_ms)
      time.sleep(0.01)

  def _log(self, message: str) -> None:
    print(f"[raft-test] {message}")
  
  def _start_cluster(self, size: int = 3, apply_callbacks: list | None = None):
    if apply_callbacks is None:
      apply_callbacks = [None for _ in range(size)]

    nodes = []
    servers = []

    for node_id in range(size):
      node = RaftNode(
        node_id=node_id,
        peers=list(range(size)),
        apply=apply_callbacks[node_id],
        rng=random.Random(node_id),
      )
      server = GrpcRaftServer(node, "127.0.0.1:0")
      nodes.append(node)
      servers.append(server)

    addresses = {
      node.id: f"127.0.0.1:{server.bound_port}"
      for node, server in zip(nodes, servers)
    }

    transports = []
    for node in nodes:
      transport = GrpcTransport(node=node, peer_addresses=addresses)
      node.set_transport(transport)
      transports.append(transport)

    for server in servers:
      server.start()

    return nodes, servers, transports

  def _stop_cluster(self, servers, transports) -> None:
    for transport in transports:
      transport.close()
    for server in servers:
      server.stop(grace=0).wait()

  def test_grpc_server_stop_returns_waitable_handle(self) -> None:
    node = RaftNode(node_id=0, peers=[0], rng=random.Random(0))
    server = GrpcRaftServer(node, "127.0.0.1:0")

    server.start()
    stop_handle = server.stop(grace=0)

    self.assertIsNotNone(stop_handle)
    self.assertTrue(hasattr(stop_handle, "wait"))
    stop_handle.wait()

  def test_leader_election_over_grpc(self) -> None:
    nodes, servers, transports = self._start_cluster()
    try:
      self._log("starting leader election test")
      self._drive_time(nodes, steps=40)
      leaders = [node for node in nodes if node.role == Role.LEADER]
      self._log(f"leaders: {[node.id for node in leaders]}")
      self.assertEqual(len(leaders), 1)
    finally:
      self._stop_cluster(servers, transports)


if __name__ == '__main__':
  unittest.main()
