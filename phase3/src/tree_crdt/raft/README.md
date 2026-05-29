# The Raft Implementation for Phase 3

This guide explains how to use the Raft module in this project without diving
into every implementation detail.

If you want the full technical reference, see [README.md](README.md).

## What this module does

The Raft package helps replicas agree on the same ordered log of operations.
Once an operation is committed, each replica applies it in the same order.

In this implementation, Raft provides:

- leader election
- log replication from leader to followers
- commit/apply after majority acknowledgement
- snapshot-based catch-up for lagging followers

## Files you should know first

- [node.py](node.py): main Raft behavior (roles, election, replication)
- [grpc_transport.py](grpc_transport.py): network RPC send/receive logic
- [log.py](log.py): log entries and compaction behavior
- [messages.py](messages.py): Raft message types
- [raft.proto](raft.proto): gRPC message/service schema

## Raft roles (quick view)

- Follower: waits for leader heartbeats
- Candidate: starts election when timeout expires
- Leader: accepts client commands and replicates them

## Minimal setup example

```python
from tree_crdt.raft import GrpcRaftServer, GrpcTransport, RaftNode

addresses = {
  0: "127.0.0.1:50051",
  1: "127.0.0.1:50052",
  2: "127.0.0.1:50053",
}

nodes = {}
servers = {}
transports = {}

def apply_op(command, index):
  print(f"apply index={index} command={command}")

for node_id in addresses:
  node = RaftNode(node_id=node_id, peers=list(addresses.keys()), apply=apply_op)
  transport = GrpcTransport(node=node, peer_addresses=addresses)
  node.set_transport(transport)

  server = GrpcRaftServer(node=node, address=addresses[node_id])
  server.start()

  nodes[node_id] = node
  transports[node_id] = transport
  servers[node_id] = server

# Raft time is manual in this project: tick every node periodically.
for _ in range(100):
  for node in nodes.values():
    node.tick(50)
```

## Sending a client command

Only the current leader accepts client appends.

```python
leader = next((n for n in nodes.values() if n.role.value == "leader"), None)
if leader is not None:
  idx = leader.client_append({"op": "insert", "value": "x"})
  print("appended index:", idx)
```

If you call `client_append` on a follower/candidate, it returns `None`.

## How commit/apply works

1. Leader appends an entry locally.
2. Leader sends `AppendEntries` to followers.
3. Once a majority has the entry, leader marks it committed.
4. Nodes apply committed entries using the `apply(command, index)` callback.

## Snapshot behavior (what to remember)

If a follower is too far behind and the leader has already compacted old log
entries, the leader sends a snapshot instead of missing log entries.

Useful hooks:

- `set_snapshot_provider(...)` on leader
- `set_apply_snapshot_handler(...)` on follower

## Common mistakes

- Not calling `tick(...)` often enough, so elections/heartbeats do not progress
- Sending client commands to a non-leader node
- Forgetting to connect transport with `node.set_transport(...)`
- Using different command encoding logic across nodes

## Debug tips

- Watch stdout for `[RAFT ...]` and `[RAFT transport ...]` logs
- Check each node role to identify the current leader
- Verify all node addresses are reachable and unique
- Start with 3 nodes before testing larger clusters

## Cleanup

```python
for transport in transports.values():
  transport.close()

for server in servers.values():
  server.stop(grace=0.5)
```