# NOTE: You can add new imports if you need
import json
import multiprocessing
import os
import pprint
import random
import re
import threading
import time
import uuid
import zmq

from dotenv import load_dotenv

from tree_crdt import Replica
from tree_crdt.payload import MovePayload

# -----------------------------------------

# Helper functions:

def validate_ip(ip):
  pattern = r'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'
  return re.match(pattern, ip) is not None

def parse_hosts(host_str):
  return [host for host in host_str.split(",") if validate_ip(host)]

def parse_numbers(nums_str):
  return [int(num) for num in nums_str.split(",")]

# -----------------------------------------

# Pub-Sub topics:
#   "MOVE"

# -----------------------------------------

# Configuration generators:

def generate_hierarchical_move(counter):
  """Config 1: Hierarchical tree (Root -> Children -> Grandchildren)"""
  if counter == 0:
    parent_id = None
    child_id = 0
    tree_type = "root"
  elif counter <= 3:
    parent_id = 0
    child_id = counter
    tree_type = "child"
  else:
    parent_id = ((counter - 4) % 3) + 1
    child_id = counter + 10
    tree_type = "grandchild"

  return parent_id, child_id, tree_type

def generate_wide_tree_move(counter):
  """Config 2: Wide tree with many siblings (one root with many children)"""
  parent_id = None if counter == 0 else 0
  child_id = counter
  tree_type = "root" if counter == 0 else "wide_child"

  return parent_id, child_id, tree_type

def generate_deep_chain_move(counter):
  """Config 3: Deep chain - linear parent-child relationships"""
  parent_id = counter - 1 if counter > 0 else None
  child_id = counter
  tree_type = "chain_node"

  return parent_id, child_id, tree_type

def generate_random_move_delete(replica, counter, tree_config):
  """Generates random moves and deletes based on the current tree state.

  The behaviour is the same as in Phase 2: starting from the current
  tree, pick either an existing node to delete (low probability) or
  an existing node to move under a new parent. The choice of parent
  is biased by `tree_config` so that the resulting tree retains the
  shape selected by the configuration generators above.
  """
  tree = replica.tree
  active_nodes = list(tree())
  active_ids = [node.child for node in active_nodes if node.child >= 0 and tree.get_active(node.child) is not None]
  root_ids = [node.child for node in active_nodes if node.parent is None and tree.get_active(node.child) is not None]

  # Fallback only when the tree is empty.
  if not active_ids:
    child_id = (replica.id * 1000) + counter + 50
    return None, child_id, "random_root"

  primary_root = min(root_ids) if root_ids else (0 if 0 in active_ids else active_ids[0])

  # Prefer deleting non-root nodes to preserve a single-root style.
  non_root_ids = [nid for nid in active_ids if nid != primary_root]
  if random.random() < 0.13 and non_root_ids:
    victim_id = random.choice(non_root_ids)
    victim = tree[victim_id]
    victim_parent = victim.parent if victim is not None else primary_root
    return victim_parent, victim_id, "random_delete"

  # Random move of an existing node while preferring non-root reparenting.
  moving_pool = non_root_ids if non_root_ids else active_ids
  moving_id = random.choice(moving_pool)
  parent_candidates = [pid for pid in active_ids if pid != moving_id]

  if not parent_candidates:
    parent_id = primary_root if moving_id != primary_root else None
    return parent_id, moving_id, "random_move"

  children_by_parent = {}
  for node in active_nodes:
    children_by_parent.setdefault(node.parent, []).append(node.child)

  # Parents that currently have no children can extend a chain-like shape.
  leaf_ids = [nid for nid in active_ids if nid not in children_by_parent]

  if tree_config == "wide":
    parent_id = primary_root if primary_root in parent_candidates else random.choice(parent_candidates)
  elif tree_config == "chain":
    chain_candidates = [nid for nid in leaf_ids if nid != moving_id]
    parent_id = random.choice(chain_candidates) if chain_candidates else random.choice(parent_candidates)
  elif tree_config == "hierarchical":
    root_children = children_by_parent.get(primary_root, [])
    hierarchy_candidates = [nid for nid in root_children if nid != moving_id]
    if hierarchy_candidates:
      parent_id = random.choice(hierarchy_candidates)
    elif primary_root in parent_candidates:
      parent_id = primary_root
    else:
      parent_id = random.choice(parent_candidates)
  else:
    if primary_root in parent_candidates and random.random() < 0.60:
      parent_id = primary_root
    else:
      parent_id = random.choice(parent_candidates)

  return parent_id, moving_id, "random_move"

def get_move_generator(config_name):
  """Get the appropriate move generator function based on configuration name"""
  generators = {
    "hierarchical": generate_hierarchical_move,
    "wide": generate_wide_tree_move,
    "chain": generate_deep_chain_move
  }
  return generators.get(config_name, generate_hierarchical_move)

# -----------------------------------------

# TODO: Create the function for the listener thread of the replica here.
#
# Signature:
# (
#   replica_obj,                       # The Replica object
#   zmq_context,                       # The ZeroMQ context for the sockets
#   shutdown_event,                    # Local shutdown signal for this listener
#   replica_info,                      # Replica info: (host, main_base, listener_base)
#   num_replicas,                      # Number of replicas in the system
#   hosts,                             # The list of hosts in the system
#   all_replicas_done_event,           # Set once a "DONE" has been seen from every peer
# )
#
# Phase 3 keeps the Phase 2 listener responsibilities. Behaviour summary:
#
#   - Sockets owned by this thread:
#       * MOVE-SUB (PUB-SUB), connected to every peer's MOVE-PUB
#         socket; subscribed to the "MOVE" topic.
#       * BARRIER-REP (REQ-REP), bound to the replica's listener
#         address. This socket carries DONE barrier messages.
#         The reply is always the literal string "ACK".
#
#   - On a "MOVE" message via MOVE-SUB:
#       Extract sender_id, timestamp, parent, metadata, child;
#       construct a MovePayload and call
#       `replica_obj.apply_remote_move(...)`.
#
#   - On a barrier request via BARRIER-REP:
#       Mark the sender as having issued DONE, reply with "ACK". Set
#       `all_replicas_done_event` once a DONE has been observed
#       from every peer. The local replica counts as having
#       signalled both events for itself.
#
#   - Exit when `shutdown_event` is set.
#
# Use a short poll timeout so the thread can observe
# `shutdown_event` promptly even when no peer is sending.

def run_replica( # NOTE: All the parameters are set here; do not modify them
  run_id,           # The random UUID for the running session of the system
  tree_config,      # Retrieved from .env
  replica_id,       # The ID of the replica
  replica_info,     # Replica info: (host, main_base, listener_base)
  raft_base,        # The base TCP port for the embedded RAFT gRPC server
  num_replicas,     # Number of replicas in the system
  hosts,            # The list of hosts in the system
  num_op_messages,  # Per-replica operation limits (Phase 3 op_limits, indexed by replica id)
):
  host, main_base, listener_base = replica_info
  num_messages = num_op_messages[replica_id]
  phase1_limit = num_messages // 2  # configured tree shape
  phase2_limit = num_messages        # then random moves/deletes
  op_limits = {rid: num_op_messages[rid] for rid in range(num_replicas)}

  # TODO: Create the Replica object.
  #       Phase 3 constructor signature:
  #         Replica(replica_id, host, main_base, listener_base,
  #                 num_replicas, raft_base, hosts, op_limits)
  #       See the Phase 3 PDF, section "The Replica Class", for the
  #       meaning of the new arguments.

  # TODO: Create the ZeroMQ context for the sockets.

  # TODO: Create and set up the MOVE-PUB socket (PUB-SUB), bound to the
  #       replica's main address. Used to broadcast MOVE operations.

  # Give the MOVE-PUB socket time to stabilise before other replicas
  # connect to it.
  time.sleep(1)

  # TODO: Create and set up one BARRIER-REQ socket (REQ-REP) PER PEER,
  #       each connected to that peer's listener address. This
  #       socket type is used for DONE messages.
  #       Set a recv() timeout (e.g. 5 seconds) on every BARRIER-REQ
  #       socket so a crashed peer cannot deadlock you.

  # TODO: Create the listener thread with the arguments described in
  #       the listener-function signature above, and start it. You
  #       will need to construct the barrier event (DONE)
  #       here and thread them through.

  # Wait for all replicas to bind their sockets.
  time.sleep(3)

  counter = 0
  move_generator = get_move_generator(tree_config)

  try:
    # 1. PROCESSING PHASE: generate `phase2_limit` local operations.
    while counter < phase2_limit:
      if counter < phase1_limit:
        parent_id, child_id, tree_type = move_generator(counter)
      else:
        parent_id, child_id, tree_type = generate_random_move_delete(replica_obj, counter, tree_config)

      # TODO: Apply the move locally and broadcast it.
      #
      # Example metadata structure (the "status" key is REQUIRED in
      # Phase 3; it carries "active" for moves and "deleted" for
      # deletes):
      #   {
      #     "count":   counter,
      #     "config":  tree_type,
      #     "replica": replica_obj.id,
      #     "status":  "deleted" if tree_type == "random_delete" else "active",
      #   }
      #
      # The Phase 2 `metadata["applied"]` key is no longer used in
      # Phase 3 -- see the Phase 3 PDF, section "Insertion Into the
      # Log and the Undo-Do-Redo Sequence".
      #
      # Broadcast via MOVE-PUB socket as a JSON object containing
      # at least {sender_id, timestamp, parent, metadata, child}.

      counter += 1
      time.sleep(1)

    # 2. PHASE 1 - BARRIER: DONE / ACK on top of completed local generation.
    #    See the Phase 3 PDF, section "Two-Phase Shutdown" -- this
    #    is the Phase 2 barrier kept in place.
    #
    # TODO: Generate the DONE message
    #       ({"sender_id": replica_id, "type": "DONE"}) and run the
    #       same exit-condition loop as in Phase 2:
    #         while not all_replicas_done_event.is_set()
    #               or received_acks != set(range(num_replicas)) - {replica_id}:
    #           for each peer not yet in received_acks:
    #             send DONE on its BARRIER-REQ socket
    #             try to recv() "ACK" within the timeout
    #               - on success: add peer to received_acks
    #               - on zmq.Again: close and recreate the socket
    #                 (a REQ socket is stuck in "awaiting reply"
    #                 state after a timeout)
    #         time.sleep(0.2)
    #
    # The barrier ends when every peer has acknowledged DONE.

    # 3. PHASE 2 - RAFT FLUSH: block until the RAFT-driven compaction frontier
    #    catches up with the total number of operations every replica
    #    has agreed to issue (sum of op_limits). See the Phase 3 PDF,
    #    section "Two-Phase Shutdown".
    #
    # TODO: success = replica_obj.flush_snapshot(timeout_seconds=30.0)
    #       Log a warning if `success` is False; do NOT abort -- the
    #       teardown sequence below still has to run.

  finally:
    # 4. PHASE 2 - TWO-STAGE RAFT TEARDOWN. The two stages exist for a specific
    #    reason -- see the Phase 3 PDF, section "Two-Phase Shutdown":
    #
    #      - close_raft_channels() stops the tick loop, joins the
    #        RAFT daemon thread, and drops the outbound gRPC stubs.
    #        After this call the replica issues no further RAFT
    #        RPCs.
    #      - close_raft_server() stops the inbound gRPC server.
    #        After this call the replica no longer accepts RAFT
    #        RPCs.
    #
    #    The order matters: if the inbound server were torn down
    #    first, peers whose `close_raft_channels()` had not yet
    #    fired would see their outbound calls time out.
    #
    # TODO: shutdown_event.set()
    #       listener_thread.join(2.0)
    #     if num_replicas > 1:
    #       replica_obj.close_raft_channels()
    #       replica_obj.close_raft_server()
    #       move_pub_socket.close()
    #       zmq_context.term()

    # NOTE: You are not allowed to modify the with block below.
    with open(f"runs/{run_id.hex}_replica_{replica_obj.id}.txt", "w") as final_file:
      final_file.write(f"[TREE STATE]\nReplica {replica_obj.id}, tree structure: {tree_config}, number of op messages: {num_messages}\n")
      final_file.write("\nLocal tree:\n" + pprint.pformat(replica_obj.tree(deleted=True)))
      final_file.write("\n\nTree snapshot:\n" + pprint.pformat(replica_obj.tree_snapshot(deleted=True)))

# -----------------------------------------

def main( # NOTE: All the parameters are set here; do not modify them
  num_replicas,
  tree_config,
  hosts,
  main_base,
  listener_base,
  raft_base,
  num_op_messages,
):
  # A run ID is generated as a random UUID for the current run of the system.
  run_id = uuid.uuid4()

  # The directory to write the final tree state is created beforehand.
  os.makedirs("runs", exist_ok=True)

  # TODO: Create and start the replica processes (one per local host).
  #       Each process target is `run_replica` with the per-replica
  #       arguments. Use `multiprocessing.Process(...)` and only
  #       launch processes for replica IDs whose host is local
  #       (typically `127.0.0.1`).
  #
  # TODO: Join the replica processes.

# -----------------------------------------

if __name__ == "__main__":
  load_dotenv()

  if os.getenv("HOSTS") is None:
    exit(os.EX_USAGE)

  hosts = parse_hosts(str(os.getenv("HOSTS")))
  num_replicas = len(hosts)

  try:
    if os.getenv("MAIN_BASE") is None:
      exit(os.EX_USAGE)

    main_base = int(str(os.getenv("MAIN_BASE")))
  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)

  try:
    if os.getenv("LISTENER_BASE") is None:
      exit(os.EX_USAGE)

    listener_base = int(str(os.getenv("LISTENER_BASE")))
  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)

  try:
    if os.getenv("RAFT_BASE") is None:
      exit(os.EX_USAGE)

    raft_base = int(str(os.getenv("RAFT_BASE")))
  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)

  try:
    if os.getenv("NUM_OP_MESSAGES") is None:
      exit(os.EX_USAGE)

    num_op_messages = parse_numbers(str(os.getenv("NUM_OP_MESSAGES")))

    if len(num_op_messages) != num_replicas:
      print("NUM_OP_MESSAGES must have one entry per host in HOSTS")
      exit(os.EX_USAGE)

    if any(n < 0 for n in num_op_messages):
      print("NUM_OP_MESSAGES entries must be non-negative integers")
      exit(os.EX_USAGE)

  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)

  try:
    if os.getenv("TREE_CONFIG") is None:
      exit(os.EX_USAGE)

    tree_config = str(os.getenv("TREE_CONFIG"))

    if (tree_config != "hierarchical") and (tree_config != "wide") and (tree_config != "chain"):
      print("TREE_CONFIG must be \"hierarchical\", \"wide\", or \"chain\"")
      exit(os.EX_USAGE)

  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)

  main(num_replicas, tree_config, hosts, main_base, listener_base, raft_base, num_op_messages)
