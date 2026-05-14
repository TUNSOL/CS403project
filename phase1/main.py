# NOTE: You can add new imports if you need
import json
import multiprocessing
import os
import pprint
import re
import sys
import threading
import time
import uuid

try:
  import zmq
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
  zmq = None

try:
  from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
  def load_dotenv():
    return False

try:
  from tree_crdt import Replica
  from tree_crdt.payload import MovePayload
except ModuleNotFoundError:  # pragma: no cover - convenient local fallback
  sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
  from tree_crdt import Replica
  from tree_crdt.payload import MovePayload

def _require_zmq():
  if zmq is None:
    raise RuntimeError("pyzmq is required to run main.py")

def _send_topic_message(socket, topic, payload):
  socket.send_multipart([
    topic.encode("utf-8"),
    json.dumps(payload).encode("utf-8"),
  ])

def _receive_topic_message(socket):
  topic_raw, payload_raw = socket.recv_multipart()
  return topic_raw.decode("utf-8"), json.loads(payload_raw.decode("utf-8"))

def _message_field(message, *keys):
  # The original implementation used keys like "sender id", while the
  # group5 DONE/ACK test sends snake_case keys. Accept both for protocol
  # compatibility instead of dropping a valid DONE message as malformed.
  for key in keys:
    if key in message:
      return message[key]
  raise KeyError(keys[0])

# -----------------------------------------

# Helper functions:
def validate_ip(ip):
  pattern = r'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'
  return re.match(pattern, ip) is not None

def parse_hosts(host_str):
  return [host for host in host_str.split(",") if validate_ip(host)]

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

def get_move_generator(config_name):
  """Get the appropriate move generator function based on configuration name"""
  generators = {
    "hierarchical": generate_hierarchical_move,
    "wide": generate_wide_tree_move,
    "chain": generate_deep_chain_move
  }
  return generators.get(config_name, generate_hierarchical_move)  # Default to hierarchical

# -----------------------------------------

# TODO: Create the function for the listener thread of the replica here
# Signature:
# (
#  replica_obj, # The Replica object
#  zmq_context, # The ZeroMQ context for the sockets
#  shutdown_event, # The system shutdown event for the replica
#  replica_info, # Replica info: (host, main_base, listener_base)
#  num_replicas, # Number of replicas in the system
#  hosts, # The list of hosts in the system
#  all_replicas_done_event # The event that all replicas have reached the maximum timestamp
# )
# The function should not return any value
# See the PDF for a detailed description of the function
def listener_thread(
  replica_obj,
  zmq_context,
  shutdown_event,
  replica_info,
  num_replicas,
  hosts,
  all_replicas_done_event,
) -> None:
  _require_zmq()

  _, main_base, _ = replica_info

  move_sub = zmq_context.socket(zmq.SUB)
  move_sub.setsockopt(zmq.SUBSCRIBE, b"MOVE")
  move_sub.setsockopt(zmq.SUBSCRIBE, b"DONE")

  ack_pub = zmq_context.socket(zmq.PUB)
  ack_pub.bind(replica_obj.listener_addr)

  for peer_id, peer_host in enumerate(hosts):
    if peer_id == replica_obj.id:
      continue

    # MOVE messages are published from the peers' main-thread addresses.
    move_sub.connect(f"tcp://{peer_host}:{main_base + peer_id}")

  poller = zmq.Poller()
  poller.register(move_sub, zmq.POLLIN)

  replicas_done: set[int] = set()

  try:
    while not shutdown_event.is_set():
      events = dict(poller.poll(100))
      if move_sub not in events:
        continue

      try:
        topic, message = _receive_topic_message(move_sub)
      except (ValueError, TypeError, json.JSONDecodeError):
        continue

      if topic == "MOVE":
        try:
          payload = MovePayload(
            i=_message_field(message, "sender_id", "sender id"),
            t=message["timestamp"],
            p=message["parent"],
            m=message["metadata"],
            c=message["child"],
          )
        except KeyError:
          continue

        replica_obj.apply_remote_move(payload)
        continue

      if topic != "DONE":
        continue

      try:
        sender_id = _message_field(message, "sender_id", "sender id")
        sender_timestamp = message["timestamp"]
      except KeyError:
        continue

      if sender_id == replica_obj.id:
        continue

      replicas_done.add(sender_id)
      replica_obj.tick_clock(sender_timestamp)

      ack_message = {
        # Include both key styles so older Phase 1 code and the group5
        # DONE/ACK test can parse the ACK response.
        "sender_id": replica_obj.id,
        "sender id": replica_obj.id,
        "timestamp": replica_obj.current_timestamp(),
        "ack_to": sender_id,
        "ack to": sender_id,
        "ack_timestamp": sender_timestamp,
        "ack timestamp": sender_timestamp,
      }
      _send_topic_message(ack_pub, "ACK", ack_message)

      if len(replicas_done) >= max(0, num_replicas - 1):
        all_replicas_done_event.set()
  finally:
    move_sub.close(0)
    ack_pub.close(0)

# Some scenario tests and older Phase 1 handouts call the listener
# `replica_listener`. Keep the alias so they exercise the same implementation.
replica_listener = listener_thread

def run_replica( # NOTE: All the parameters are set here; do not modify them
  run_id, # The random UUID for the running session of the system
  tree_config, # Retrieved from .env
  replica_id, # The ID of the replica
  replica_info, # Replica info: (host, main_base, listener_base)
  num_replicas, # Number of replicas in the system
  hosts, # The list of hosts in the system
  max_timestamp, # The specified maximum timestamp
) -> None:
  _require_zmq()

  replica = Replica(
    id=replica_id,
    host=replica_info[0],
    main_base=replica_info[1],
    listener_base=replica_info[2],
  )
  shutdown_event = threading.Event()
  all_replicas_done_event = threading.Event()

  zmq_context = zmq.Context()
  move_pub = zmq_context.socket(zmq.PUB)
  move_pub.bind(replica.main_addr)

  # Give the socket time to stabilize before other replicas connect
  time.sleep(1)

  ack_sub = zmq_context.socket(zmq.SUB)
  ack_sub.setsockopt(zmq.SUBSCRIBE, b"ACK")
  for peer_id, peer_host in enumerate(hosts):
    if peer_id == replica.id:
      continue

    ack_sub.connect(f"tcp://{peer_host}:{replica_info[2] + peer_id}")

  listener = threading.Thread(
    target=listener_thread,
    name=f"Replica-{replica.id}-listener",
    args=(
      replica,
      zmq_context,
      shutdown_event,
      replica_info,
      num_replicas,
      hosts,
      all_replicas_done_event,
    ),
  )
  listener.start()

  # Wait for all replicas to bind their sockets
  time.sleep(3)

  try:
    # A counter you can use for metadata purposes
    counter: int = 0
    
    # The tree configuration and the associated generator are retrieved
    move_generator = get_move_generator(tree_config)

    # Continue until the maximum timestamp is reached (if it is not set, run indefinitely)
    while (max_timestamp is None) or (replica.current_timestamp() < max_timestamp):
      parent_id, child_id, tree_type = move_generator(counter)

      metadata = {
        "count": counter,
        "timestamp": replica.current_timestamp() + 1,
        "config": tree_type,
        "replica": replica.id,
      }
      op = replica.apply_local_move(parent_id, metadata, child_id)

      move_message = {
        # Keep both protocol key styles on the wire for backward compatibility.
        "sender_id": op.id,
        "sender id": op.id,
        "timestamp": op.timestamp,
        "parent": op.parent,
        "metadata": op.metadata,
        "child": op.child,
      }
      _send_topic_message(move_pub, "MOVE", move_message)

      counter += 1
      time.sleep(0.05)

    acked_replicas: set[int] = set()
    ack_poller = zmq.Poller()
    ack_poller.register(ack_sub, zmq.POLLIN)

    # CHANGE 1: only one replica --> we will never receive DONE from others
    if num_replicas == 1:
      all_replicas_done_event.set()

    # CHANGE 2: if num_replicas > 1 condition is removed
    expected_acks = {rid for rid in range(num_replicas) if rid != replica.id}


    while not (expected_acks.issubset(acked_replicas) and all_replicas_done_event.is_set()):  # ← CHANGE 3
        done_message = {
            # The listener accepts either key; publishing both keeps mixed
            # implementations from missing DONE messages.
            "sender_id": replica.id,
            "sender id": replica.id,
            "timestamp": replica.current_timestamp(),
        }
        _send_topic_message(move_pub, "DONE", done_message)

        events = dict(ack_poller.poll(300))
        if ack_sub not in events:
            continue

        try:
            topic, message = _receive_topic_message(ack_sub)
        except (ValueError, TypeError, json.JSONDecodeError):
            continue

        if topic != "ACK":
            continue

        try:
            sender_id = _message_field(message, "sender_id", "sender id")
            ack_to = _message_field(message, "ack_to", "ack to")
        except KeyError:
            continue

        if sender_id == replica.id or ack_to != replica.id:
            continue

        acked_replicas.add(sender_id)
        # CHANGE 3: no break here

    time.sleep(1.0)
    shutdown_event.set()

  finally:
    listener.join()
    ack_sub.close(0)
    move_pub.close(0)
    zmq_context.term()

  # NOTE: You are not allowed to modify the with block below
  with open(f"runs/{run_id.hex}_replica_{replica.id}.txt", "w") as final_file:
    final_file.write(f"[TREE STATE]\nReplica {replica.id}, tree structure: {tree_config}, maximum timestamp: {max_timestamp}\n")
    final_file.write(pprint.pformat(replica.tree()))

# -----------------------------------------
def main( # NOTE: All the parameters are set here; do not modify them
  num_replicas,
  tree_config,
  hosts,
  main_base,
  listener_base,
  max_timestamp = None,
) -> None:
  # A run ID is generated as a random UUID for the current run of the system
  run_id: uuid.UUID = uuid.uuid4()
 
  # The directory to write final tree state is created beforehand
  os.makedirs("runs", exist_ok = True)

  processes = []

  # NOTE: You should not create replica processes for remote replicas if you use any
  for replica_id, host in enumerate(hosts[:num_replicas]):
    if host != "127.0.0.1":
      continue

    process = multiprocessing.Process(
      target=run_replica,
      name=f"Replica-{replica_id}",
      args=(
        run_id,
        tree_config,
        replica_id,
        (host, main_base, listener_base),
        num_replicas,
        hosts,
        max_timestamp,
      ),
    )
    processes.append(process)

  for process in processes:
    process.start()

  for process in processes:
    process.join()

# -----------------------------------------

if __name__ == "__main__":
  load_dotenv()

  if os.getenv("HOSTS") is None:
    exit(os.EX_USAGE)

  hosts: list[str] = parse_hosts(str(os.getenv("HOSTS")))
  num_replicas: int = len(hosts)

  try:
    if os.getenv("MAIN_BASE") is None:
      exit(os.EX_USAGE)

    main_base: int = int(str(os.getenv("MAIN_BASE")))
  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)

  try:
    if os.getenv("LISTENER_BASE") is None:
      exit(os.EX_USAGE)

    listener_base: int = int(str(os.getenv("LISTENER_BASE")))
  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)

  try:
    if os.getenv("MAX_TIMESTAMP") is None:
      exit(os.EX_USAGE)

    max_timestamp: int = int(str(os.getenv("MAX_TIMESTAMP")))

    if max_timestamp < 0:
      print("MAX_TIMESTAMP must be a non-negative integer")
      exit(os.EX_USAGE)

  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)

  try:
    if os.getenv("TREE_CONFIG") is None:
      exit(os.EX_USAGE)

    tree_config: str = str(os.getenv("TREE_CONFIG"))

    if (tree_config != "hierarchical") and (tree_config != "wide") and tree_config != "chain":
      print("TREE_CONFIG must be \"hierarchical\", \"wide\", or \"chain\"")
      exit(os.EX_USAGE)

  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)
  
  main(num_replicas, tree_config, hosts, main_base, listener_base, max_timestamp)
