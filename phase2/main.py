import json
import multiprocessing
import os
import pprint
import random
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


def validate_ip(ip):
  pattern = r'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'
  return re.match(pattern, ip) is not None


def parse_hosts(host_str):
  return [host for host in host_str.split(",") if validate_ip(host)]


def generate_hierarchical_move(counter):
  """Config 1: Hierarchical tree (Root -> Children -> Grandchildren)."""
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
  """Config 2: Wide tree with many siblings."""
  parent_id = None if counter == 0 else 0
  child_id = counter
  tree_type = "root" if counter == 0 else "wide_child"

  return parent_id, child_id, tree_type


def generate_deep_chain_move(counter):
  """Config 3: Deep chain with linear parent-child relationships."""
  parent_id = counter - 1 if counter > 0 else None
  child_id = counter
  tree_type = "chain_node"

  return parent_id, child_id, tree_type


def generate_random_move_delete(replica, counter):
  """Generate random Phase 2 moves and tombstone deletes."""
  tree = replica.tree
  current_nodes = [
    node_id
    for node_id in tree
    if isinstance(node_id, int) and node_id >= 0
  ]

  if not current_nodes or counter < 2 or random.random() < 0.1:
    child_id = (replica.id * 1000) + counter + 50
    return None, child_id, "random_root"

  if random.random() < 0.2:
    child_to_delete = random.choice(current_nodes)
    return None, child_to_delete, "random_delete"

  child_id = random.choice(current_nodes)
  potential_parents = [None] + current_nodes
  parent_id = random.choice(potential_parents)
  return parent_id, child_id, "random_move"


def get_move_generator(config_name):
  generators = {
    "hierarchical": generate_hierarchical_move,
    "wide": generate_wide_tree_move,
    "chain": generate_deep_chain_move,
  }
  return generators.get(config_name, generate_hierarchical_move)


def _message_get(message, *keys, default=None):
  for key in keys:
    if key in message:
      return message[key]
  return default


def _strip_wire_metadata(metadata):
  clean = dict(metadata)
  clean.pop("last_ts", None)
  return clean


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

  done_rep = zmq_context.socket(zmq.REP)
  done_rep.bind(replica_obj.listener_addr)

  for peer_id, peer_host in enumerate(hosts):
    if peer_id != replica_obj.id:
      move_sub.connect(f"tcp://{peer_host}:{main_base + peer_id}")

  poller = zmq.Poller()
  poller.register(move_sub, zmq.POLLIN)
  poller.register(done_rep, zmq.POLLIN)
  replicas_done: set[int] = set()

  try:
    while not shutdown_event.is_set():
      events = dict(poller.poll(100))

      if move_sub in events:
        try:
          topic, message = _receive_topic_message(move_sub)
        except (ValueError, TypeError, json.JSONDecodeError):
          topic, message = None, {}

        if topic == "MOVE":
          try:
            sender_id = _message_get(message, "sender_id", "sender id")
            timestamp = message["timestamp"]
            metadata = dict(message["metadata"])
            progress = _message_get(
              message,
              "last_ts",
              default=metadata.get("last_ts", timestamp),
            )
            replica_obj.record_last_timestamp(sender_id, progress)
            payload = MovePayload(
              i=sender_id,
              t=timestamp,
              p=message["parent"],
              m=_strip_wire_metadata(metadata),
              c=message["child"],
            )
          except (KeyError, TypeError, ValueError):
            payload = None

          if payload is not None:
            replica_obj.apply_remote_move(payload)

      if done_rep in events:
        try:
          message = done_rep.recv_json()
          sender_id = _message_get(message, "sender_id", "sender id")
          sender_timestamp = message["timestamp"]
          replica_obj.tick_clock(sender_timestamp)
          replica_obj.record_last_timestamp(sender_id, sender_timestamp)
          if sender_id != replica_obj.id:
            replicas_done.add(sender_id)
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
          pass

        done_rep.send_string("ACK")

        if len(replicas_done) >= max(0, num_replicas - 1):
          all_replicas_done_event.set()
  finally:
    move_sub.close(0)
    done_rep.close(0)


def run_replica(
  run_id,
  tree_config,
  replica_id,
  replica_info,
  num_replicas,
  hosts,
  max_timestamp,
) -> None:
  _require_zmq()

  replica = Replica(
    id=replica_id,
    host=replica_info[0],
    main_base=replica_info[1],
    listener_base=replica_info[2],
    num_replicas=num_replicas,
  )
  shutdown_event = threading.Event()
  all_replicas_done_event = threading.Event()

  zmq_context = zmq.Context()
  move_pub_socket = zmq_context.socket(zmq.PUB)
  move_pub_socket.bind(replica.main_addr)

  def make_done_socket(peer_id):
    socket = zmq_context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(f"tcp://{hosts[peer_id]}:{replica_info[2] + peer_id}")
    return socket

  done_req_sockets = {
    peer_id: make_done_socket(peer_id)
    for peer_id in range(num_replicas)
    if peer_id != replica.id
  }

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

  time.sleep(3)
  counter = 0
  move_generator = get_move_generator(tree_config)

  try:
    while True:
      current_timestamp = replica.current_timestamp()
      is_at_max = False
      is_phase2 = False

      if max_timestamp is not None:
        is_at_max = counter >= 2 * max_timestamp
        is_phase2 = counter >= max_timestamp

      if is_at_max:
        break

      if is_phase2 and max_timestamp is not None:
        local_vclock = current_timestamp if isinstance(current_timestamp, dict) else None
        others_ready = True

        for other_id in range(num_replicas):
          if other_id == replica.id:
            continue

          peer_ts_entry = replica.get_peer_timestamp(other_id)
          vclock_entry = local_vclock.get(other_id, 0) if local_vclock else 0
          if isinstance(peer_ts_entry, dict):
            last_val = peer_ts_entry.get(other_id, 0)
          else:
            last_val = peer_ts_entry or 0

          if max(last_val, vclock_entry) < max_timestamp:
            others_ready = False
            break

        if not others_ready:
          time.sleep(0.5)
          continue

      if is_phase2:
        parent_id, child_id, tree_type = generate_random_move_delete(replica, counter)
      else:
        parent_id, child_id, tree_type = move_generator(counter)

      metadata = {
        "count": counter,
        "config": tree_type,
        "replica": replica.id,
        "status": "deleted" if tree_type == "random_delete" else "active",
      }
      move_payload = replica.apply_local_move(parent_id, metadata, child_id)
      last_ts = replica.current_timestamp()
      _send_topic_message(
        move_pub_socket,
        "MOVE",
        {
          "sender_id": replica.id,
          "timestamp": move_payload.timestamp,
          "parent": move_payload.parent,
          "metadata": move_payload.metadata,
          "child": move_payload.child,
          "last_ts": last_ts,
        },
      )

      counter += 1
      time.sleep(0.05)

    done_message = {
      "sender_id": replica.id,
      "timestamp": replica.current_timestamp(),
    }

    if num_replicas > 1:
      received_acks: set[int] = set()
      failed_to_send_acks: set[int] = set()
      all_replica_ids = set(range(num_replicas))

      while True:
        for peer_id in range(num_replicas):
          if peer_id == replica.id or peer_id in received_acks:
            continue

          socket = done_req_sockets[peer_id]
          try:
            socket.send_json(done_message)
            ack = socket.recv_string()
            if ack == "ACK":
              received_acks.add(peer_id)
              failed_to_send_acks.discard(peer_id)
          except zmq.Again:
            failed_to_send_acks.add(peer_id)
            socket.close(0)
            done_req_sockets[peer_id] = make_done_socket(peer_id)

        if (
          all_replicas_done_event.is_set()
          and (received_acks | failed_to_send_acks | {replica.id}) == all_replica_ids
        ):
          break

        time.sleep(0.1)

    # Let listener threads drain MOVE messages that were published just before DONE.
    time.sleep(1.0)
  finally:
    shutdown_event.set()
    listener.join()
    replica.finalize()
    for socket in done_req_sockets.values():
      socket.close(0)
    move_pub_socket.close(0)
    zmq_context.term()

  with open(f"runs/{run_id.hex}_replica_{replica.id}.txt", "w") as final_file:
    final_file.write(
      f"[TREE STATE]\nReplica {replica.id}, tree structure: {tree_config}, "
      f"maximum timestamp: {max_timestamp}\n"
    )
    final_file.write(pprint.pformat(replica.tree(deleted=True)))


def main(
  num_replicas,
  tree_config,
  hosts,
  main_base,
  listener_base,
  max_timestamp=None,
) -> None:
  run_id = uuid.uuid4()
  os.makedirs("runs", exist_ok=True)

  processes = []
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
    if os.getenv("MAX_TIMESTAMP") is None:
      exit(os.EX_USAGE)
    max_timestamp = int(str(os.getenv("MAX_TIMESTAMP")))
    if max_timestamp < 0:
      print("MAX_TIMESTAMP must be a non-negative integer")
      exit(os.EX_USAGE)
  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)

  try:
    if os.getenv("TREE_CONFIG") is None:
      exit(os.EX_USAGE)
    tree_config = str(os.getenv("TREE_CONFIG"))
    if tree_config not in {"hierarchical", "wide", "chain"}:
      print('TREE_CONFIG must be "hierarchical", "wide", or "chain"')
      exit(os.EX_USAGE)
  except ValueError as err:
    print(err)
    exit(os.EX_USAGE)

  main(num_replicas, tree_config, hosts, main_base, listener_base, max_timestamp)
