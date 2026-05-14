import json
import threading
import unittest
from unittest.mock import patch

import main
from tree_crdt.replica import Replica

class _FakeSocket:
  def __init__(self, incoming: list[list[bytes]] | None = None) -> None:
    self.incoming = incoming or []
    self.sent: list[list[bytes]] = []
    self.subscriptions: list[bytes] = []
    self.options: dict[int, str] = dict()

  def connect(self, addr: str) -> None:
    return None

  def subscribe(self, topic: bytes) -> None:
    self.subscriptions.append(topic)

  def bind(self, addr: str) -> None:
    return None

  def poll(self, timeout: int) -> int:
    return 1 if self.incoming else 0

  def recv_string(self, flags: int = 0) -> str:
    if not self.incoming:
      raise RuntimeError("no incoming message")
    parts = self.incoming.pop(0)
    # Join parts with space if it was multipart or just return first if it's the expected format
    if len(parts) == 2:
      return f"{parts[0].decode()} {parts[1].decode()}"
    return parts[0].decode()

  def send_string(self, message: str) -> None:
    self.sent.append(message)

  def recv_multipart(self, flags: int = 0) -> list[bytes]:
    if not self.incoming:
      import zmq
      if flags & zmq.NOBLOCK:
        raise zmq.Again()
      raise RuntimeError("no incoming message")
    return self.incoming.pop(0)

  def send_multipart(self, message: list[bytes]) -> None:
    self.sent.append(message)

  def setsockopt_string(self, option: int, value: str) -> None:
    self.options[option] = value

  def setsockopt(self, option: int, value: bytes) -> None:
    self.options[option] = value

  def close(self, linger: int = 0) -> None:
    return None


class _FakePoller:
  def __init__(self) -> None:
    self.sockets: list[_FakeSocket] = []

  def register(self, socket: "_FakeSocket", flags: int = 0) -> None:
    self.sockets.append(socket)

  def poll(self, timeout: int | None = None) -> dict["_FakeSocket", int]:
    result = {}
    for s in self.sockets:
      if s.incoming:
        result[s] = 1
    return result

class _FakeContext:
  def __init__(self, sub_socket: _FakeSocket, pub_socket: _FakeSocket) -> None:
    self._sub_socket = sub_socket
    self._pub_socket = pub_socket

  def socket(self, socket_type: int) -> _FakeSocket:
    # from zmq enum:
    # PUB = 1
    # SUB = 2

    if socket_type == 1:
      return self._pub_socket
    elif socket_type == 2:
      return self._sub_socket
    
class TestAdvancedScenarioA4ListenerDoneAckProtocol(unittest.TestCase):
  def test_done_message_triggers_ack_and_sets_done_event(self) -> None:
    done_payload = json.dumps({"sender_id": 1, "timestamp": 9}).encode()
    sub_socket = _FakeSocket(incoming=[[b"DONE", done_payload]])
    pub_socket = _FakeSocket()
    fake_context = _FakeContext(sub_socket=sub_socket, pub_socket=pub_socket)

    replica = Replica(id=0, host="127.0.0.1", main_base=5630, listener_base=6630)
    shutdown_event = threading.Event()
    all_done_event = threading.Event()

    with patch("main.time.sleep", return_value=None), \
         patch("main.zmq.Poller", return_value=_FakePoller()):
      listener_thread = threading.Thread(
        target=main.replica_listener,
        args=(
          replica,
          fake_context,
          shutdown_event,
          ("127.0.0.1", 5630, 6630),
          2,
          ["127.0.0.1", "127.0.0.2"],
          all_done_event,
        ),
      )

      listener_thread.start()
      for _ in range(2000): # If still does not work, try to increase this timeout
        if all_done_event.is_set() and pub_socket.sent:
          break
      shutdown_event.set()
      listener_thread.join(timeout=2)

    self.assertTrue(all_done_event.is_set())
    self.assertGreaterEqual(len(pub_socket.sent), 1)

    # Content is like [b"ACK", b'{"sender_id": 0, "timestamp": 10, "ack_to": 1, "ack_timestamp": 9}']
    full_message = pub_socket.sent[0]
    self.assertIsInstance(full_message, list)
    self.assertEqual(len(full_message), 2)
    
    topic = full_message[0].decode()
    payload_str = full_message[1].decode()
    self.assertEqual(topic, "ACK")

    ack = json.loads(payload_str)
    self.assertEqual(ack["ack_to"], 1)
    self.assertEqual(ack["sender_id"], 0)

if __name__ == "__main__":
  unittest.main()