from __future__ import annotations

from concurrent import futures
from typing import Any, Callable, Iterable, Protocol, TypeVar
import json

import grpc

from . import raft_pb2, raft_pb2_grpc
from .log import LogEntry
from .messages import AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse
from .messages import InstallSnapshot, InstallSnapshotResponse
from .node import RaftNode

CommandT = TypeVar("CommandT")
EncodeFn = Callable[[CommandT], bytes]
DecodeFn = Callable[[bytes], CommandT]

class Transport(Protocol):
  def send_request_vote(self, target_id: int, message: RequestVote) -> None: ...

  def send_append_entries(self, target_id: int, message: AppendEntries) -> None: ...


class CommandCodec:
  def __init__(self, encode: EncodeFn[CommandT], decode: DecodeFn[CommandT]) -> None:
    self.encode = encode
    self.decode = decode


def _default_encode(command: Any) -> bytes:
  if command is None:
    return b""
  if isinstance(command, (bytes, bytearray)):
    return bytes(command)
  return json.dumps(command, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _default_decode(data: bytes) -> Any:
  if not data:
    return None
  try:
    return json.loads(data.decode("utf-8"))
  except json.JSONDecodeError:
    return data


def _default_codec() -> CommandCodec[Any]:
  return CommandCodec(_default_encode, _default_decode)


class _RaftServiceServicer(raft_pb2_grpc.RaftServiceServicer):
  def __init__(self, node: RaftNode[Any], codec: CommandCodec[Any]) -> None:
    self.__node: RaftNode[Any] = node
    self.__codec: CommandCodec[Any] = codec

  def RequestVoteRpc(
    self,
    request: raft_pb2.RequestVote,
    _context: grpc.ServicerContext,
  ) -> raft_pb2.RequestVoteResponse:
    message = RequestVote(
      term=request.term,
      candidate_id=request.candidate_id,
      last_log_index=request.last_log_index,
      last_log_term=request.last_log_term,
    )
    response = self.__node.handle_request_vote(message)
    return raft_pb2.RequestVoteResponse(
      term=response.term,
      voter_id=response.voter_id,
      vote_granted=response.vote_granted,
    )

  def AppendEntriesRpc(
    self,
    request: raft_pb2.AppendEntries,
    _context: grpc.ServicerContext,
  ) -> raft_pb2.AppendEntriesResponse:
    entries = [
      LogEntry(term=entry.term, command=self.__codec.decode(entry.command))
      for entry in request.entries
    ]
    message = AppendEntries(
      term=request.term,
      leader_id=request.leader_id,
      prev_log_index=request.prev_log_index,
      prev_log_term=request.prev_log_term,
      entries=entries,
      leader_commit=request.leader_commit,
    )
    response = self.__node.handle_append_entries(message)
    return raft_pb2.AppendEntriesResponse(
      term=response.term,
      follower_id=response.follower_id,
      success=response.success,
      match_index=response.match_index,
    )

  def InstallSnapshotRpc(
    self,
    request: raft_pb2.Snapshot,
    _context: grpc.ServicerContext,
  ) -> raft_pb2.InstallSnapshotResponse:
    message = InstallSnapshot(
      term=request.term,
      leader_id=request.leader_id,
      last_included_index=request.last_included_index,
      last_included_term=request.last_included_term,
      data=request.data,
    )
    response = self.__node.handle_install_snapshot(message)
    return raft_pb2.InstallSnapshotResponse(term=response.term, follower_id=response.follower_id)



class GrpcRaftServer:
  def __init__(
    self,
    node: RaftNode[Any],
    address: str,
    codec: CommandCodec[Any] | None = None,
    max_workers: int = 4,
  ) -> None:
    self.__node: RaftNode[Any] = node
    self.__codec: CommandCodec[Any] = codec or _default_codec()
    self.__server: grpc.Server = grpc.server(
      futures.ThreadPoolExecutor(max_workers=max_workers)
    )

    servicer = _RaftServiceServicer(node, self.__codec)
    raft_pb2_grpc.add_RaftServiceServicer_to_server(servicer, self.__server)

    # InstallSnapshotRpc is handled by the generated servicer method above.

    self.__bound_port = self.__server.add_insecure_port(address)

  @property
  def bound_port(self) -> int:
    return self.__bound_port

  def start(self) -> None:
    self.__server.start()

  def stop(self, grace: float | None = None) -> None:
    return self.__server.stop(grace=grace)


class GrpcTransport(Transport):
  def __init__(
    self,
    node: RaftNode[Any],
    peer_addresses: dict[int, str],
    codec: CommandCodec[Any] | None = None,
    channel_options: Iterable[tuple[str, int]] | None = None,
  ) -> None:
    self.__node: RaftNode[Any] = node
    self.__peer_addresses: dict[int, str] = peer_addresses
    self.__codec: CommandCodec[Any] = codec or _default_codec()
    self.__channel_options: list[tuple[str, int]] = (
      list(channel_options) if channel_options else []
    )
    self.__channels: dict[int, grpc.Channel] = {}
    self.__stubs: dict[int, raft_pb2_grpc.RaftServiceStub] = {}

  def __log(self, message: str) -> None:
    print(f"[RAFT transport node={self.__node.id}] {message}", flush=True)

  def close(self) -> None:
    #for channel in self.__channels.values():
    #  channel.close()
    #self.__channels.clear()
    self.__stubs.clear()

  def send_request_vote(self, target_id: int, message: RequestVote) -> None:
    try:
      stub = self.__stub_for(target_id)
      request = raft_pb2.RequestVote(
        term=message.term,
        candidate_id=message.candidate_id,
        last_log_index=message.last_log_index,
        last_log_term=message.last_log_term,
      )
      response = stub.RequestVoteRpc(request, timeout=0.5)
      self.__node.handle_request_vote_response(
        RequestVoteResponse(
          term=response.term,
          voter_id=response.voter_id,
          vote_granted=response.vote_granted,
        )
      )
    except grpc.RpcError as grpc_err:
      self.__log(f"RequestVote RPC to peer={target_id} failed: {grpc_err}")
      return
    except OSError as os_err:
      self.__log(f"RequestVote transport to peer={target_id} unavailable: {os_err}")
      return

  def send_append_entries(self, target_id: int, message: AppendEntries) -> None:
    try:
      if message.entries:
        self.__log(
          f"sending AppendEntries to peer={target_id} term={message.term} prev=({message.prev_log_index},{message.prev_log_term}) entries={len(message.entries)} commit={message.leader_commit}"
        )
      stub = self.__stub_for(target_id)
      request = raft_pb2.AppendEntries(
        term=message.term,
        leader_id=message.leader_id,
        prev_log_index=message.prev_log_index,
        prev_log_term=message.prev_log_term,
        entries=[
          raft_pb2.LogEntry(term=entry.term, command=self.__codec.encode(entry.command))
          for entry in message.entries
        ],
        leader_commit=message.leader_commit,
      )
      response = stub.AppendEntriesRpc(request, timeout=0.5)
      if message.entries:
        self.__log(
          f"received AppendEntries response from peer={target_id} term={response.term} success={response.success} match_index={response.match_index}"
        )
      self.__node.handle_append_entries_response(
        AppendEntriesResponse(
          term=response.term,
          follower_id=response.follower_id,
          success=response.success,
          match_index=response.match_index,
        )
      )
    except grpc.RpcError as grpc_err:
      self.__log(f"AppendEntries RPC to peer={target_id} failed: {grpc_err}")
      return
    except OSError as os_err:
      self.__log(f"AppendEntries transport to peer={target_id} unavailable: {os_err}")
      return

  def send_install_snapshot(self, target_id: int, message: InstallSnapshot) -> None:
    try:
      stub = self.__stub_for(target_id)
      request = raft_pb2.Snapshot(
        term=message.term,
        leader_id=message.leader_id,
        last_included_index=message.last_included_index,
        last_included_term=message.last_included_term,
        data=message.data,
      )
      response = stub.InstallSnapshotRpc(request, timeout=0.5)
      self.__log(f"received InstallSnapshot response from peer={target_id} term={response.term} follower_id={response.follower_id}")
      self.__node.handle_install_snapshot_response(InstallSnapshotResponse(term=response.term, follower_id=response.follower_id))
    except grpc.RpcError as grpc_err:
      self.__log(f"InstallSnapshot RPC to peer={target_id} failed: {grpc_err}")
      return
    except OSError as os_err:
      self.__log(f"InstallSnapshot transport to peer={target_id} unavailable: {os_err}")
      return

  def __stub_for(self, peer_id: int) -> raft_pb2_grpc.RaftServiceStub:
    if peer_id not in self.__stubs:
      address = self.__peer_addresses[peer_id]
      self.__log(f"opening channel to peer={peer_id} at {address}")
      channel = grpc.insecure_channel(address, options=self.__channel_options)
      self.__channels[peer_id] = channel
      self.__stubs[peer_id] = raft_pb2_grpc.RaftServiceStub(channel)
    return self.__stubs[peer_id]
