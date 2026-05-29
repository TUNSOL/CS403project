from __future__ import annotations

import random
import threading

from dataclasses import dataclass
from enum import Enum
from typing import Callable, Generic, TypeVar

from .log import LogEntry, RaftLog
from .messages import (
  AppendEntries,
  AppendEntriesResponse,
  RequestVote,
  RequestVoteResponse,
  InstallSnapshot,
  InstallSnapshotResponse,
)
  

CommandT = TypeVar("CommandT")


class Role(Enum):
  FOLLOWER = "follower"
  CANDIDATE = "candidate"
  LEADER = "leader"


@dataclass(frozen=True)
class NodeState:
  term: int
  role: Role
  commit_index: int
  last_applied: int


class RaftNode(Generic[CommandT]):
  def __init__(
    self,
    node_id: int,
    peers: list[int],
    transport = None,
    apply: Callable[[CommandT, int], None] | None = None,
    election_timeout_range_ms: tuple[int, int] = (150, 300),
    heartbeat_interval_ms: int = 50,
    rng: random.Random | None = None,
  ) -> None:
    self.__id: int = node_id
    self.__peers: list[int] = [peer for peer in peers if peer != node_id]
    self.__transport = transport
    self.__apply: Callable[[CommandT], None] | None = apply
    self.__rng: random.Random = rng or random.Random()

    self.__role: Role = Role.FOLLOWER
    self.__current_term: int = 0
    self.__voted_for: int | None = None

    self.__log: RaftLog[CommandT] = RaftLog()
    self.__commit_index: int = 0
    self.__last_applied: int = 0

    self.__next_index: dict[int, int] = {}
    self.__match_index: dict[int, int] = {}
    self.__votes_received: set[int] = set()

    self.__limit_index: int | None = None
    self.__election_timeout_range_ms: tuple[int, int] = election_timeout_range_ms
    self.__election_timer_ms: int = self.__new_election_timeout()
    self.__heartbeat_interval_ms: int = heartbeat_interval_ms
    self.__heartbeat_timer_ms: int = heartbeat_interval_ms
    self.__snapshot_provider: Callable[[int], bytes] | None = None
    self.__apply_snapshot_handler: Callable[[bytes, int], None] | None = None

  def __raft_log(self, message: str) -> None:
    print(
      f"[RAFT node={self.__id} term={self.__current_term} role={self.__role.value}] {message}",
      flush=True,
    )

  @property
  def id(self) -> int:
    return self.__id

  @property
  def role(self) -> Role:
    return self.__role

  @property
  def state(self) -> NodeState:
    return NodeState(
      term=self.__current_term,
      role=self.__role,
      commit_index=self.__commit_index,
      last_applied=self.__last_applied,
    )

  @property
  def log(self) -> RaftLog[CommandT]:
    return self.__log

  def set_limit_index(self, index: int | None) -> None:
    self.__limit_index = index

  def set_transport(self, transport) -> None:
    self.__transport = transport

  def tick(self, elapsed_ms: int) -> None:
    if self.__role == Role.LEADER:
      self.__heartbeat_timer_ms -= elapsed_ms
      if self.__heartbeat_timer_ms <= 0:
        self.__heartbeat_timer_ms = self.__heartbeat_interval_ms
        self.__send_heartbeats()
      return

    self.__election_timer_ms -= elapsed_ms
    if self.__election_timer_ms <= 0:
      self.start_election()

  def start_election(self) -> None:
    self.__raft_log("starting election")
    self.__become_candidate(self.__current_term + 1)
    self.__votes_received = {self.__id}
    self.__voted_for = self.__id
    self.__reset_election_timer()

    if len(self.__votes_received) >= self.__quorum_size():
      self.__become_leader()
      return

    request = RequestVote(
      term=self.__current_term,
      candidate_id=self.__id,
      last_log_index=self.__log.last_index(),
      last_log_term=self.__log.last_term(),
    )

    self.__send_request_votes(request)

  def handle_request_vote(self, message: RequestVote) -> RequestVoteResponse:
    if message.term < self.__current_term:
      self.__raft_log(f"rejecting vote for candidate={message.candidate_id}: stale term")
      return RequestVoteResponse(
        term=self.__current_term,
        voter_id=self.__id,
        vote_granted=False,
      )

    if message.term > self.__current_term:
      self.__become_follower(message.term)

    vote_granted = False
    if (self.__voted_for is None or self.__voted_for == message.candidate_id) and \
      self.__is_log_up_to_date(message.last_log_index, message.last_log_term):
      self.__voted_for = message.candidate_id
      self.__reset_election_timer()
      vote_granted = True

    return RequestVoteResponse(
      term=self.__current_term,
      voter_id=self.__id,
      vote_granted=vote_granted,
    )

  def handle_request_vote_response(self, message: RequestVoteResponse) -> None:
    if message.term > self.__current_term:
      self.__become_follower(message.term)
      return

    if self.__role != Role.CANDIDATE or message.term != self.__current_term:
      return

    if message.vote_granted:
      self.__votes_received.add(message.voter_id)
      if len(self.__votes_received) >= self.__quorum_size():
        self.__become_leader()

  def handle_append_entries(self, message: AppendEntries[CommandT]) -> AppendEntriesResponse:
    if message.term < self.__current_term:
      self.__raft_log(f"rejecting AppendEntries from leader={message.leader_id}: stale term")
      return AppendEntriesResponse(
        term=self.__current_term,
        follower_id=self.__id,
        success=False,
        match_index=self.__log.last_index(),
      )

    if message.term > self.__current_term or self.__role != Role.FOLLOWER:
      self.__become_follower(message.term)

    self.__reset_election_timer()

    if message.prev_log_index > 0:
      if not self.__log.match_term(message.prev_log_index, message.prev_log_term):
        self.__raft_log(
          f"rejecting AppendEntries from leader={message.leader_id}: log mismatch at index {message.prev_log_index}"
        )
        return AppendEntriesResponse(
          term=self.__current_term,
          follower_id=self.__id,
          success=False,
          match_index=self.__log.last_index(),
        )

    if message.entries:
      insert_index = message.prev_log_index + 1
      for offset, entry in enumerate(message.entries):
        log_index = insert_index + offset
        existing = self.__log.entry_at(log_index)
        if existing is None:
          self.__log.append_entries(log_index, message.entries[offset:])
          break
        if existing.term != entry.term:
          self.__log.delete_from(log_index)
          self.__log.append_entries(log_index, message.entries[offset:])
          break

    if message.leader_commit > self.__commit_index:
      self.__commit_index = min(message.leader_commit, self.__log.last_index())
      self.__raft_log(f"advancing commit_index to {self.__commit_index}")
      self.__apply_committed()

    match_index = message.prev_log_index + len(message.entries)
    return AppendEntriesResponse(
      term=self.__current_term,
      follower_id=self.__id,
      success=True,
      match_index=match_index,
    )

  def handle_append_entries_response(self, message: AppendEntriesResponse) -> None:
    if message.term > self.__current_term:
      self.__become_follower(message.term)
      return

    if self.__role != Role.LEADER or message.term != self.__current_term:
      return

    if message.success:
      self.__match_index[message.follower_id] = message.match_index
      self.__next_index[message.follower_id] = message.match_index + 1
      self.__advance_commit_index()
      return

    next_index = self.__next_index.get(message.follower_id, self.__log.last_index() + 1)
    self.__next_index[message.follower_id] = max(1, next_index - 1)
    self.__send_append_entries(message.follower_id)

  def client_append(self, command: CommandT) -> int | None:
    """Append a command to the log as leader.

    Returns the Raft log index at which the entry was appended, or None if
    this node is not the leader.  Callers that need to cap replication (e.g.
    via set_limit_index) should use the returned index.
    """
    if self.__role != Role.LEADER:
      self.__raft_log(f"rejecting client append because node is not leader: {command}")
      return None

    index = self.__log.append(LogEntry(term=self.__current_term, command=command))
    self.__advance_commit_index()
    self.__send_heartbeats()
    return index

  def __send_request_votes(self, message: RequestVote) -> None:
    if not self.__transport:
      return
    for peer_id in self.__peers:
      threading.Thread(
        target=self.__transport.send_request_vote,
        args=(peer_id, message),
        daemon=True,
      ).start()

  def __send_heartbeats(self) -> None:
    for peer_id in self.__peers:
      self.__send_append_entries(peer_id)

  def __send_append_entries(self, peer_id: int) -> None:
    if not self.__transport:
      return

    next_index = self.__next_index.get(peer_id, self.__log.last_index() + 1)
    prev_index = next_index - 1
    prev_term = self.__log.term_at(prev_index) or 0
    
    # Cap entries up to limit_index if set
    last_visible = self.__limit_index if self.__limit_index is not None else self.__log.last_index()
    
    # If follower is so far behind that leader has compacted its log, send InstallSnapshot
    try:
      base_index = self.__log.base_index()
    except Exception:
      base_index = 0
    if next_index <= base_index:
      snapshot_data = self.__snapshot_provider(base_index) if self.__snapshot_provider else b""
      snapshot = InstallSnapshot(
        term=self.__current_term,
        leader_id=self.__id,
        last_included_index=base_index,
        last_included_term=self.__log.term_at(base_index) or 0,
        data=snapshot_data,
      )
      # Move the follower's Raft frontier to the snapshot boundary immediately.
      self.__next_index[peer_id] = base_index + 1
      self.__match_index[peer_id] = base_index
      self.__raft_log(f"sending InstallSnapshot to peer={peer_id} base={base_index}")
      threading.Thread(
        target=self.__transport.send_install_snapshot,
        args=(peer_id, snapshot),
        daemon=True,
      ).start()
      return

    if next_index > last_visible:
      entries = []
    else:
      entries = self.__log.entries_from(next_index)
      # Suffix beyond last_visible is hidden
      if next_index + len(entries) - 1 > last_visible:
        entries = entries[:last_visible - next_index + 1]

    message = AppendEntries(
      term=self.__current_term,
      leader_id=self.__id,
      prev_log_index=prev_index,
      prev_log_term=prev_term,
      entries=entries,
      leader_commit=self.__commit_index,
    )
    if entries:
      self.__raft_log(
        f"sending AppendEntries to peer={peer_id} prev=({prev_index},{prev_term}) entries={len(entries)} commit={self.__commit_index}"
      )
    threading.Thread(
      target=self.__transport.send_append_entries,
      args=(peer_id, message),
      daemon=True,
    ).start()

  def handle_install_snapshot(self, message: InstallSnapshot) -> InstallSnapshotResponse:
    if message.term < self.__current_term:
      return InstallSnapshotResponse(term=self.__current_term, follower_id=self.__id)

    if message.term > self.__current_term or self.__role != Role.FOLLOWER:
      self.__become_follower(message.term)

    self.__reset_election_timer()

    # Apply snapshot to the local state machine if handler is registered
    if self.__apply_snapshot_handler:
      try:
        self.__apply_snapshot_handler(message.data, message.last_included_index)
      except Exception as e:
        self.__raft_log(f"error applying snapshot: {e}")

    # Compact Raft log up to last_included_index and advance commit/last_applied
    self.__log.compact(message.last_included_index)
    self.__commit_index = max(self.__commit_index, message.last_included_index)
    self.__last_applied = max(self.__last_applied, message.last_included_index)

    return InstallSnapshotResponse(term=self.__current_term, follower_id=self.__id)

  def handle_install_snapshot_response(self, message: InstallSnapshotResponse) -> None:
    if message.term > self.__current_term:
      self.__become_follower(message.term)
      return

    if self.__role != Role.LEADER or message.term != self.__current_term:
      return

    # Advance follower's indices to after the snapshot
    base = self.__log.base_index()
    self.__next_index[message.follower_id] = max(self.__next_index.get(message.follower_id, 0), base + 1)
    self.__match_index[message.follower_id] = max(self.__match_index.get(message.follower_id, 0), base)

  def set_snapshot_provider(self, provider: Callable[[int], bytes] | None) -> None:
    self.__snapshot_provider = provider

  def set_apply_snapshot_handler(self, handler: Callable[[bytes, int], None] | None) -> None:
    self.__apply_snapshot_handler = handler

  def __advance_commit_index(self) -> None:
    # Leader-side commit-index advancement. Implements the safety rule of
    # RAFT Section 5.3 + 5.4.2: an entry is committed once it is stored on
    # a majority of replicas AND it was created in the leader's CURRENT
    # term. The second clause is essential -- without it, a leader could
    # commit an entry from a prior term that a future leader might
    # overwrite (the "Figure 8" anomaly in the RAFT paper).
    #
    # Use `self.__log.last_index()` for the highest log index, and
    # `self.__log.term_at(index)` to fetch the term for a given index.
    # The leader's match_index for itself is implicitly `last_index`, so
    # remember to count yourself in the matched tally.
    previous_commit = self.__commit_index

    for index in range(self.__commit_index + 1, self.__log.last_index() + 1):
      if self.__log.term_at(index) != self.__current_term:
        continue

      replicated_count = 1
      for peer_id in self.__peers:
        if self.__match_index.get(peer_id, 0) >= index:
          replicated_count += 1

      if replicated_count >= self.__quorum_size():
        self.__commit_index = index

    if self.__commit_index != previous_commit:
      self.__raft_log(f"commit_index advanced from {previous_commit} to {self.__commit_index}")
    self.__apply_committed()

  def __apply_committed(self) -> None:
    # State-machine application loop. Walks `self.__last_applied` forward
    # toward `self.__commit_index`, firing the apply callback exactly once
    # per committed entry, in log order. This is the only place where
    # `self.__last_applied` is mutated.
    #
    # Use `self.__log.entry_at(index)` to fetch the entry; it may return
    # None (if the index is below the log's compacted base), in which
    # case skip the application but DO still advance `__last_applied`
    # (it was already done above; the skip is for the callback only).
    #
    # `self.__apply` is the callback registered by the embedding code
    # (the Replica's `__apply_raft_command`). It receives
    # `(command, raft_log_index)`. The callback may be None during
    # bootstrap; guard accordingly.
    #
    while self.__last_applied < self.__commit_index:
      self.__last_applied += 1
      entry = self.__log.entry_at(self.__last_applied)
      if entry is None or self.__apply is None:
        continue

      self.__raft_log(f"applying committed log entry at index={self.__last_applied}: {entry.command}")
      self.__apply(entry.command, self.__last_applied)

  def __is_log_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
    # The "log up-to-date" check from RAFT Section 5.4.1. Used by
    # handle_request_vote to decide whether to grant a vote: a candidate
    # is only granted a vote if its log is AT LEAST as up-to-date as the
    # voter's. The comparison is lexicographic on (last term, last index):
    #
    #   - The candidate is "more up-to-date" if its last log term is
    #     STRICTLY greater than ours.
    #   - If the last terms are equal, the candidate is "as up-to-date"
    #     if its last log index is GREATER THAN OR EQUAL to ours.
    #
    # Use `self.__log.last_index()` and `self.__log.last_term()` for the
    # local values. Returns True iff the candidate's log is at least as
    # up-to-date as ours.
    #
    local_last_term = self.__log.last_term()
    local_last_index = self.__log.last_index()

    if last_log_term != local_last_term:
      return last_log_term > local_last_term

    return last_log_index >= local_last_index

  def __become_candidate(self, term: int) -> None:
    self.__role = Role.CANDIDATE
    self.__current_term = term
    self.__raft_log(f"became candidate for term={term}")

  def __become_follower(self, term: int) -> None:
    self.__role = Role.FOLLOWER
    self.__current_term = term
    self.__voted_for = None
    self.__votes_received.clear()
    self.__reset_election_timer()
    self.__raft_log(f"became follower for term={term}")

  def __become_leader(self) -> None:
    self.__role = Role.LEADER
    self.__voted_for = None
    self.__votes_received.clear()

    last_index = self.__log.last_index()
    self.__next_index = {peer: last_index + 1 for peer in self.__peers}
    self.__match_index = {peer: 0 for peer in self.__peers}

    self.__heartbeat_timer_ms = self.__heartbeat_interval_ms
    self.__raft_log(f"became leader for term={self.__current_term}")
    self.__send_heartbeats()

  def __quorum_size(self) -> int:
    # The number of replicas (out of the full cluster of size
    # `len(self.__peers) + 1` -- the peers plus this node) that have to
    # acknowledge an entry before the leader can consider it committed.
    # Standard RAFT uses a strict MAJORITY: floor(N / 2) + 1 where
    # N = total cluster size. With 3 replicas the quorum is 2, with 5
    # it is 3, etc. `self.__peers` excludes this node, so the cluster
    # size is `len(self.__peers) + 1`.
    #
    cluster_size = len(self.__peers) + 1
    return (cluster_size // 2) + 1

  def __new_election_timeout(self) -> int:
    low, high = self.__election_timeout_range_ms
    return self.__rng.randint(low, high)

  def __reset_election_timer(self) -> None:
    self.__election_timer_ms = self.__new_election_timeout()
