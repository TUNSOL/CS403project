from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from .log import LogEntry

CommandT = TypeVar("CommandT")


@dataclass(frozen=True)
class RequestVote:
  term: int
  candidate_id: int
  last_log_index: int
  last_log_term: int


@dataclass(frozen=True)
class RequestVoteResponse:
  term: int
  voter_id: int
  vote_granted: bool


@dataclass(frozen=True)
class AppendEntries(Generic[CommandT]):
  term: int
  leader_id: int
  prev_log_index: int
  prev_log_term: int
  entries: list[LogEntry[CommandT]]
  leader_commit: int


@dataclass(frozen=True)
class AppendEntriesResponse:
  term: int
  follower_id: int
  success: bool
  match_index: int


@dataclass(frozen=True)
class InstallSnapshot:
  term: int
  leader_id: int
  last_included_index: int
  last_included_term: int
  data: bytes


@dataclass(frozen=True)
class InstallSnapshotResponse:
  term: int
  follower_id: int
