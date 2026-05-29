from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Iterable, TypeVar, cast

CommandT = TypeVar("CommandT")


@dataclass(frozen=True)
class LogEntry(Generic[CommandT]):
  term: int
  command: CommandT

  @property
  def i(self) -> int | None:
    return self.command.get("i") if isinstance(self.command, dict) else None

  @property
  def op_t(self) -> dict[int, int] | None:
    return self.command.get("op_t") if isinstance(self.command, dict) else None

  @property
  def delivery_t(self) -> dict[int, int] | None:
    return self.command.get("delivery_t") if isinstance(self.command, dict) else None

  @property
  def old_p(self) -> int | None:
    return self.command.get("old_p") if isinstance(self.command, dict) else None

  @property
  def old_m(self) -> dict | None:
    return self.command.get("old_m") if isinstance(self.command, dict) else None

  @property
  def p(self) -> int | None:
    return self.command.get("p") if isinstance(self.command, dict) else None

  @property
  def m(self) -> dict | None:
    return self.command.get("m") if isinstance(self.command, dict) else {}

  @property
  def c(self) -> int | None:
    return self.command.get("c") if isinstance(self.command, dict) else None


class RaftLog(Generic[CommandT]):
  def __init__(self, entries: Iterable[LogEntry[CommandT]] | None = None) -> None:
    sentinel = cast(LogEntry[CommandT], LogEntry(term=0, command={}))
    self.__entries: list[LogEntry[CommandT]] = [sentinel]
    self.__base_index: int = 0
    if entries:
      self.__entries.extend(entries)

  def last_index(self) -> int:
    return self.__base_index + len(self.__entries) - 1

  def last_term(self) -> int:
    return self.__entries[-1].term

  @property
  def entries(self) -> list[LogEntry[CommandT]]:
    return list(self.__entries)

  def entry_at(self, index: int) -> LogEntry[CommandT] | None:
    relative_index = index - self.__base_index
    if relative_index < 0 or relative_index >= len(self.__entries):
      return None
    return self.__entries[relative_index]

  def term_at(self, index: int) -> int | None:
    entry = self.entry_at(index)
    if entry is None:
      return None
    return entry.term

  def match_term(self, index: int, term: int) -> bool:
    entry = self.entry_at(index)
    if entry is None:
      return False
    return entry.term == term

  def append(self, entry: LogEntry[CommandT]) -> int:
    self.__entries.append(entry)
    return self.last_index()

  def append_entries(self, start_index: int, entries: list[LogEntry[CommandT]]) -> None:
    if start_index <= self.__base_index:
      raise ValueError("start_index must be > base_index")
    if not entries:
      return
    if start_index <= self.last_index():
      self.delete_from(start_index)
    self.__entries.extend(entries)

  def delete_from(self, index: int) -> None:
    if index <= self.__base_index:
      raise ValueError("index must be > base_index")
    relative_index = index - self.__base_index
    if relative_index <= len(self.__entries):
      self.__entries = self.__entries[:relative_index]

  def entries_from(self, index: int) -> list[LogEntry[CommandT]]:
    if index <= self.__base_index:
      raise ValueError("index must be > base_index")
    relative_index = index - self.__base_index
    if relative_index >= len(self.__entries):
      return []
    return list(self.__entries[relative_index:])

  def insert(self, index: int, entry: LogEntry[CommandT]) -> None:
    if index <= self.__base_index:
      raise ValueError("index must be > base_index")
    relative_index = index - self.__base_index
    self.__entries.insert(relative_index, entry)

  def compact(self, index: int) -> None:
    if index <= self.__base_index or index > self.last_index():
      return
    relative_index = index - self.__base_index
    # The entry at 'index' becomes the new sentinel (index 0 relative)
    self.__entries = self.__entries[relative_index:]
    self.__base_index = index

  def base_index(self) -> int:
    return self.__base_index

  def __len__(self) -> int:
    # Number of live entries (excluding the sentinel at index 0)
    return len(self.__entries) - 1

  def __getitem__(self, idx: int) -> LogEntry[CommandT]:
    # Support indexing for Replica logic if needed, but entry_at is safer
    # Assuming the caller expects 0-based relative to live entries
    return self.__entries[idx + 1]
