from .clock import Clock


class LamportClock(Clock):
  """Lamport clock kept for Phase 1 compatibility paths."""

  def __init__(self, id: int) -> None:
    self.__id = id
    self.__timestamp = 0

  @property
  def id(self) -> int:
    return self.__id

  @property
  def timestamp(self) -> int:
    return self.__timestamp

  def update(self, received: int | None) -> None:
    if received is None:
      self.__timestamp += 1
      return

    self.__timestamp = max(self.__timestamp, int(received)) + 1

  def __str__(self) -> str:
    return str(self.__timestamp)
