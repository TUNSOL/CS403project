from abc import ABC, abstractmethod

class Clock(ABC):
  @property
  @abstractmethod
  def id(self) -> int: ...

  @property
  @abstractmethod
  def timestamp(self) -> int | dict[int, int] : ...

  @abstractmethod
  def update(self, received) -> None: ...

  @abstractmethod
  def __str__(self) -> str : ...

  @classmethod
  @abstractmethod
  def timestamp_le(cls, lhs, rhs) -> bool: ...

  @classmethod
  @abstractmethod
  def timestamp_lt(cls, lhs, rhs) -> bool: ...

  @classmethod
  @abstractmethod
  def timestamp_eq(cls, lhs, rhs) -> bool: ...

  @classmethod
  @abstractmethod
  def timestamp_concurrent(cls, lhs, rhs) -> bool: ...