from copy import deepcopy

from .clock import Clock


class VectorClock(Clock):
  """Vector logical clock for a replica.

  See the Phase 2 PDF, section "The VectorClock Class", for the
  required contracts. Briefly:

    - The internal timestamp is a dict[int, int] of size `max_id`, with
      every component initialised to 0.
    - On a local event (`update(None)`), increment the component for
      this replica's id by 1.
    - On a remote event (`update(received)`), take the component-wise
      maximum of self and `received`, then perform the same self-tick.
    - The `timestamp` property MUST return a deep copy of the internal
      dict; vector timestamps are mutable and aliasing is a frequent
      source of bugs.
  """

  def __init__(self, id: int, max_id: int) -> None:
    """Initialise the clock for replica `id` in a system of `max_id` replicas."""
    super().__init__()
    self.__id = id
    clock_size = max(max_id, id + 1)
    self.__timestamp = {replica_id: 0 for replica_id in range(clock_size)}

  @property
  def id(self) -> int:
    """Return the ID of the replica that owns this clock."""
    return self.__id

  @property
  def timestamp(self) -> dict[int, int]:
    """Return a DEEP COPY of the current vector timestamp."""
    return deepcopy(self.__timestamp)

  def update(self, received: dict[int, int] | None) -> None:
    """Update the clock.

    If `received` is None, this is a local event: increment self[self.id] by 1.
    If `received` is a dict, this is a remote event: take component-wise max
    with `received`, then increment self[self.id] by 1.
    """
    if received is not None:
      normalized = self._normalize_timestamp(received)
      for replica_id in self.__timestamp:
        self.__timestamp[replica_id] = max(
          self.__timestamp[replica_id],
          normalized.get(replica_id, 0),
        )

    self.__timestamp[self.__id] += 1

  def __str__(self) -> str:
    return str(self.__timestamp)

  @staticmethod
  def _normalize_timestamp(timestamp):
    if not isinstance(timestamp, dict):
      return timestamp

    normalized = {}
    for key, value in timestamp.items():
      try:
        normalized[int(key)] = int(value)
      except (TypeError, ValueError):
        normalized[key] = value
    return normalized

  # ---------------------------------------------------------------------
  # Static comparison helpers (Phase 2)
  # ---------------------------------------------------------------------
  # The four helpers below provide a single comparison API that works for
  # both Lamport timestamps (int) and vector timestamps (dict[int, int]).
  # They are required by Replica and Tree in Phase 2; see the Phase 2 PDF,
  # section "Static Comparison Helpers".
  #
  # Contracts:
  #   - timestamp_le        : returns True iff lhs <=    rhs
  #   - timestamp_lt        : returns True iff lhs <     rhs
  #   - timestamp_eq        : returns True iff lhs ==    rhs
  #   - timestamp_concurrent: returns True iff lhs || rhs (vector-only;
  #                            for Lamport, this is identically False)
  # Mismatched operand types should be treated as incomparable.

  @staticmethod
  def timestamp_le(lhs, rhs):
    """Return True iff lhs <= rhs under the appropriate clock order."""
    lhs = VectorClock._normalize_timestamp(lhs)
    rhs = VectorClock._normalize_timestamp(rhs)

    if isinstance(lhs, int) and isinstance(rhs, int):
      return lhs <= rhs

    if not isinstance(lhs, dict) or not isinstance(rhs, dict):
      return False

    if set(lhs.keys()) != set(rhs.keys()):
      return False

    return all(lhs[key] <= rhs[key] for key in lhs)

  @staticmethod
  def timestamp_lt(lhs, rhs):
    """Return True iff lhs < rhs under the appropriate clock order."""
    return VectorClock.timestamp_le(lhs, rhs) and not VectorClock.timestamp_eq(lhs, rhs)

  @staticmethod
  def timestamp_eq(lhs, rhs):
    """Return True iff lhs == rhs."""
    lhs = VectorClock._normalize_timestamp(lhs)
    rhs = VectorClock._normalize_timestamp(rhs)

    if isinstance(lhs, int) and isinstance(rhs, int):
      return lhs == rhs

    if isinstance(lhs, dict) and isinstance(rhs, dict):
      return set(lhs.keys()) == set(rhs.keys()) and all(lhs[key] == rhs[key] for key in lhs)

    return False

  @staticmethod
  def timestamp_concurrent(lhs, rhs):
    """Return True iff lhs and rhs are concurrent (||) under the partial order."""
    lhs = VectorClock._normalize_timestamp(lhs)
    rhs = VectorClock._normalize_timestamp(rhs)

    if not isinstance(lhs, dict) or not isinstance(rhs, dict):
      return False

    if set(lhs.keys()) != set(rhs.keys()):
      return False

    return not VectorClock.timestamp_le(lhs, rhs) and not VectorClock.timestamp_le(rhs, lhs)
