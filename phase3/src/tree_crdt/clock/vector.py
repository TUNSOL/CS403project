from copy import deepcopy

from . import Clock


class VectorClock(Clock):
  """Vector logical clock for a replica, with the Phase 3 total order on top.

  See the Phase 3 PDF, sections "Total Ordering of Vector Timestamps"
  and "The VectorClock Class", for the required contracts. Briefly:

    - The internal timestamp is a dict[int, int] of size `max_id`, with
      every component initialised to 0.
    - On a local event (`update(None)`), increment the component for
      this replica's id by 1.
    - On a remote event (`update(received)`), take the component-wise
      maximum of self and `received`. *Do not* self-tick on remote
      events in Phase 3 (this is a change from Phase 2 and is what
      makes the safe-index admissibility checks fireable).
    - The `timestamp` property MUST return a deep copy of the internal
      dict; vector timestamps are mutable and aliasing is a frequent
      source of bugs.

  The four static helpers (`timestamp_le`, `timestamp_lt`,
  `timestamp_eq`, `timestamp_concurrent`) implement the *total* order
  of Phase 3, not the partial order of Phase 2. Given two vectors
  `t1` and `t2`, define `sum(t) = sum(t[k] for k in t)`. Then:

    - `timestamp_lt(t1, t2)` is True iff `sum(t1) < sum(t2)`, or the
      sums are equal and at the first left-to-right index where they
      differ, `t1[k] < t2[k]`.
    - `timestamp_le(t1, t2)` is `timestamp_lt(t1, t2) or
      timestamp_eq(t1, t2)`.
    - `timestamp_eq(t1, t2)` is fieldwise equality.
    - `timestamp_concurrent(t1, t2)` is always False under the total
      order -- the tiebreaker forces every pair of distinct vectors
      into one direction or the other.
  """

  def __init__(self, id, max_id):
    """Initialise the clock for replica `id` in a system of `max_id` replicas."""
    super().__init__()
    self.__id = int(id)
    clock_size = max(int(max_id), self.__id + 1)
    self.__timestamp = {replica_id: 0 for replica_id in range(clock_size)}

  @property
  def id(self):
    """Return the ID of the replica that owns this clock."""
    return self.__id

  @property
  def timestamp(self):
    """Return a DEEP COPY of the current vector timestamp."""
    return deepcopy(self.__timestamp)

  def update(self, received):
    """Update the clock.

    If `received` is None, this is a local event: increment
    `self[self.id]` by 1.
    If `received` is a dict, this is a remote event: take the
    component-wise maximum with `received` and DO NOT self-tick
    (Phase 3 change from Phase 2).
    """
    if received is None:
      self.__timestamp[self.__id] += 1
      return

    normalized = self._normalize_timestamp(received)
    if not isinstance(normalized, dict):
      return

    for replica_id in self.__timestamp:
      self.__timestamp[replica_id] = max(
        self.__timestamp[replica_id],
        int(normalized.get(replica_id, 0)),
      )

  def __str__(self):
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
  # Class methods for comparison helpers (Phase 3 total order)
  # ---------------------------------------------------------------------
  # Contracts:
  #   - timestamp_lt        : sum-then-lexicographic strict order
  #   - timestamp_le        : timestamp_lt or timestamp_eq
  #   - timestamp_eq        : fieldwise equality (key sets must match)
  #   - timestamp_concurrent: identically False under the total order
  # Vectors with mismatched key sets are incomparable and the helpers
  # should return False for them.

  @classmethod
  def timestamp_le(cls, lhs, rhs):
    """Return True iff lhs <= rhs under the total order of Phase 3."""
    return cls.timestamp_lt(lhs, rhs) or cls.timestamp_eq(lhs, rhs)

  @classmethod
  def timestamp_lt(cls, lhs, rhs):
    """Return True iff lhs < rhs under the total order of Phase 3.

    Use the sum-then-left-to-right-lexicographic rule described in
    Section "Total Ordering of Vector Timestamps" of the Phase 3 PDF.
    """
    lhs = cls._normalize_timestamp(lhs)
    rhs = cls._normalize_timestamp(rhs)

    if isinstance(lhs, int) and isinstance(rhs, int):
      return lhs < rhs

    if not isinstance(lhs, dict) or not isinstance(rhs, dict):
      return False

    if set(lhs.keys()) != set(rhs.keys()):
      return False

    lhs_sum = sum(lhs.values())
    rhs_sum = sum(rhs.values())
    if lhs_sum != rhs_sum:
      return lhs_sum < rhs_sum

    for key in sorted(lhs):
      if lhs[key] != rhs[key]:
        return lhs[key] < rhs[key]

    return False

  @classmethod
  def timestamp_eq(cls, lhs, rhs):
    """Return True iff lhs and rhs are componentwise equal."""
    lhs = cls._normalize_timestamp(lhs)
    rhs = cls._normalize_timestamp(rhs)

    if isinstance(lhs, int) and isinstance(rhs, int):
      return lhs == rhs

    if not isinstance(lhs, dict) or not isinstance(rhs, dict):
      return False

    return set(lhs.keys()) == set(rhs.keys()) and all(lhs[key] == rhs[key] for key in lhs)

  @classmethod
  def timestamp_concurrent(cls, lhs, rhs):
    """Return True iff lhs and rhs are concurrent.

    Under the Phase 3 total order this is identically False: the
    tiebreaker forces every pair of distinct vectors into one order
    or the other.
    """
    return False
