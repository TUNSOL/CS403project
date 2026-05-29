from copy import deepcopy

from . import Clock
from .vector import VectorClock


class DeliveryClock(Clock):
  """The checkpoint vector C of Phase 3.

  See the Phase 3 PDF, sections "The Checkpoint Vector and the Safe
  Index" and "The DeliveryClock Class", for the required contracts.
  Briefly:

    - The internal timestamp is a dict[int, int] of size `max_id`,
      one entry per replica, initialised to 0. Entry `C[k]` records
      the number of operations originating from replica `k` that the
      local stable tree has absorbed.
    - The fields and helpers mirror `VectorClock`; the substantive
      difference is that `update(received)` takes an integer source
      identifier rather than a vector, and increments `C[received]`
      by one. When `received` is None, increment the owner's own
      field. Each call therefore checkpoints exactly one more
      operation from the named source.
    - The `timestamp` property MUST return a deep copy of the
      internal dict.

  The four static helpers carry the same total-order semantics as
  `VectorClock`'s. They are provided as classmethods so that the
  Replica can compare `DeliveryClock` timestamps against
  `VectorClock` timestamps without going through an explicit cast.
  """

  def __init__(self, id, max_id):
    """Initialise the checkpoint vector for replica `id` in a system of `max_id` replicas."""
    super().__init__()
    self.__id = int(id)
    clock_size = max(int(max_id), self.__id + 1)
    self.__timestamp = {replica_id: 0 for replica_id in range(clock_size)}

  @property
  def id(self):
    """Return the ID of the replica that owns this checkpoint vector."""
    return self.__id

  @property
  def timestamp(self):
    """Return a DEEP COPY of the current checkpoint vector."""
    return deepcopy(self.__timestamp)

  def update(self, received):
    """Advance the checkpoint vector by one operation from the given source.

    If `received` is None, increment `C[self.id]` by 1.
    If `received` is an int `k`, increment `C[k]` by 1.

    Each call corresponds to exactly one operation being folded into
    the local stable tree.
    """
    source = self.__id if received is None else int(received)
    if source not in self.__timestamp:
      self.__timestamp[source] = 0
    self.__timestamp[source] += 1

  def set_timestamp(self, timestamp):
    """Overwrite the internal checkpoint vector with the given dict.

    Used in tests and for rewinding the checkpoint vector; the
    argument is copied component-by-component, aliasing is not
    permitted.
    """
    normalized = VectorClock._normalize_timestamp(timestamp)
    if not isinstance(normalized, dict):
      return
    self.__timestamp = {int(key): int(value) for key, value in normalized.items()}

  def __str__(self):
    return str(self.__timestamp)

  # ---------------------------------------------------------------------
  # Class methods for comparison helpers (Phase 3 total order)
  # ---------------------------------------------------------------------
  # Same contracts as VectorClock's helpers (sum-then-lexicographic
  # total order); see the docstring of the VectorClock class for the
  # full specification.

  @classmethod
  def timestamp_le(cls, lhs, rhs):
    """Return True iff lhs <= rhs under the total order of Phase 3."""
    return VectorClock.timestamp_le(lhs, rhs)

  @classmethod
  def timestamp_lt(cls, lhs, rhs):
    """Return True iff lhs < rhs under the total order of Phase 3."""
    return VectorClock.timestamp_lt(lhs, rhs)

  @classmethod
  def timestamp_eq(cls, lhs, rhs):
    """Return True iff lhs and rhs are componentwise equal."""
    return VectorClock.timestamp_eq(lhs, rhs)

  @classmethod
  def timestamp_concurrent(cls, lhs, rhs):
    """Return True iff lhs and rhs are concurrent.

    Under the Phase 3 total order this is identically False.
    """
    return False
