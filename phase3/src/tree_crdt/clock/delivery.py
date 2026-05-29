from . import Clock


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
    # TODO: store the replica id and initialise the dict {0: 0, 1: 0, ..., max_id-1: 0}.
    raise NotImplementedError("TODO: implement DeliveryClock.__init__")

  @property
  def id(self):
    """Return the ID of the replica that owns this checkpoint vector."""
    # TODO
    raise NotImplementedError("TODO: implement DeliveryClock.id")

  @property
  def timestamp(self):
    """Return a DEEP COPY of the current checkpoint vector."""
    # TODO: deepcopy is required; do not return the internal dict directly.
    raise NotImplementedError("TODO: implement DeliveryClock.timestamp")

  def update(self, received):
    """Advance the checkpoint vector by one operation from the given source.

    If `received` is None, increment `C[self.id]` by 1.
    If `received` is an int `k`, increment `C[k]` by 1.

    Each call corresponds to exactly one operation being folded into
    the local stable tree.
    """
    # TODO
    raise NotImplementedError("TODO: implement DeliveryClock.update")

  def set_timestamp(self, timestamp):
    """Overwrite the internal checkpoint vector with the given dict.

    Used in tests and for rewinding the checkpoint vector; the
    argument is copied component-by-component, aliasing is not
    permitted.
    """
    # TODO
    raise NotImplementedError("TODO: implement DeliveryClock.set_timestamp")

  def __str__(self):
    # TODO
    raise NotImplementedError("TODO: implement DeliveryClock.__str__")

  # ---------------------------------------------------------------------
  # Class methods for comparison helpers (Phase 3 total order)
  # ---------------------------------------------------------------------
  # Same contracts as VectorClock's helpers (sum-then-lexicographic
  # total order); see the docstring of the VectorClock class for the
  # full specification.

  @classmethod
  def timestamp_le(cls, lhs, rhs):
    """Return True iff lhs <= rhs under the total order of Phase 3."""
    # TODO
    raise NotImplementedError("TODO: implement DeliveryClock.timestamp_le")

  @classmethod
  def timestamp_lt(cls, lhs, rhs):
    """Return True iff lhs < rhs under the total order of Phase 3."""
    # TODO
    raise NotImplementedError("TODO: implement DeliveryClock.timestamp_lt")

  @classmethod
  def timestamp_eq(cls, lhs, rhs):
    """Return True iff lhs and rhs are componentwise equal."""
    # TODO
    raise NotImplementedError("TODO: implement DeliveryClock.timestamp_eq")

  @classmethod
  def timestamp_concurrent(cls, lhs, rhs):
    """Return True iff lhs and rhs are concurrent.

    Under the Phase 3 total order this is identically False.
    """
    # TODO
    raise NotImplementedError("TODO: implement DeliveryClock.timestamp_concurrent")
