# NOTE: You can add additional imports as you need
import threading

from .clock import DeliveryClock, VectorClock
from .payload import MovePayload
from .raft import GrpcRaftServer, GrpcTransport, RaftNode
from .tree import Tree, Node


class Replica:
  """A single replica of the Tree CRDT, Phase 3.

  See the Phase 3 PDF, section "The Replica Class", for the full
  contracts. Briefly, the Replica owns:

    - A `VectorClock` (`__op_clock`) tracking the causality of
      operations issued and observed.
    - A `DeliveryClock` (`__checkpoint_clock`) holding the
      checkpoint vector C of Section "The Checkpoint Vector and the
      Safe Index". `C[k]` is the number of operations originating
      from replica `k` that have been folded into the local stable
      tree.
    - Two `Tree` instances: the *current tree* (`__tree`, all visible
      operations applied) and the *stable tree* (`__tree_snapshot`,
      only checkpointed operations).
    - An operation log `__op_log` whose entries record both the
      forward and the old-state fields needed for exact undo:
      `(replica_id, timestamp, old_parent, old_metadata, parent,
      metadata, child)`. Two absolute-index pointers
      (`__log_offset`, `__safe_index_abs`) track the boundary
      between physically erased, checkpointed, and uncheckpointed
      entries.
    - An embedded `RaftNode` and its gRPC server/transport sides.
      The Replica drives a daemon thread that calls
      `RaftNode.tick(...)`, attempts to advance the safe index, and
      proposes `{"op": "safe_index", "value": k}` commands to RAFT
      when this replica is the leader.

  Public API:
    Properties (read-only deep copies, except the addresses):
      id, op_clock, checkpoint_clock, tree, tree_snapshot, log,
      raft_log, log_offset, safe_index, main_addr, listener_addr
    Methods:
      current_timestamp(), tick_op_clock(received),
      tick_checkpoint_clock(source),
      apply_local_move(parent, metadata, child),
      apply_remote_move(op),
      flush_snapshot(timeout_seconds),
      close_raft_channels(), close_raft_server()

  Thread-safety:
    The CRDT state is protected by a single `RLock` (`__lock`).
    The pending-commits queue between the gRPC callback and the
    RAFT daemon thread uses a separate lightweight `Lock`
    (`__commit_lock`) so the gRPC thread NEVER blocks on
    user-level CRDT work.
  """

  def __init__(
    self,
    id,
    host,
    main_base,
    listener_base,
    num_replicas,
    raft_base,
    peer_hosts,
    op_limits,
  ):
    """Initialise the Replica.

    The three Phase 3 additions to the Phase 2 signature are
    `raft_base`, `peer_hosts`, and `op_limits`:

      - `raft_base` (int): base TCP port for the RAFT gRPC server.
        Replica `i` binds on `host:raft_base + i`.
      - `peer_hosts` (list[str]): host of every replica indexed by
        replica ID. Used to build the
        `{peer_id -> host[peer_id]:raft_base + peer_id}` table for
        the `GrpcTransport`.
      - `op_limits` (dict[int, int]): the operation-limit vector L
        of Section "Termination and Saturated Fields". L[k] is the
        total number of operations replica k will ever issue.

    The constructor must:

      1. Build the `VectorClock` and `DeliveryClock`.
      2. Build the current and stable `Tree` instances.
      3. Build the operation log and the absolute-index bookkeeping
         (`__log_offset`, `__safe_index_abs`,
         `__last_sent_safe_index`, `__raft_committed_safe_index`).
      4. Build the pub/sub address strings.
      5. Build the `RaftNode`, then the `GrpcRaftServer`, then the
         `GrpcTransport`, then attach the transport to the node.
         The order matters: the gRPC server must be up before the
         transport begins delivering responses to the node.
      6. Start the RAFT daemon thread (`__run_raft`).
    """
    super().__init__()
    # TODO: initialise all of the fields listed in the class docstring.
    raise NotImplementedError("TODO: implement Replica.__init__")

  # -------------------------------------------------------------------------
  # Properties (read-only views; mutable values must be deep-copied)
  # -------------------------------------------------------------------------

  @property
  def id(self):
    """Return this replica's ID."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.id")

  @property
  def op_clock(self):
    """Return a DEEP COPY of the operation clock."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.op_clock")

  @property
  def checkpoint_clock(self):
    """Return a DEEP COPY of the checkpoint clock (the C vector)."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.checkpoint_clock")

  @property
  def tree(self):
    """Return a DEEP COPY of the current tree."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.tree")

  @property
  def tree_snapshot(self):
    """Return a DEEP COPY of the stable tree."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.tree_snapshot")

  @property
  def log(self):
    """Return a DEEP COPY of the operation log."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.log")

  @property
  def raft_log(self):
    """Return a DEEP COPY of the RAFT log entries."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.raft_log")

  @property
  def log_offset(self):
    """Return the number of operation-log entries already physically erased."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.log_offset")

  @property
  def safe_index(self):
    """Return the absolute safe index (boundary up to which entries are checkpointed)."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.safe_index")

  @property
  def main_addr(self):
    """Return the ZeroMQ main (publisher) address of this replica."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.main_addr")

  @property
  def listener_addr(self):
    """Return the ZeroMQ listener (subscriber/reply) address of this replica."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.listener_addr")

  def current_timestamp(self):
    """Return a deep copy of the current operation-clock timestamp."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.current_timestamp")

  # -------------------------------------------------------------------------
  # Public CRDT operations
  # -------------------------------------------------------------------------

  def tick_op_clock(self, received):
    """Advance the operation clock.

    If `received` is None, this is a local event: increment
    `__op_clock[self.id]` by 1. Otherwise, take the componentwise
    max with `received` (no self-tick on remote events; see the
    Phase 3 VectorClock contract).
    Returns a deep copy of the resulting timestamp.
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.tick_op_clock")

  def tick_checkpoint_clock(self, source):
    """Advance the checkpoint clock by one operation from the given source.

    Wraps `DeliveryClock.update(source)` under `__lock`.
    Returns a deep copy of the resulting checkpoint vector.
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.tick_checkpoint_clock")

  def apply_local_move(self, parent, metadata, child):
    """Apply a locally-generated Move/Delete.

    1. Tick the operation clock locally (`update(None)`).
    2. Build a `MovePayload(id, timestamp, parent, metadata, child)`.
    3. Apply it via `__apply_move(...)`.
    4. Return the constructed payload (the main thread broadcasts it).
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.apply_local_move")

  def apply_remote_move(self, op):
    """Apply a Move/Delete received from a peer.

    1. Tick the operation clock with `update(op.timestamp)` (no
       self-tick).
    2. Apply via `__apply_move(op)`.
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.apply_remote_move")

  # -------------------------------------------------------------------------
  # Lifecycle / Teardown
  # -------------------------------------------------------------------------

  def flush_snapshot(self, timeout_seconds=30.0):
    """Block until the RAFT-committed compaction frontier reaches the expected total.

    The expected total is the sum of the entries in `op_limits`.
    Polls `__log_offset` on a 50 ms loop and gives up after
    `timeout_seconds`. Returns True on success, False on timeout.
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.flush_snapshot")

  def close_raft_channels(self):
    """Stage 1 of the RAFT teardown: stop the tick loop and drop outbound channels.

    Sets `__raft_shutdown`, joins the `__run_raft` daemon thread
    (with a 2 s deadline), and calls `__raft_transport.close()`.
    After this call the replica issues no further RAFT RPCs.
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.close_raft_channels")

  def close_raft_server(self):
    """Stage 2 of the RAFT teardown: stop the inbound gRPC server (1 s grace)."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.close_raft_server")

  # -------------------------------------------------------------------------
  # RAFT tick loop and command handling (private)
  # -------------------------------------------------------------------------

  def __run_raft(self):
    """RAFT daemon loop.

    Once per iteration (with a 10 ms sleep):
      1. Call `__raft_node.tick(elapsed_ms)`.
      2. Drain any RAFT commits that the gRPC callback enqueued.
      3. Attempt to advance the local safe index, and, if this
         replica is currently the leader and the safe index has
         advanced past `__last_sent_safe_index`, propose
         `{"op": "safe_index", "value": k}` via
         `__raft_node.client_append(...)`.
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.__run_raft")

  def __apply_raft_command(self, command, raft_log_index):
    """Callback fired by the RAFT node when a new command is applied.

    Runs on the gRPC thread; must NOT acquire `__lock`. Just
    enqueues the (value, raft_log_index) pair for the RAFT daemon
    thread to drain.
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.__apply_raft_command")

  def __drain_pending_commits(self):
    """Drain `__pending_commits` and dispatch each pair to `__apply_safe_index_commit`."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.__drain_pending_commits")

  def __apply_safe_index_commit(self, safe_index_abs, raft_log_index):
    """Record the RAFT-committed compaction frontier and attempt to compact."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.__apply_safe_index_commit")

  def __compact_log(self, raft_log_index):
    """Drop operation-log entries that are both RAFT-committed and locally safe.

    The drop target is
    `min(__raft_committed_safe_index, __safe_index_abs)`; the cap
    is what makes the routine safe in both directions (see Section
    "Why safe_index Is the Only Command").
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.__compact_log")

  # -------------------------------------------------------------------------
  # Total order, insertion, undo-do-redo (private)
  # -------------------------------------------------------------------------

  def __apply_move(self, op):
    """Apply `op` to the current tree and to the operation log.

    Must be called under `__lock`. The shape:

      1. Look up the existing node at `op.child` (if any) to capture
         `old_p`/`old_m` for exact undo.
      2. If `op` belongs at the end of the log under the total
         order, append it and `Tree.move(...)` it directly.
      3. Otherwise, find the insertion point under the total order,
         undo the entries after that point in reverse, apply `op`,
         then redo the previously undone entries in original order.
      4. Call `__try_advance_safe_index()` to fold as many entries
         as possible into the stable tree.
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.__apply_move")

  def __is_in_order(self, op):
    """Return True iff `op` belongs at the end of the operation log."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.__is_in_order")

  def __find_insertion_point(self, op):
    """Binary-search the insertion position of `op` in the operation log under the total order."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.__find_insertion_point")

  def __undo_operations(self, ops):
    """Undo `ops` (in reverse order) on the current tree using their old_p/old_m fields."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.__undo_operations")

  def __do_operation(self, op, old_p, old_m, p, m, c, insert_pos=None):
    """Append (or insert) the operation into the log and apply it to the current tree."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.__do_operation")

  def __redo_operations(self, ops):
    """Redo `ops` on the current tree in original order."""
    # TODO
    raise NotImplementedError("TODO: implement Replica.__redo_operations")

  # -------------------------------------------------------------------------
  # Safe-index advancement (private)
  # -------------------------------------------------------------------------

  def __try_advance_safe_index(self):
    """Implement the admissibility checks of Section "Advancing the Safe Index".

    Loop, attempting to fold the front-of-suffix entry into the
    stable tree:

      1. `start_pos = __safe_index_abs - __log_offset`; return if the
         suffix is empty.
      2. Read C from `__checkpoint_clock.timestamp`.
      3. For every replica `k` that has not yet hit `op_limits[k]`,
         scan the suffix for an entry whose source is `k` and whose
         source-field timestamp is `C[k] + 1`. If none exists,
         return (this is the tiebreak-coverage check; saturated
         replicas are skipped per Section "Termination and
         Saturated Fields").
      4. Read the candidate at `start_pos`. The first three
         admissibility checks reduce to `candidate.t[k] = C[k] + 1`
         where `k` is the candidate's source.
      5. Fold the candidate into the stable tree, increment C[k]
         via `__checkpoint_clock.update(k)`, and advance
         `__safe_index_abs` by one. Iterate.
    """
    # TODO
    raise NotImplementedError("TODO: implement Replica.__try_advance_safe_index")

  def __str__(self):
    # TODO
    raise NotImplementedError("TODO: implement Replica.__str__")
