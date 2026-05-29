# CS 403/534 Project — Phase 3

The repository for the project of CS 403/534 based on the paper by [Kleppmann et al.](https://doi.org/10.1109/TPDS.2021.3118603) on a Tree CRDT with a Move operation, implemented as a library named ```tree_crdt``` in the directory [src/tree_crdt](src/tree_crdt/), with a starter test suite in the [tests](tests/) directory and a main program in [```main.py```](main.py).

This is the **Phase 3** starter package. It assumes you have a working Phase 2 implementation as a starting point: many classes/files (e.g. ```VectorClock```, ```Tree```, ```Replica```, ```main.py```) were complete in Phase 2 and have now been gutted again so that you can extend them. A new sub-package, [```src/tree_crdt/raft```](src/tree_crdt/raft/), is provided as a **reference implementation** that you should not modify once you implement the parts that you have been asked for.

## What changes in Phase 3?

A short summary; please read the Phase 3 project PDF for the full specification.

- **Total order on vector timestamps.** ```VectorClock``` keeps its componentwise structure, but the four static comparison helpers (```timestamp_le```, ```timestamp_lt```, ```timestamp_eq```, ```timestamp_concurrent```) now implement a deterministic total order $\prec$: first by the sum of vector components, then by left-to-right lexicographic tiebreaker. Under this order ```timestamp_concurrent``` is identically False. The clock update rule also changes: ```update(received)``` no longer performs a self-tick on remote events.

- **Single-valued tree.** The multi-value semantics of Phase 2 (and the associated Move-Wins resolution) are no longer needed. Each child ID maps to at most one ```Node```. ```Tree.__call__``` now returns a ```set[Node]```, ```Tree.__getitem__``` returns ```Node | None```, and ```Tree.get_active``` returns ```Node | None```. ```Node``` reverts to the Phase 1 triple ```(p, m, c)``` and the ```metadata["applied"]``` key is no longer used.

- **A new clock type, ```DeliveryClock```.** ```DeliveryClock``` holds the **checkpoint vector** $C$: ```C[k]``` is the number of operations from replica $k$ that the local replica has folded into its stable tree. ```update(received)``` takes an integer source ID (or ```None``` for self) and increments that field by one.

- **Two trees per replica.** Each replica now maintains both a *current tree* (all visible operations applied) and a *stable tree* (only checkpointed operations applied, exposed via the new ```tree_snapshot``` property).

- **Safe-index advancement.** The Replica owns an absolute ```safe_index``` pointer into the operation log. The local advancement routine implements the four admissibility checks of the spec (single-field exceedance, source-field match, unit increment, tiebreak coverage) and folds eligible entries into the stable tree.

- **Termination via operation limits.** The configuration declares, for each replica, the total number of operations it will ever issue (```NUM_OP_MESSAGES``` in the ```.env``` file; ```op_limits``` argument to the ```Replica``` constructor). The safe-index routine treats a saturated field as terminally saturated, so the protocol does not deadlock when a replica stops generating operations.

- **RAFT-driven log compaction.** A RAFT instance is layered over the operation log to coordinate physical erasure of entries across replicas. The Replica embeds a ```RaftNode``` driven by a daemon thread, proposes ```{"op": "safe_index", "value": k}``` commands when it is the leader, and physically erases the log prefix up to ```min(raft_committed_safe_index, safe_index)``` when its commit index advances. **The RAFT sub-package [```src/tree_crdt/raft```](src/tree_crdt/raft/) is a reference implementation; do not modify its files. Your work is in wiring the Replica around it.**

- **Two-stage shutdown.** A new ```SHUTDOWN```/```ACK``` barrier is layered on top of the Phase 2 ```DONE```/```ACK``` exchange. Between the two barriers, ```Replica.flush_snapshot(...)``` blocks until the RAFT-committed compaction frontier catches up with the total number of operations expected from all replicas. RAFT teardown is split into ```close_raft_channels()``` (drop outbound gRPC) and ```close_raft_server()``` (stop the inbound gRPC server), called in that order.

## Package Contents

- **The directory [```src/tree_crdt```](src/tree_crdt/):** The base directory for the library, ```tree-crdt```, which you are going to extend as part of Phase 3.

  - **The directory [```clock```](src/tree_crdt/clock/):** The ```clock``` subpackage.

    - **The file [```clock.py```](src/tree_crdt/clock/clock.py):** The ```Clock``` abstract class.

    - **The file [```vector.py```](src/tree_crdt/clock/vector.py):** The file where you implement the Phase 3 ```VectorClock```. The internal representation and the constructor signature are unchanged from Phase 2; the four static helpers now implement the Phase 3 total order, and ```update(received)``` drops the self-tick on the remote path.

    - **The file [```delivery.py```](src/tree_crdt/clock/delivery.py):** **(New in Phase 3)** The file where you implement the ```DeliveryClock``` class.

  - **The directory [```payload```](src/tree_crdt/payload/):** The ```payload``` subpackage.

    - **The file [```move.py```](src/tree_crdt/payload/move.py):** The ```MovePayload``` class (provided).

  - **The directory [```tree```](src/tree_crdt/tree/):** The ```tree``` subpackage.

    - **The file [```node.py```](src/tree_crdt/tree/node.py):** The ```Node``` class (provided). Phase 3 reverts to the Phase 1 triple ```(p, m, c)```.

    - **The file [```tree.py```](src/tree_crdt/tree/tree.py):** The file where you implement the Phase 3 ```Tree```. The tree is single-valued; the return types of ```__call__```, ```__getitem__```, and ```get_active``` are tightened accordingly. ```move``` takes ```(parent, metadata, child)``` — the producer arguments of Phase 2 are gone.

  - **The directory [```raft```](src/tree_crdt/raft/):** **(New in Phase 3, mostly a reference implementation — see the note below)** The RAFT consensus implementation. ```RaftNode``` is the consensus state machine, ```RaftLog``` is the per-node log with movable base index for compaction, ```GrpcRaftServer``` and ```GrpcTransport``` are the inbound and outbound gRPC sides, and ```raft.proto``` plus the generated ```raft_pb2.py``` / ```raft_pb2_grpc.py``` files define the wire format. **You are expected to leave every file in this directory alone EXCEPT for four short, conceptually crucial methods on ```RaftNode``` in [```node.py```](src/tree_crdt/raft/node.py), which carry a ```TODO``` in the starter:**
    - ```__is_log_up_to_date``` — the "log up-to-date" check from RAFT §5.4.1 used during voting.
    - ```__quorum_size``` — the majority threshold across the full cluster.
    - ```__advance_commit_index``` — the leader's commit-index advancement, including the same-term restriction of §5.4.2 (the "Figure 8" anomaly).
    - ```__apply_committed``` — walks ```last_applied``` toward ```commit_index```, firing the apply callback once per entry.

    Together these four are ~30 lines of code. Everything else in [```src/tree_crdt/raft```](src/tree_crdt/raft/) — the gRPC plumbing, the message types, the protobuf wire format, the timer logic, the snapshot path, the role transitions — is given to you.

  - **The file [```replica.py```](src/tree_crdt/replica.py):** The file where you implement the Phase 3 ```Replica```. The constructor takes three new arguments (```raft_base```, ```peer_hosts```, ```op_limits```); the class gains a ```DeliveryClock```, a second ```Tree``` (the stable tree), absolute-index bookkeeping for the operation log, an embedded ```RaftNode``` with its gRPC sides, and the safe-index / two-stage-shutdown methods described in the spec.

- **The file [```main.py```](main.py):** The main program for Phase 3; partially implemented, you are going to complete it. Note that the **termination protocol gains a second SHUTDOWN barrier and a ```flush_snapshot``` step** — see the spec PDF for details.

- **The directory [```sample_runs```](sample_runs/):** Some example runs of the library with different configurations. (Empty in the starter package; populated as you produce Phase 3 runs.)

- **The file [```.env.example```](.env.example):** Use this file as the basis for your ```.env``` file to configure the project.

- **The directory [```tests```](tests/):** A small subset of unit tests provided to help you smoke-test the helper classes you must implement. **Please note that you are not allowed to modify any of the contents of this directory.** Coverage in this starter package is intentionally narrow; passing these tests does **not** prove your implementation is correct overall — you are responsible for verifying the broader behaviour (safe-index admissibility, two-tree split, RAFT-driven compaction, two-stage shutdown, convergence under concurrency, thread safety) yourself, ideally with your own additional tests.

- **The file [```pyproject.toml```](pyproject.toml):** Normally, you should not need to deal with this file; if you think that you ever need to do so, you can reach out the course staff. **(Phase 3 note: ```grpcio-tools``` is now a dependency.)**

- **The files named ```__init__.py```:** You do not need to deal with these files as these are already set for you.

- **The file [```py.typed```](src/tree_crdt/py.typed):** You do not need to deal with this file.

## Setting up the project

### How to set up the project workflow?

- Install ```uv``` on your machine for handling the (isolated) Python environment for the project and managing dependencies: [Installation | uv](https://docs.astral.sh/uv/getting-started/installation)

- Run the following command to create the Python environment for the project:
  ```bash
  uv venv
  ```

- After creating the environment, activate it on your terminal:
  ```bash
  source .venv/bin/activate
  ```

- Once you activated the environment, install the dependencies:
  ```bash
  uv sync
  ```

- You can check the installed packages using the following command:
  ```bash
  uv pip list
  ```

## How to configure the environment variables before running the project?

Create a ```.env``` file in your root directory in a similar structure to the provided [```.env.example```](.env.example) file, with the following fields set:

- ```HOSTS```: Comma-separated IP addresses for each replica, ordered ${0,1,2,...,i}$.

  - For the replicas you plan to run on your machine locally, assign **127.0.0.1** as the IP address.

- ```MAIN_BASE```: The base port number for the **MOVE-PUB** sockets (and the per-peer **BARRIER-REQ** sockets used by the DONE/SHUTDOWN protocols) on the main thread of the replicas. (See [```replica.py```](src/tree_crdt/replica.py) and [```main.py```](main.py).)

- ```LISTENER_BASE```: The base port number for the **MOVE-SUB** sockets and the **BARRIER-REP** socket of the listener thread of the replicas. (See [```replica.py```](src/tree_crdt/replica.py) and [```main.py```](main.py).)

- ```RAFT_BASE```: **(New in Phase 3)** The base port number for the embedded **RAFT gRPC server** on each replica. Replica $i$ binds on ```host:RAFT_BASE + i```. (See [```replica.py```](src/tree_crdt/replica.py) and [```main.py```](main.py).)

- ```NUM_OP_MESSAGES```: **(New in Phase 3)** Comma-separated, per-replica operation limits. Entry $i$ is the total number of MOVE/DELETE operations replica $i$ will ever generate. The length of this list must match the length of ```HOSTS```. Used both to terminate the local generation loop and as the saturation vector $L$ in the safe-index admissibility checks (see Section "Termination and Saturated Fields" in the spec).

- ```TREE_CONFIG```: The structure requested for the tree. Should be set to ```hierarchical```, ```wide```, or ```chain```. (See [```main.py```](main.py).)
  - ```hierarchical```: Generates a tree with a root node, three children, and their grandchildren as the program runs.
  - ```wide```: Generates a tree such that there is a root node and all the new nodes are generated to be children of the root node.
  - ```chain```: Generates a tree where each new node is the child of the last generated node, creating a chain-like structure.

## Generating the RAFT protobuf / gRPC bindings

The RAFT wire format is declared in [```src/tree_crdt/raft/raft.proto```](src/tree_crdt/raft/raft.proto). The generated Python modules ```raft_pb2.py``` and ```raft_pb2_grpc.py``` are **not** checked into the repository — you must regenerate them once after cloning (and again whenever ```raft.proto``` changes). Run this from the project root:

```bash
uv run python -m grpc_tools.protoc \
  -I src/tree_crdt/raft \
  --python_out=src/tree_crdt/raft \
  --grpc_python_out=src/tree_crdt/raft \
  src/tree_crdt/raft/raft.proto
```

What this does:

- ```-I src/tree_crdt/raft``` tells ```protoc``` where to find imported ```.proto``` files (here, the directory containing ```raft.proto``` itself).
- ```--python_out=...``` emits ```raft_pb2.py```, the generated message classes (```RequestVote```, ```AppendEntries```, ```LogEntry```, etc.).
- ```--grpc_python_out=...``` emits ```raft_pb2_grpc.py```, the generated gRPC stubs and servicer base class used by [```GrpcRaftServer``` and ```GrpcTransport```](src/tree_crdt/raft/grpc_transport.py).

The dependency ```grpcio-tools``` is already declared in [```pyproject.toml```](pyproject.toml), so ```uv sync``` is sufficient to make the ```grpc_tools.protoc``` module available. If the import in [```src/tree_crdt/raft/__init__.py```](src/tree_crdt/raft/__init__.py) fails with ```ModuleNotFoundError: No module named 'raft_pb2'```, it means the generated files are missing — re-run the command above.

## How to run the project?

If you did not modify the contents of the project, you can directly run the main program:

```bash
uv run main.py # Runs main.py on the environment's Python interpreter
```

If you had modifications, however, you will first need to reinstall the package:

```bash
uv pip install -e . # Installs (or reinstalls) tree-crdt in development mode
```

- After reinstalling the package, you can run the script ```main.py``` as described above.

- After you complete running ```main.py```, you are going to see files in the ```runs``` directory with the following name format: ```<run-id>_replica_<replica-id>.txt```; you can check the final versions of the tree (current tree and stable snapshot) on your replicas from that file.

## How to run the unit tests provided in the project?

You can run the unit tests, which reside in the directory ```tests/```, through the following command:

```bash
uv run python -m unittest discover -s tests -t . -v
```

## What are the required packages for the ```tree-crdt``` library?

- ```pyzmq```: Required for using ZeroMQ sockets in Python.

- ```python-dotenv```: Required for getting the environment variables from the ```.env``` files in Python.

- ```grpcio``` / ```grpcio-tools```: **(New in Phase 3)** Required by the bundled RAFT reference implementation.

- ```mypy``` and ```ty```: Set up as development dependencies for type checking. You can use them with the following commands:

  ```bash
  uv run ty check
  ```

  ```bash
  uv run mypy .
  ```
