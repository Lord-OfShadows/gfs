**PRODUCT REQUIREMENTS DOCUMENT**

GFS: Distributed File System Replica

*A fault-tolerant, chunk-based distributed storage system*

**Target stack: Go · gRPC · Protocol Buffers · Docker**

This PRD is derived from the project description on the resume. It
defines a buildable specification, not a claim that the listed features
or performance targets have already been implemented or verified.

# 1. Executive summary

Build a small, Google File System (GFS)-inspired distributed file system
that stores files as chunks across multiple storage nodes. A coordinator
(master) owns namespace and chunk-location metadata; chunkservers store
the bytes. A Go client communicates with the system over gRPC using
Protocol Buffers. Docker Compose runs a repeatable local cluster. The
system should detect unavailable chunkservers, mark their replicas
suspect, and re-replicate recoverable chunks so that a single
storage-node failure does not make data unavailable when a healthy
replica remains.

# 2. Problem statement

A file stored on one machine is vulnerable to machine, disk, and process
failures. This project demonstrates the core distributed-systems
mechanisms used to improve availability: splitting files into chunks,
maintaining metadata separately from data, replicating chunks, detecting
node failures, and repairing under-replicated data.

# 3. Goals and non-goals

## Goals

- Upload, download, list, and delete files through a CLI client.

- Split files into fixed-size chunks and assign each chunk a stable
  identifier.

- Keep namespace and chunk-location metadata in a single coordinator for
  the first version.

- Store multiple replicas of each chunk on independent chunkservers.

- Detect missed heartbeats, exclude unavailable nodes from reads, and
  schedule replica repair.

- Validate data integrity using checksums and verify that downloaded
  files match their original bytes.

- Run a cluster locally with Docker Compose and make failures
  reproducible through tests.

- Expose enough logs, metrics, and tests to explain the system's
  behavior in an interview.

## Non-goals for the first release

- Production-grade multi-master operation or consensus-based coordinator
  election.

- POSIX/FUSE mount support, permissions/ACLs, snapshots, compression,
  encryption, or cloud deployment.

- Arbitrary concurrent writers to the same file with full transactional
  semantics.

- Claiming 99.9% uptime or a three-second recovery guarantee before
  repeatable tests establish it.

- Matching Google's internal GFS implementation or its scale.

# 4. Users and primary workflows

| **User**               | **Need**                                       | **Success outcome**                                                            |
|------------------------|------------------------------------------------|--------------------------------------------------------------------------------|
| Developer / operator   | Start a local cluster and inspect node health. | One command starts coordinator and chunkservers; health status is visible.     |
| CLI client             | Upload and retrieve a file.                    | Downloaded bytes match uploaded bytes exactly.                                 |
| System under test      | Survive a chunkserver failure.                 | Reads use a healthy replica and repair restores the target replication factor. |
| Reviewer / interviewer | Understand guarantees and trade-offs.          | Architecture, failure behavior, metrics, and limitations are documented.       |

# Primary workflow: upload

1.  Client asks the coordinator to create a file and obtain chunk
    assignments.

2.  Client splits the file into chunks and computes a checksum for each
    chunk.

3.  Coordinator selects chunkservers and returns the primary/replica
    destinations for each chunk.

4.  Client streams chunk data to the designated servers; the system
    confirms each successful durable write.

5.  Coordinator records the chunk metadata and acknowledges the file
    only after the required number of replicas is confirmed.

6.  Client receives a success response with file metadata.

# Primary workflow: download

7.  Client asks the coordinator for the file's chunk IDs, lengths,
    checksums, and replica locations.

8.  For each chunk, the client chooses a healthy replica and fetches the
    bytes.

9.  Client validates the checksum and retries another replica if a read
    fails or data is corrupt.

10. Client concatenates chunks in order and writes the completed file
    locally.

# Primary workflow: node failure and repair

11. Chunkservers send heartbeats at a configured interval.

12. After a configurable missed-heartbeat threshold, the coordinator
    marks a node unavailable and stops assigning it new reads/writes.

13. The coordinator identifies chunks whose healthy replica count is
    below the configured replication factor.

14. A healthy source replica copies each affected chunk to a replacement
    chunkserver.

15. The destination verifies the checksum and reports completion; only
    then is the new replica marked healthy.

16. Repair stops when the target replication factor is restored or no
    eligible destination/source is available.

# 5. Functional requirements

| **ID** | **Requirement**        | **Acceptance criteria**                                                                                                                                |
|--------|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| FR-01  | Namespace operations   | Client can create/upload, list, download, and delete files; invalid paths return clear errors.                                                         |
| FR-02  | Chunking               | Files larger than one chunk are split into ordered chunks; empty and small files work correctly.                                                       |
| FR-03  | Chunk storage          | Chunkservers persist chunks to local disk using chunk IDs, not user-provided filenames.                                                                |
| FR-04  | Replication            | Default replication factor is 3 where enough nodes exist; each chunk's replica locations are tracked.                                                  |
| FR-05  | Integrity              | Every stored chunk has a checksum; corrupted reads are detected and another replica is tried.                                                          |
| FR-06  | Heartbeat and liveness | Coordinator tracks last heartbeat and marks a node unavailable after the configured timeout.                                                           |
| FR-07  | Automatic repair       | When a chunk falls below its target healthy replica count, repair is queued and eventually restores the target if capacity and a healthy source exist. |
| FR-08  | Consistency            | The coordinator is the single metadata authority; an upload is not reported successful until the required write acknowledgements arrive.               |
| FR-09  | Concurrency            | Concurrent independent file operations do not corrupt metadata or chunk contents; shared state is protected with appropriate synchronization.          |
| FR-10  | Observability          | Structured logs identify request, file, chunk, and node; basic metrics expose node state, replica count, failures, and repair duration.                |
| FR-11  | Deployment             | Docker Compose starts one coordinator and at least three chunkservers with persistent storage volumes.                                                 |
| FR-12  | Failure testing        | A test can stop a chunkserver, verify reads from a surviving replica, and verify eventual re-replication.                                              |

# 6. Architecture

| **Component**        | **Responsibilities**                                                                            | **Important design choice**                                          |
|----------------------|-------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| Go client / CLI      | File operations, chunk splitting, checksum verification, retrying replicas.                     | Keep the user-facing interface simple; no FUSE in MVP.               |
| Coordinator / master | Namespace, file-to-chunk mapping, chunk locations, node liveness, placement, repair scheduling. | Single active coordinator in MVP; metadata is authoritative here.    |
| Chunkserver          | Write/read/delete chunk bytes, report health, serve replica-copy requests.                      | Store chunk data on disk and verify checksum on read/write.          |
| gRPC + Protobuf      | Typed service contracts between client, coordinator, and chunkservers.                          | Version message schemas carefully; define deadlines and error codes. |
| Docker Compose       | Repeatable local cluster and fault-injection environment.                                       | Use separate containers and persistent volumes per chunkserver.      |

## Suggested service boundaries

- Coordinator service: CreateFile, GetFileMetadata, ListFiles,
  DeleteFile, Heartbeat, ReportChunk, GetRepairTasks.

- Chunkserver service: WriteChunk, ReadChunk, DeleteChunk, CopyChunk,
  GetHealth.

- Client commands: upload, download, ls, rm, status, and optionally
  inspect \<file\>.

# 7. Data model

| **Entity**    | **Key fields**                                                    | **Purpose**                                                       |
|---------------|-------------------------------------------------------------------|-------------------------------------------------------------------|
| FileMetadata  | path, file_id, ordered chunk_ids, size_bytes, created_at, version | Maps a user-visible file to its ordered chunks.                   |
| ChunkMetadata | chunk_id, file_id, index, length, checksum, target_replication    | Describes immutable chunk contents and expected replicas.         |
| Replica       | chunk_id, node_id, state, last_verified_at                        | Tracks a chunk copy as healthy, suspect, copying, or unavailable. |
| NodeMetadata  | node_id, address, state, last_heartbeat, capacity                 | Tracks chunkserver identity, liveness, and placement eligibility. |
| RepairTask    | task_id, chunk_id, source_node, target_node, state, attempts      | Tracks asynchronous replica repair and failures.                  |

Persistence recommendation: start with a simple local metadata store
(for example, a durable embedded database) or a carefully checkpointed
metadata file. Do not rely only on in-memory maps if restart recovery is
part of the demo. Chunk bytes remain on each chunkserver's local
persistent volume.

# 8. Consistency and failure semantics

- Metadata authority: one active coordinator. This avoids split-brain
  complexity in the first version.

- File visibility: a file is visible as successfully uploaded only after
  all chunks have met the configured acknowledgement policy.

- Write policy: chunks are immutable in MVP. Updating a file creates a
  new file version or replacement chunk set rather than mutating a chunk
  in place.

- Delete policy: coordinator marks file/chunks deleted and schedules
  replica deletion; garbage collection must not remove chunks still
  referenced by a live file version.

- Node failure: a timeout is evidence of unreachability, not proof that
  a machine is permanently dead. Mark it unavailable and repair
  conservatively.

- Network partition: coordinator cannot safely guarantee progress to an
  isolated node. Nodes that cannot reach the coordinator should not
  independently authorize metadata changes.

- Coordinator failure: MVP may be unavailable until coordinator metadata
  is recovered/restarted. Automatic leader election is explicitly out of
  scope.

- Durability: acknowledgement means data was written to the
  chunkserver's persistent storage and the configured number of replicas
  acknowledged. Document filesystem/OS assumptions; do not overstate
  hardware-level durability.

# 9. Non-functional requirements and measurable targets

| **Area**          | **Initial target**                                                                                                                       | **How to verify**                                                                       |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| Correctness       | 100% byte-for-byte equality for successful upload/download test corpus.                                                                  | Automated tests for empty, small, multi-chunk, binary, and large files.                 |
| Replication       | Target replication factor 3 when at least 3 eligible chunkservers exist.                                                                 | Inspect replica metadata and stored copies.                                             |
| Failure detection | Mark a stopped node unavailable within 3 seconds under the chosen heartbeat configuration.                                               | Repeated process-kill tests; report median and p95.                                     |
| Repair            | Restore missing replicas within 10 seconds for a small test file in a local 4-node cluster, when a source and destination are available. | Fault-injection test with measured repair duration. Treat as a target, not a guarantee. |
| Concurrency       | Pass a test with 1,000 concurrent client operations without data corruption or metadata races.                                           | Load test; report throughput, p50/p95 latency, error rate, CPU, and memory.             |
| Reliability       | Survive one chunkserver loss for data with at least one healthy replica.                                                                 | Stop a node during and after uploads; verify reads and repair.                          |
| Security          | Bind services to the local Docker network by default; validate paths and request sizes.                                                  | Negative tests for path traversal and malformed requests.                               |

Important: the resume's '1,000+ simultaneous reads/writes', 'strong
consistency', '3-second recovery', and '99.9% uptime' are not
automatically guaranteed by the architecture. Treat them as claims to
earn through tests. For a local portfolio project, a reproducible
benchmark report is more credible than an unsupported uptime percentage.

# 10. Concurrency design

- Keep coordinator metadata updates behind a small repository/service
  layer and use locks or transactional storage rather than exposing
  shared maps to handlers.

- Use bounded worker pools for background repair; avoid spawning
  unbounded goroutines for every task.

- Make repair tasks idempotent: retrying a task must not create
  conflicting metadata or corrupt a chunk.

- Give every RPC a deadline and define retryable versus non-retryable
  errors.

- Use request IDs or operation IDs for retryable writes so client
  retries do not create duplicate file/chunk records.

- Do not hold global metadata locks while copying large chunk payloads;
  reserve/update metadata briefly, perform I/O, then commit the result.

# 11. API sketch

| **RPC / command**      | **Input**                                      | **Output / behavior**                                      |
|------------------------|------------------------------------------------|------------------------------------------------------------|
| CreateFile             | path, size, chunk checksums                    | file_id and chunk placement plan, or typed error.          |
| GetFileMetadata        | path                                           | ordered chunks, checksums, and available replicas.         |
| WriteChunk             | chunk_id, bytes/stream, checksum, operation_id | acknowledgement after validation and persistence.          |
| ReadChunk              | chunk_id                                       | stream of bytes plus checksum/length metadata.             |
| Heartbeat              | node_id, capacity, local health                | coordinator response with node status/config.              |
| ReportChunk            | node_id, chunk_id, checksum                    | reconcile local inventory with coordinator metadata.       |
| DeleteFile             | path, expected version (optional)              | logical deletion followed by safe replica cleanup.         |
| upload/download/status | CLI arguments                                  | human-readable output and non-zero exit status on failure. |

The API sketch is illustrative, not a fixed protobuf schema. Define
concrete .proto files only after the state model and failure semantics
are agreed.

# 12. Testing strategy

## Unit tests

- Chunking boundaries: 0 bytes, 1 byte, exactly one chunk, one byte over
  a chunk, and many chunks.

- Checksum calculation and corruption detection.

- Replica placement avoids duplicate node assignments for a chunk.

- Metadata transitions: healthy → suspect/unavailable → copying →
  healthy.

- Repair task retries are idempotent.

## Integration tests

- Upload/download round trip for text, binary, empty, and multi-chunk
  files.

- Stop one chunkserver; reads continue from another replica.

- Restart a node; inventory reconciliation does not overwrite newer
  authoritative metadata.

- Corrupt one stored chunk; client detects it and reads a healthy
  replica.

- Kill a node and verify repair restores the target replica count.

- Run parallel uploads/downloads and verify checksums and namespace
  correctness.

## Fault-injection / benchmark tests

- Kill a chunkserver during a read and during an upload.

- Delay or drop heartbeat traffic and observe false-positive failure
  detection behavior.

- Run 1,000 concurrent operations as a stress test; separate read/write
  mix and file sizes.

- Record benchmark environment, number of nodes, data size, concurrency,
  throughput, p50/p95 latency, errors, and recovery time.

- Repeat failure tests at least 20 times before reporting a
  recovery-time percentile.

# 13. Delivery plan

| **Milestone**                 | **Scope**                                                                          | **Exit criteria**                                                                   |
|-------------------------------|------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| M0 — Contracts                | Write architecture notes, .proto service definitions, error model, and invariants. | A request can be traced through client → coordinator → chunkserver.                 |
| M1 — Single-node file store   | CLI upload/download/list; local chunkserver; chunk IDs and checksums.              | Round-trip tests pass.                                                              |
| M2 — Coordinator + chunking   | Namespace and chunk map; files split into multiple chunks.                         | Multi-chunk round trips pass.                                                       |
| M3 — Replication              | Three chunkservers; placement and write acknowledgements.                          | Every chunk has target replicas; one node can be lost without losing readable data. |
| M4 — Heartbeats and repair    | Liveness state machine, repair queue, replica copy, checksum verification.         | Automated failure test restores replication.                                        |
| M5 — Concurrency and recovery | Bounded workers, metadata persistence, restart reconciliation, load tests.         | Parallel operations pass; restart behavior documented.                              |
| M6 — Portfolio polish         | README, architecture diagram, demo script, benchmark report, limitations.          | A reviewer can reproduce the demo from a clean checkout.                            |

# 14. Risks and mitigations

| **Risk**                             | **Why it matters**                                                          | **Mitigation**                                                                               |
|--------------------------------------|-----------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| Metadata lost on coordinator restart | File namespace and replica map may disappear.                               | Persist metadata and test restart recovery before claiming resilience.                       |
| False failure detection              | Slow node may be treated as dead and receive duplicate replicas.            | Use configurable thresholds and explicit replica states; make repair idempotent.             |
| Inconsistent metadata vs disk        | A node may have chunks that coordinator does not know about, or vice versa. | Reconcile chunkserver inventory after restart; checksum before adopting a replica.           |
| Partial upload                       | Some chunks may exist even when file upload fails.                          | Track upload state and clean up orphan chunks safely.                                        |
| Overclaiming performance             | Numbers without methodology are difficult to defend.                        | Publish reproducible scripts, environment, raw results, and measured percentiles.            |
| Scope creep                          | Consensus, FUSE, and production hardening can consume the project timeline. | Finish a reliable single-coordinator MVP first; add advanced features only after tests pass. |

# 15. Definition of done

- A clean checkout can start one coordinator and three or four
  chunkservers with one documented command.

- CLI can upload, list, download, and delete files.

- Chunking, replication, checksums, heartbeat-based failure detection,
  and automatic re-replication work in integration tests.

- A single chunkserver failure does not prevent reading a file when a
  healthy replica exists.

- Coordinator metadata survives a normal restart, or the limitation and
  recovery procedure are explicitly documented.

- Concurrency and fault-injection tests run from scripts and produce
  reproducible output.

- README contains an architecture diagram, setup steps, RPC overview,
  consistency/failure guarantees, benchmark methodology, and known
  limitations.

- Every numeric performance claim in the resume is supported by a
  repeatable test or is removed/softened.

# 16. Resume-claim evidence checklist

| **Potential resume claim**                                            | **Evidence required before using it**                                                                                                           |
|-----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| Fault-tolerant distributed file system                                | Demo showing a chunkserver failure while reads continue from a healthy replica.                                                                 |
| Automated re-replication and stale replica detection within 3 seconds | Repeated failure-injection runs with a stated timeout configuration and measured p50/p95 detection time.                                        |
| 1,000+ simultaneous reads/writes                                      | Benchmark harness, machine/container specs, operation mix, data sizes, success/error rate, throughput and latency.                              |
| Strong consistency                                                    | Precise consistency definition and tests for concurrent operations; otherwise describe the actual single-coordinator/immutable-chunk guarantee. |
| 99.9% uptime under network partitions                                 | A defined measurement period and availability SLI, plus partition scenarios and data. Do not claim this from a short local demo.                |

# 17. Recommended build order

17. Start with a single chunkserver and end-to-end upload/download. Do
    not begin with failure recovery.

18. Add coordinator metadata and multi-chunk files.

19. Add replication across three chunkservers and checksum validation.

20. Add heartbeat state transitions and failure-aware reads.

21. Add asynchronous, bounded replica repair and test node loss
    repeatedly.

22. Add persistent metadata and restart reconciliation.

23. Only then stress-test concurrency and tune performance.

24. Finish with a reproducible demo, benchmark report, architecture
    diagram, and honest limitations.

# Appendix: design principles to preserve

- Keep the metadata plane separate from the chunk-data plane.

- Prefer explicit state machines and typed errors over implicit boolean
  flags.

- Treat timeouts as suspicion, not certainty.

- Make background repair safe to retry.

- Measure end-to-end behavior, not just individual RPC speed.

- A smaller system with demonstrated guarantees is stronger than a large
  system with unverified claims.
