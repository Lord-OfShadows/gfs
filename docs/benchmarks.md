# Benchmarks

Two benchmarks, both on a single laptop running the Docker Compose cluster: [failover](#failover) (how fast a dead node is detected and its chunks re-replicated) and [latency](#latency) (upload/download times, including during a failure and during repair).

## Failover

How long the cluster takes to notice that a chunkserver has died, and to restore every lost replica.

### Setup

| | |
|---|---|
| Command | `.\scripts\bench-failover.ps1 -Runs 20` |
| Cluster | Docker Compose: 1 coordinator + 4 chunkservers, replication factor 3, all on one laptop |
| Workload | one 20 MB random file (5 chunks of 4 MiB), uploaded fresh in every run |
| Failure | `docker compose stop cs2` |
| Host | Intel Core i7-12650H (10 cores / 16 threads), 16 GB RAM, NVMe SSD, Windows 11, plugged in |
| Docker | Docker Desktop 29.1.3; Docker VM with 16 CPUs and 7.6 GiB RAM |

Liveness and repair settings (constants in the code):

| Setting | Value |
|---|---|
| Heartbeat interval | 1 s |
| Suspect after | 2 s without a heartbeat |
| Unavailable after | 3 s without a heartbeat |
| Sweeper (re-checks node states) | every 500 ms |
| Repair round | every 2 s |
| Repair workers | 4 |

### Method

Each run starts from a clean cluster (`docker compose down -v`, then `up -d`), waits until all 4 chunkservers are heartbeating, uploads the file, and stops cs2. Three timestamps are taken:

- **T_stop:** the container's `State.FinishedAt` from `docker inspect`, i.e. when the cs2 process exited.
- **T_unavailable:** the `time` field of the coordinator's `node state changed` log line for cs2 with `state: unavailable`.
- **T_repaired:** the `time` field of the first `repair round finished` log line after T_unavailable.

All three come from the Docker VM's clock (the Docker daemon and the coordinator run inside the same VM), so they can be subtracted directly. The Windows host clock is never used. The script polls the logs, but the times come from the log lines themselves, so how often it polls doesn't affect the results.

- **detection** = T_unavailable − T_stop
- **repair** = T_repaired − T_unavailable
- **total** = T_repaired − T_stop

A run fails if it hasn't finished within 60 s.

### Results

20 of 20 runs succeeded. Every run repaired 4 chunks: with 5 chunks placed round-robin across 4 nodes, cs2 held a copy of 4 of them.

| Metric | min | median | p95 | max |
|---|---|---|---|---|
| detection | 2240 ms | 2812.5 ms | 3113 ms | 3148 ms |
| repair | 554 ms | 593.5 ms | 1562 ms | 1623 ms |
| total | 2829 ms | 3429.5 ms | 3816 ms | 4442 ms |

p95 is the nearest-rank percentile (the 19th of 20 sorted values).

Repair times fall into two groups: 18 runs between 554 and 649 ms, and 2 runs at 1562 and 1623 ms.

### Analysis

**Detection is about 2–3.5 s, as designed.** The 3 s Unavailable threshold counts from the node's *last* heartbeat, not from when it stopped. Heartbeats go out every second, so the last one was sent between 0 and 1 s before the stop, which puts the threshold 2–3 s after T_stop. The state only changes on a sweep, which adds up to 0.5 s. The measured range of 2240–3148 ms falls inside that window.

**Repair time is mostly waiting for the next repair round.** The copies themselves are quick: in the demo's coordinator log, all four `chunk re-replicated` lines and the `repair round finished` line fall within 2 ms of each other. What varies is how long the coordinator waits after marking the node Unavailable before its next 2 s repair tick fires. Both the sweeper (500 ms) and the repair loop (2 s) tick on fixed schedules from coordinator start. So that wait takes only a few distinct values, set by which sweep tick happened to mark the node: here about 0.6 s in 18 runs and about 1.6 s in 2 runs. In no run did a repair round miss the chunks and leave them to a later round; that would have added a further 2 s.

**Trade-offs and possible improvements:**

- **Lower thresholds** would detect failures sooner, but a brief pause (a GC pause, a slow disk, a network blip) would then look like a dead node and trigger needless re-replication. That's why there's a Suspect state: a Suspect node gets no new chunks, but nothing is copied until it's Unavailable.
- **Repair on state change:** starting a repair round as soon as the sweeper marks a node Unavailable, in addition to the 2 s ticker, would remove the 0.6–1.6 s wait. That could cut total time to roughly detection plus copy time, for little added complexity: the sweeper would signal the repair loop over a channel. This is not implemented.

### Caveats

- Everything runs on one machine: all 5 containers share one host, one Docker VM and one Docker network.
- The file is small (5 chunks), so each repair round copies only 4 chunks of 4 MiB.
- These numbers measure the liveness and repair logic: the timers, state transitions and repair scheduling. They don't measure network or disk throughput, or behaviour at larger scale (more nodes, more chunks, real network latency).

### Reproduce

From the repo root (works from any directory), with Docker Desktop running:

```powershell
.\scripts\bench-failover.ps1 -Runs 20            # defaults: -FileMB 20 -Node cs2
```

The script writes one row per run to `results\failover-<timestamp>.csv` (git-ignored). It prints min / median / p95 / max for each metric, the number of failed runs, and the environment (Docker version, Docker VM CPUs and memory, host CPU and OS). It tears the cluster down with `docker compose down -v` at the end.

## Latency

How long uploads and downloads take as seen by the client: the cost of replication, and what happens to reads and writes while a node is failing or being repaired.

### Setup and method

| | |
|---|---|
| Command | `.\scripts\bench-latency.ps1` (defaults: `-Runs 10 -FileMB 20 -BigFileMB 400`) |
| Environment | same as the failover benchmark above |
| Files | 20 MB random files (5 chunks of 4 MiB); a 400 MB file in scenario D (100 chunks) |
| Stopped node | cs2 |

**Latency** is the duration the CLI itself reports (`downloaded ... in 509ms`), measured inside the client process. It excludes `docker compose run`'s container startup, which is on the order of a second and varies. Every download is checked against the original file's SHA-256. Each command's container start time is also recorded (from `docker inspect`, on the Docker VM's clock), to place it relative to the stop and to the coordinator's log lines.

Scenarios:

- **A: replication cost.** A fresh cluster with `--replication 1`, then one with `--replication 3`. In each, 10 different files are uploaded and each is downloaded once.
- **B: reads during the detection window** (replication 3). Upload a file, stop cs2, and download straight away (before the coordinator has noticed). Wait for the `unavailable` log line, then download again. cs2 is restarted and back to Healthy before the next run.
- **C: writes during the detection window.** Same as B, but the two operations are uploads of new files.
- **D: reads during repair** (fresh cluster per run). Upload the 400 MB file and a 20 MB file, and take a baseline download of the 20 MB file. Stop cs2, wait for `unavailable`, then download the 20 MB file again while the 400 MB file's lost replicas are re-copied. A download counts as *overlapping* the repair if the `repair round finished` line comes after the download started. Repair rounds don't log when they start, so this shows the round was still unfinished when the download began. It doesn't show how much copying ran alongside it.

### Results

All times in ms. p95 is the nearest-rank percentile; with 9–10 samples it is the maximum.

| Scenario | Variant | n | Failed | min | median | p95 | max |
|---|---|---|---|---|---|---|---|
| A | upload, replication 1 | 10 | 0 | 744 | 807.5 | 990 | 990 |
| A | download, replication 1 | 10 | 0 | 459 | 528 | 545 | 545 |
| A | upload, replication 3 | 10 | 0 | 764 | 989.5 | 1551 | 1551 |
| A | download, replication 3 | 10 | 0 | 449 | 509.5 | 605 | 605 |
| B | download during detection | 9 | 0 | 4199 | 4405 | 4574 | 4574 |
| B | download after unavailable | 9 | 0 | 461 | 495 | 544 | 544 |
| C | upload during detection | 9 | 5 | 959 | 1042 | 1082 | 1082 |
| C | upload after unavailable | 9 | 0 | 913 | 1012 | 1126 | 1126 |
| D | baseline download | 10 | 0 | 450 | 513 | 951 | 951 |
| D | download during repair | 10 | 0 | 551 | 624.5 | 1136 | 1136 |

The C "during detection" times are for the 4 uploads that succeeded.

- **B:** the "during detection" downloads started 632–936 ms after cs2 stopped, all before cs2 was marked unavailable.
- **C:** all 5 failures had the same error: `stored locally but forwarding failed`.
- **D:** all 10 downloads overlapped a repair round, and each round re-replicated 79 chunks.
- **Setup failures:** B and C ran 9 times each, not 10, because the setup upload failed twice (B run 8 and C run 4). Both times this happened right after cs2 had been restarted, with the same forwarding error.

### Analysis

**Replication costs about 23% on upload here, not 3×.** The median 20 MB upload went from 807.5 ms with 1 replica to 989.5 ms with 3: +182 ms, +23%. Spread over 5 chunks and 2 extra hops per chunk, that's roughly 18 ms per extra hop per 4 MiB chunk. On one host, a hop is a copy over the loopback-speed Docker network plus a local fsync, so the client's fixed costs dominate: reading the file twice, checksums, and the RPCs to the coordinator. On a real network each hop would add a full transfer time per chunk, because a server receives the whole chunk before forwarding it. That's where cut-through pipelining, as in GFS (forwarding data while it's still arriving), would help. Reads are unaffected (509.5 vs 528 ms), since a download reads one replica per chunk.

**Reads during the detection window pay about 3.9 s.** Downloads started before cs2 was marked unavailable took a median 4405 ms, against 495 ms once it was, about 3.9 s extra. Until the coordinator notices, cs2 still counts as Healthy, so it's listed first for the chunks where it's first in the original order, and the client tries it first. The likely cause of the long wait (not confirmed) is that a stopped container's address disappears from the Docker network. A connection attempt then gets no response, rather than a fast refusal, and the client waits until gRPC gives up before falling back to the next replica. Once cs2 is marked unavailable, health-ordered replicas put it last and the penalty disappears entirely. Possible fixes, neither implemented: a short per-attempt deadline in `fetchChunk`, or hedged reads (ask a second replica if the first hasn't answered within a short delay).

**Writes during the detection window can fail.** 5 of 9 uploads started during the window failed: their write chain still included cs2, and forwarding to it failed. With cs2 Healthy, round-robin placement over 4 nodes puts it in the chain for 4 of the 5 chunks. So the 4 uploads that succeeded most likely got their placement after cs2 had turned Suspect, which excludes it from new placements. Once cs2 was Unavailable, 9 of 9 uploads succeeded at normal latency (median 1012 ms). Possible fix, not implemented: the client retries a failed upload with a fresh `CreateFile` plan, which would leave out the failing node once the coordinator has noticed. A failed upload also leaves a pending file and orphan chunks behind (see the README's known limitations).

**Uploads right after a node restarts can fail.** Two setup uploads failed with the same forwarding error, each right after cs2 had come back. The likely cause (not confirmed) is that the other chunkservers' cached gRPC connections to cs2 were still in reconnect backoff from when it was down. A forward sent in that state fails at once instead of waiting for the connection. Possible fixes: retry forwards, or use gRPC's wait-for-ready option on forwarding calls.

**Repair interferes only a little with reads.** While a round re-replicated 79 chunks (about 316 MiB), the median small download went from 513 to 624.5 ms, +22%. The worker pool limits repair to 4 copies at a time. The highest values in both D variants (951 and 1136 ms) are single runs, so medians are the better comparison here.

### Caveats

- Single host: every "network" hop is between containers on one machine, so these numbers say little about bandwidth-bound behaviour on a real network.
- 9–10 samples per variant: p95 equals the maximum and is sensitive to a single slow run.
- The causes marked "likely" above are inferred from the timings and error messages, not confirmed with network traces.
