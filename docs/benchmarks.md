# Failover benchmark

How long the cluster takes to notice that a chunkserver has died, and to restore every lost replica.

## Setup

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

## Method

Each run starts from a clean cluster (`docker compose down -v`, then `up -d`), waits until all 4 chunkservers are heartbeating, uploads the file, and stops cs2. Three timestamps are taken:

- **T_stop:** the container's `State.FinishedAt` from `docker inspect`, i.e. when the cs2 process exited.
- **T_unavailable:** the `time` field of the coordinator's `node state changed` log line for cs2 with `state: unavailable`.
- **T_repaired:** the `time` field of the first `repair round finished` log line after T_unavailable.

All three come from the Docker VM's clock (the Docker daemon and the coordinator run inside the same VM), so they can be subtracted directly. The Windows host clock is never used. The script polls the logs, but the times come from the log lines themselves, so how often it polls doesn't affect the results.

- **detection** = T_unavailable − T_stop
- **repair** = T_repaired − T_unavailable
- **total** = T_repaired − T_stop

A run fails if it hasn't finished within 60 s.

## Results

20 of 20 runs succeeded. Every run repaired 4 chunks: with 5 chunks placed round-robin across 4 nodes, cs2 held a copy of 4 of them.

| Metric | min | median | p95 | max |
|---|---|---|---|---|
| detection | 2240 ms | 2812.5 ms | 3113 ms | 3148 ms |
| repair | 554 ms | 593.5 ms | 1562 ms | 1623 ms |
| total | 2829 ms | 3429.5 ms | 3816 ms | 4442 ms |

p95 is the nearest-rank percentile (the 19th of 20 sorted values).

Repair times fall into two groups: 18 runs between 554 and 649 ms, and 2 runs at 1562 and 1623 ms.

## Analysis

**Detection is about 2–3.5 s, as designed.** The 3 s Unavailable threshold counts from the node's *last* heartbeat, not from when it stopped. Heartbeats go out every second, so the last one was sent between 0 and 1 s before the stop, which puts the threshold 2–3 s after T_stop. The state only changes on a sweep, which adds up to 0.5 s. The measured range of 2240–3148 ms falls inside that window.

**Repair time is mostly waiting for the next repair round.** The copies themselves are quick: in the demo's coordinator log, all four `chunk re-replicated` lines and the `repair round finished` line fall within 2 ms of each other. What varies is how long the coordinator waits after marking the node Unavailable before its next 2 s repair tick fires. Both the sweeper (500 ms) and the repair loop (2 s) tick on fixed schedules from coordinator start. So that wait takes only a few distinct values, set by which sweep tick happened to mark the node: here about 0.6 s in 18 runs and about 1.6 s in 2 runs. In no run did a repair round miss the chunks and leave them to a later round; that would have added a further 2 s.

**Trade-offs and possible improvements:**

- **Lower thresholds** would detect failures sooner, but a brief pause (a GC pause, a slow disk, a network blip) would then look like a dead node and trigger needless re-replication. That's why there's a Suspect state: a Suspect node gets no new chunks, but nothing is copied until it's Unavailable.
- **Repair on state change:** starting a repair round as soon as the sweeper marks a node Unavailable, in addition to the 2 s ticker, would remove the 0.6–1.6 s wait. That could cut total time to roughly detection plus copy time, for little added complexity: the sweeper would signal the repair loop over a channel. This is not implemented.

## Caveats

- Everything runs on one machine: all 5 containers share one host, one Docker VM and one Docker network.
- The file is small (5 chunks), so each repair round copies only 4 chunks of 4 MiB.
- These numbers measure the liveness and repair logic: the timers, state transitions and repair scheduling. They don't measure network or disk throughput, or behaviour at larger scale (more nodes, more chunks, real network latency).

## Reproduce

From the repo root (works from any directory), with Docker Desktop running:

```powershell
.\scripts\bench-failover.ps1 -Runs 20            # defaults: -FileMB 20 -Node cs2
```

The script writes one row per run to `results\failover-<timestamp>.csv` (git-ignored). It prints min / median / p95 / max for each metric, the number of failed runs, and the environment (Docker version, Docker VM CPUs and memory, host CPU and OS). It tears the cluster down with `docker compose down -v` at the end.
