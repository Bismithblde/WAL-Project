# WAL Project - Concurrent C++ Key-Value Database

A Windows TCP key-value database written in C++17. The server supports line-based commands and RESP2, uses a central `WSAPoll` readiness loop with a fixed worker pool, and persists acknowledged writes through a batched write-ahead log (WAL).

## Highlights

- **182K median operations/sec** at 64 concurrent clients in the primary mixed-workload benchmark.
- **159K median operations/sec** at 512 concurrent clients.
- 64 independently locked, cache-friendly `unordered_dense` storage shards.
- RESP2 pipelining, partial-read handling, and buffered nonblocking writes.
- Batched WAL commits with disk commit before an `OK` acknowledgement is released.
- WAL replay for `SET` and `DELETE`, plus compaction while writes are active.

## Architecture

```text
TCP clients
    |
    v
Central WSAPoll loop
    |  read-ready clients
    v
Fixed worker pool
    |-- parses line protocol or RESP2 requests
    |-- GET reads one storage shard
    |-- SET / DELETE reserve an ordered response slot
    v
WAL coordinator
    |-- batches writes for up to 10 ms
    |-- writes and commits the batch to disk
    |-- releases responses in client command order
    v
Buffered nonblocking socket output
```

### Storage and durability

The database hashes each key into one of 64 shards. Each shard owns an `ankerl::unordered_dense::map` and a `shared_mutex`, so reads and writes to unrelated keys do not share one global database lock.

`SET` and `DELETE` operations are queued to the WAL coordinator. The coordinator batches pending records for up to 10 ms, appends them to `wal.txt`, flushes the C runtime buffer, and calls `_commit` before completing the client response. Ordered response slots ensure a pipelined command cannot receive a later response before an earlier durable write acknowledgement.

`COMPACT` snapshots all shards under a fixed lock order and replaces the WAL with the current state. WAL replay restores both `SET` and `DELETE` records when the server restarts.

## Supported commands

Line protocol examples:

```text
SET user_1 Ryan
GET user_1
DELETE user_1
COMPACT
SUBSCRIBE updates
PUBLISH updates hello
EXIT
```

The server also auto-detects RESP2 arrays beginning with `*` and supports pipelined RESP2 requests.

## Setup

### Requirements

- Windows
- C++17-capable MinGW compiler, such as MSYS2 UCRT64 `g++`
- GNU Make
- Python 3.12+ for the end-to-end test suite
- [vcpkg](https://github.com/microsoft/vcpkg)

Install the declared dependency from the repository root:

```powershell
vcpkg install --triplet x64-mingw-dynamic
```

Build and run:

```powershell
make
.\main.exe
```

The server listens on `127.0.0.1:6379`.

## Tests and benchmarks

```powershell
# Full protocol, recovery, compaction, connection, and benchmark suite
make test-claims

# Educational comparison: custom linear-probing map vs. unordered_dense
make learning-map
.\learning_flat_map.exe
```

The primary benchmark uses a deliberately disclosed, local-loopback workload:

- RESP2 protocol
- 16-request pipeline depth
- 1,000-key working set
- 95% GET / 5% SET mix
- 120,000 operations per trial
- 3 trials per client count
- Warm-up before timing

Latest measured results:

| Concurrent clients | Median ops/sec | Min to max ops/sec | Completed operations | Errors |
| ---: | ---: | ---: | ---: | ---: |
| 64 | 182,475 | 181,927 to 183,515 | 360,000 | 0 |
| 512 | 158,973 | 158,325 to 160,662 | 360,000 | 0 |

The suite also verifies 500+ concurrent connections, bounded server thread usage, line and RESP2 pipelining, WAL recovery for acknowledged writes and deletes, compaction reduction, and compaction during active writes.

## Benchmark caveat

These figures are from a local loopback benchmark, not a multi-machine production deployment. They are useful for comparing revisions on the same machine, but network latency, disk hardware, client implementation, and operating-system settings will affect real deployment throughput.

## Learning component

`src/learning_flat_map.cpp` contains a small open-addressing, linear-probing hash table with resizing, updates, tombstones, deletion, and reinsertion. It is intentionally separate from production storage, which uses the tested `unordered_dense` implementation. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for dependency and license details.
