# LSM-Engine

![Java](https://img.shields.io/badge/Java-21-blue?logo=openjdk)
![Build](https://img.shields.io/badge/build-passing-brightgreen)
![Tests](https://img.shields.io/badge/tests-24%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![From Scratch](https://img.shields.io/badge/zero%20external%20DB%20libraries-from%20scratch-orange)

A from-scratch implementation of a **Log-Structured Merge-Tree (LSM-Tree)** storage engine in Java — the same core architecture that powers RocksDB, LevelDB, Apache Cassandra, and ScyllaDB.

Built to understand and demonstrate every internal layer of a production storage engine: write-ahead logging, in-memory buffers, sorted on-disk tables, compaction, bloom filters, sparse indexing, and checksums.

---

## Features

- **Write-Ahead Log (WAL)** — binary append-only log with CRC32 per record; full crash recovery on startup
- **Group Commit** — background fsync batching delivers ~50× throughput over strict per-write sync (4,191 vs 84 ops/sec measured)
- **MemTable** — `ConcurrentSkipListMap` for O(log n) thread-safe sorted writes; flushed to disk when full
- **SSTable** — immutable sorted files with a 16-byte magic footer; all data is byte-ordered and CRC32-protected
- **Sparse Index** — one in-memory index entry per 32 records; binary search + short linear scan cuts RAM by 32× vs full index
- **Bloom Filter** — Murmur3 double-hashing over a BitSet; definitively skips SSTables for absent keys with zero false negatives
- **Tiered Compaction** — background thread merges oldest SSTable pairs; drops stale versions and tombstones at bottommost level
- **Tiered Storage** — keep N newest SSTables on fast local disk; move cold files to a remote/slow directory transparently
- **Manifest** — JSON catalog of all SSTables; enables exact recovery without scanning files on restart
- **Range Scan** — merge-iterates MemTable + all SSTables in sorted order with tombstone filtering
- **Zero external database libraries** — every layer built from scratch in plain Java 21

---

## Table of Contents

1. [What is an LSM-Tree?](#1-what-is-an-lsm-tree)
2. [Why LSM over B-Trees?](#2-why-lsm-over-b-trees)
3. [System Architecture](#3-system-architecture)
4. [Write Path — How data gets stored](#4-write-path)
5. [Read Path — How data gets retrieved](#5-read-path)
6. [Compaction — Keeping reads fast](#6-compaction)
7. [Component Deep-Dive](#7-component-deep-dive)
   - [Write-Ahead Log (WAL)](#71-write-ahead-log-wal)
   - [MemTable](#72-memtable)
   - [SSTable](#73-sstable-sorted-string-table)
   - [Sparse Index](#74-sparse-index)
   - [Bloom Filter](#75-bloom-filter)
   - [Manifest](#76-manifest)
   - [Tiered Storage](#77-tiered-storage)
8. [On-Disk File Formats](#8-on-disk-file-formats)
9. [Durability Guarantees](#9-durability-guarantees)
10. [Deletion — Tombstones](#10-deletion--tombstones)
11. [Configuration Reference](#11-configuration-reference)
12. [Technology Stack](#12-technology-stack)
13. [Project Structure](#13-project-structure)
14. [Building and Running](#14-building-and-running)
15. [Test Coverage](#15-test-coverage)
16. [Design Decisions & Trade-offs](#16-design-decisions--trade-offs)

---

## 1. What is an LSM-Tree?

A **Log-Structured Merge-Tree** is a data structure optimized for **write-heavy workloads**. Unlike traditional databases that update data in-place on disk (random writes), an LSM-tree turns every write into an **append** — the fastest possible disk operation.

The key insight:

> **Sequential I/O is 10–100× faster than random I/O**, even on SSDs. LSM-trees exploit this by never updating existing data. Every write goes to the end of a log. Old data is cleaned up later, in bulk, in the background.

This architecture is the backbone of:
- **RocksDB** (Meta's storage engine, used in MySQL, TiKV, MongoDB WiredTiger)
- **LevelDB** (Google, basis for RocksDB)
- **Apache Cassandra** (Netflix, Apple, Discord scale)
- **ScyllaDB** (Cassandra-compatible, C++ rewrite)
- **InfluxDB, CockroachDB, Yugabyte**

---

## 2. Why LSM over B-Trees?

| Property | B-Tree | LSM-Tree |
|---|---|---|
| Write I/O | Random (update in-place) | Sequential (append-only) |
| Write amplification | Low (~1×) | Higher (compaction rewrites data) |
| Read I/O | Direct (one lookup) | Multiple files (MemTable + SSTables) |
| Space amplification | Low | Higher (stale values until compaction) |
| Best use case | Read-heavy, OLTP | Write-heavy, time-series, event logs |
| Real-world users | PostgreSQL, MySQL InnoDB | RocksDB, Cassandra, LevelDB |

**LSM wins when writes dominate** — logging, metrics, event sourcing, IoT, change-data-capture.

---

## 3. System Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                          LSM-ENGINE  —  FULL ARCHITECTURE                      ║
╚══════════════════════════════════════════════════════════════════════════════════╝

                                  ┌─────────────────────────────┐
                                  │           CLIENT            │
                                  │  put()  get()  delete()     │
                                  │  scan()  flush()  close()   │
                                  └──────────────┬──────────────┘
                                                 │
                                                 ▼
                        ╔════════════════════════════════════════╗
                        ║         StorageEngine  API             ║
                        ║      LSMStorageEngine.java             ║
                        ╚═══════════╦════════════════════════════╝
                                    ║
              ╔═════════════════════╩══════════════════════╗
              ║                 WRITE PATH                 ║
              ║  1. Assign monotonic sequence number       ║
              ║  2. Append record to WAL  (crash safety)   ║
              ║  3. Insert into MemTable  (in-memory)      ║
              ║  4. If MemTable full  →  flush to SSTable  ║
              ╚══════════════════════════════════════════╤═╝
                                                         │
                    ┌────────────────────────────────────┤
                    │                                    │
                    ▼                                    ▼
       ┌────────────────────────┐          ┌────────────────────────────┐
       │   Write-Ahead Log      │          │        MemTable            │
       │      (WAL)             │          │  ConcurrentSkipListMap     │
       │                        │          │                            │
       │  wal.log  (binary)     │          │  ● Sorted by key           │
       │  Sequential append     │          │  ● O(log n) reads/writes   │
       │  CRC32 per record      │          │  ● Thread-safe             │
       │  Group commit support  │          │  ● Tracks ~byte size       │
       │  Truncated after flush │          │  ● AtomicLong sequence #   │
       └────────────────────────┘          └─────────────┬──────────────┘
                                                         │  (when full)
                                                         │   flush ───▶
                                                         ▼
                              ┌──────────────────────────────────────────┐
                              │              SSTable  (.sst file)        │
                              │          Immutable · Sorted · On-disk    │
                              │                                          │
                              │  ┌────────────────┐  ┌───────────────┐   │
                              │  │  Bloom Filter  │  │ Sparse Index  │   │
                              │  │  (.bf  file)   │  │  (in memory)  │   │
                              │  │                │  │               │   │
                              │  │ mightContain() │  │ 1 entry per N │   │
                              │  │ → skip file if │  │ binary search │   │
                              │  │   key absent   │  │ + linear scan │   │
                              │  └────────────────┘  └───────────────┘   │
                              │                                          │
                              │  CRC32 checksum on every record          │
                              └──────────────────┬───────────────────────┘
                                                 │
                              ┌──────────────────▼───────────────────────┐
                              │            Compaction Engine             │
                              │          (background thread)             │
                              │                                          │
                              │  Wakes every 250 ms                      │
                              │  If SSTable count ≥ threshold:           │
                              │    ● Merge 2 oldest SSTables             │
                              │    ● Higher sequence number wins         │
                              │    ● Drop tombstones at bottommost level │
                              │    ● Persist manifest, delete old files  │
                              └──────────────────┬───────────────────────┘
                                                 │
                              ┌──────────────────▼───────────────────────┐
                              │            Tiered  Storage               │
                              │                                          │
                              │  Keep N newest SSTables on fast local    │
                              │  disk; move cold files to remote/slow    │
                              │  storage. Reads still work transparently │
                              └──────────────────────────────────────────┘


╔════════════════════════════════════════════════════════════════════════════════╗
║                              READ  PATH                                        ║
╠════════════════════════════════════════════════════════════════════════════════╣
║                                                                                ║
║   get(key)                                                                     ║
║      │                                                                         ║
║      ├─▶  1. MemTable          hit? ──────────────────────────▶  return value  ║
║      │        (newest data)    miss                                            ║
║      │                          │                                              ║
║      └─▶  2. SSTables  (newest ──▶ oldest)                                     ║
║               │                                                                ║
║               ├─▶  Bloom filter says NO?  ──▶  skip file entirely              ║
║               │                                                                ║
║               ├─▶  Bloom filter says maybe  ──▶  binary search sparse index    ║
║               │                                        │                       ║
║               │                               jump to nearby offset            ║
║               │                                        │                       ║
║               └─▶  linear scan  ──▶  Found    ──────▶  return value            ║
║                                  ──▶  Deleted  ──────▶  return empty           ║
║                                  ──▶  NotFound ──────▶  try next SSTable       ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
```

---

## 4. Write Path

Every write follows these exact steps in order:

```
  put("user:123", "{name: Alice}")
         │
         ▼
  ╔══════════════════════════════════════════════════════════╗
  ║  STEP 1 — Assign Sequence Number                         ║
  ╠══════════════════════════════════════════════════════════╣
  ║                                                          ║
  ║   seq = atomicCounter.incrementAndGet()  // monotonic    ║
  ║   Value v = Value.put(valueBytes, seq)                   ║
  ║                                                          ║
  ║   Every write gets a unique, ever-increasing number.     ║
  ║   Higher seq = newer. Used to resolve conflicts when     ║
  ║   two SSTables contain the same key during compaction.   ║
  ╚══════════════════════════════════════════════════════════╝
         │
         ▼
  ╔══════════════════════════════════════════════════════════╗
  ║  STEP 2 — Append to Write-Ahead Log (WAL)                ║
  ╠══════════════════════════════════════════════════════════╣
  ║                                                          ║
  ║   WALRecord r = new WALRecord(PUT, key, value, seq)      ║
  ║   long lsn = wal.append(r)                               ║
  ║                                                          ║
  ║   Written to wal.log:                                    ║
  ║   ┌──────┬─────┬────────┬────────┬─────┬───────┬─────┐   ║
  ║   │type:1│seq:8│keyLen:4│valLen:4│key… │value… │crc:4│   ║
  ║   └──────┴─────┴────────┴────────┴─────┴───────┴─────┘   ║
  ║                                                          ║
  ║   GROUP COMMIT: background thread batches fsyncs up to   ║
  ║   groupCommitMaxDelayMillis (default 5ms). Writers       ║
  ║   block on awaitDurable(lsn) until their LSN is safe.    ║
  ║   STRICT mode: fsync on every single write.              ║
  ╚══════════════════════════════════════════════════════════╝
         │
         ▼
  ╔══════════════════════════════════════════════════════════╗
  ║  STEP 3 — Insert into MemTable                           ║
  ╠══════════════════════════════════════════════════════════╣
  ║                                                          ║
  ║   memtable.put(key, value)                               ║
  ║   → ConcurrentSkipListMap.put(key, value)                ║
  ║   → approximateBytes.addAndGet(key.len + value.len + …)  ║
  ║                                                          ║
  ║   MemTable is a sorted, in-memory key-value store.       ║
  ║   Reads from it are O(log n). Data is always ordered.    ║
  ╚══════════════════════════════════════════════════════════╝
         │
         ▼  (when approximateBytes ≥ memtableMaxBytes)
  ╔══════════════════════════════════════════════════════════╗
  ║  STEP 4 — Flush MemTable → SSTable                       ║
  ╠══════════════════════════════════════════════════════════╣
  ║                                                          ║
  ║   for each entry in memtable (sorted order):             ║
  ║     ├── bloom.add(key)              // bloom filter     ║
  ║     ├── if count % N == 0:          // sparse index     ║
  ║     │     sparseIndex.add(key, offset)                  ║
  ║     └── write record + CRC32        // data section     ║
  ║                                                          ║
  ║   write sparseIndex section                             ║
  ║   write 16-byte footer (magic + indexOffset + count)    ║
  ║   channel.force(true)               // fsync to disk    ║
  ║                                                          ║
  ║   POST-FLUSH (in this exact order for crash safety):    ║
  ║     1. persistManifest()  — durable record of SSTable   ║
  ║     2. wal.truncate()     — WAL no longer needed        ║
  ║     3. reset MemTable     — start fresh                 ║
  ╚══════════════════════════════════════════════════════════╝
```

---

## 5. Read Path

```
  get("user:123")
        │
        ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │  STEP 1 — Check MemTable  (always newest data)                  │
  │                                                                 │
  │   Value v = memtable.get(key)                                   │
  │                                                                 │
  │   v == null?         ──▶  not here, go to SSTables             │
  │   v.isTombstone()?   ──▶  deleted  →  return Optional.empty()  │
  │   otherwise          ──▶  return Optional.of(v.valueBytes())    │
  └─────────────────────┬───────────────────────────────────────────┘
                        │  (miss — not in MemTable)
                        ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │  STEP 2 — Search SSTables  (newest → oldest)                    │
  │                                                                 │
  │  for each SSTableHandle h in sstablesNewestFirst:               │
  │                                                                 │
  │  ┌───────────────────────────────────────────────────────────┐  │
  │  │  2a. BLOOM FILTER CHECK                                   │  │
  │  │                                                           │  │
  │  │   h.bloom.mightContain(key)                               │  │
  │  │         │                                                 │  │
  │  │    false ──▶  key DEFINITELY absent  ──▶  skip SSTable   │  │
  │  │         │     (zero disk I/O)                            │  │
  │  │    true  ──▶  key PROBABLY present   ──▶  continue       │  │
  │  └─────────────────────────┬─────────────────────────────────┘  │
  │                            │                                    │
  │  ┌─────────────────────────▼─────────────────────────────────┐  │
  │  │  2b. SPARSE INDEX BINARY SEARCH                           │  │
  │  │                                                           │  │
  │  │   index[] loaded in RAM (1 entry per 32 records)          │  │
  │  │   binary search → largest index key ≤ target key          │  │
  │  │   → returns byte offset to jump to                        │  │
  │  │                                                           │  │
  │  │   e.g. seeking "coconut":                                 │  │
  │  │     index[2] = "cherry" @ offset 8192  ← best match      │  │
  │  │     seek directly to byte 8192 in file                    │  │
  │  └─────────────────────────┬─────────────────────────────────┘  │
  │                            │                                    │
  │  ┌─────────────────────────▼─────────────────────────────────┐  │
  │  │  2c. LINEAR SCAN FROM OFFSET                              │  │
  │  │                                                           │  │
  │  │   loop:                                                   │  │
  │  │     entry = readEntryAt(pos)                              │  │
  │  │     verify CRC32  →  throw IOException if corrupt         │  │
  │  │                                                           │  │
  │  │     entry.key == target  ──▶  Found   → return value      │  │
  │  │     entry.key == target  ──▶  Deleted → return empty      │  │
  │  │     entry.key  > target  ──▶  NotFound (data is sorted,   │  │
  │  │                               can't be further right)     │  │
  │  │     entry.key  < target  ──▶  pos = entry.nextOffset      │  │
  │  └───────────────────────────────────────────────────────────┘  │
  └─────────────────────────────────────────────────────────────────┘
        │  (not found in any SSTable)
        ▼
   return Optional.empty()
```

---

## 6. Compaction

Without compaction, reads slow down (more SSTable files to check) and disk fills with stale/deleted data.

### What compaction solves

```
  BEFORE COMPACTION                        AFTER COMPACTION
  ─────────────────────────────────────    ──────────────────────────────────────
  SSTable-0  (oldest, seq 1–100)           SSTable-merged
  ┌──────────────────────────────────┐     ┌──────────────────────────────────┐
  │  key1  →  "old-value"  (seq=1)  │     │  key1  →  "new-value"  (seq=5)  │ ← stale gone
  │  key2  →  "value-2"   (seq=2)  │     │  key2  →  "value-2"   (seq=2)  │
  │  key3  →  TOMBSTONE   (seq=3)  │     │  key4  →  "value-4"   (seq=6)  │ ← tomb gone
  └──────────────────────────────────┘     └──────────────────────────────────┘
  SSTable-1  (newer, seq 101–200)
  ┌──────────────────────────────────┐     Disk space reclaimed.
  │  key1  →  "new-value"  (seq=5)  │     Read now searches 1 file, not 2.
  │  key4  →  "value-4"   (seq=6)  │
  └──────────────────────────────────┘
```

### Compaction flow

```
  ╔══════════════════════════════════════════════════════════════════╗
  ║         BACKGROUND COMPACTION THREAD  (daemon)                  ║
  ╚══════════════════════════════════════════════════════════════════╝

  ┌──────────────────────────────────────────────────────────────┐
  │  loop every compactionSleepMillis (250 ms)                   │
  │                                                              │
  │   SSTables count < compactionThreshold (4)?                  │
  │         YES  ──▶  sleep and repeat                           │
  │          NO  ──▶  continue                                   │
  └───────────────────────────────┬──────────────────────────────┘
                                  │
                                  ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  Pick 2 oldest SSTables  (sa, sb)                            │
  │  Open SimpleSSTableScanner on both                           │
  │                                                              │
  │      Scanner A ──▶  record stream (sorted)                   │
  │      Scanner B ──▶  record stream (sorted)                   │
  │                              │                               │
  │                    TWO-POINTER MERGE                         │
  │                              │                               │
  │   ┌──────────────────────────▼───────────────────────────┐  │
  │   │  while either iterator has records:                   │  │
  │   │                                                       │  │
  │   │    compare A.key vs B.key                             │  │
  │   │    if A.key < B.key  →  emit A, advance A             │  │
  │   │    if B.key < A.key  →  emit B, advance B             │  │
  │   │    if A.key == B.key →  emit whichever seq is higher  │  │
  │   │                         advance both                  │  │
  │   │                                                       │  │
  │   │    if isBottommost AND emitted record is TOMBSTONE:   │  │
  │   │        skip it  (no older data exists below)          │  │
  │   └───────────────────────────────────────────────────────┘  │
  └───────────────────────────────┬──────────────────────────────┘
                                  │
                                  ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  Write merged stream ──▶ new SimpleSSTableWriter             │
  │  Delete old sa, sb .sst + .bf files                          │
  │  Update in-memory sstablesNewestFirst list                   │
  │  persistManifest()                                           │
  └──────────────────────────────────────────────────────────────┘
```

### Why tombstones need special handling

```
  t=1  put("x", "hello")   ──▶  SSTable-0:  x → "hello"    (seq=1)
  t=2  delete("x")         ──▶  SSTable-1:  x → TOMBSTONE  (seq=2)

  SSTable-1 is newer, so read path stops at TOMBSTONE and returns empty.  ✓
  TOMBSTONE must stay on disk — if removed early, SSTable-0's "hello"
  would resurface and corrupt the read.

  Only when compacting the BOTTOMMOST two SSTables (no older data below)
  can the TOMBSTONE be safely dropped. At that point "hello" disappears
  from disk permanently.  ✓
```

---

## 7. Component Deep-Dive

### 7.1 Write-Ahead Log (WAL)

The WAL is the crash-safety backbone. Every write is recorded here **before** touching the MemTable.
If the process dies, replay the WAL to reconstruct exactly what was in memory.

#### WAL file layout

```
  wal.log  (binary, append-only, truncated after flush)
  ┌────────────────────────────────────────────────────────────────┐
  │                                                                │
  │  ┌──────────────────────────────────────────────────────────┐  │
  │  │ RECORD                                                   │  │
  │  │  type  : 1 byte   — 0x01=PUT  0x02=DELETE                │  │
  │  │  seq   : 8 bytes  — big-endian monotonic sequence number  │  │
  │  │  kLen  : 4 bytes  — big-endian key length                 │  │
  │  │  vLen  : 4 bytes  — big-endian value length               │  │
  │  │  key   : N bytes  — raw key bytes                         │  │
  │  │  value : M bytes  — raw value bytes (absent for DELETE)   │  │
  │  │  crc32 : 4 bytes  — CRC32 over all preceding bytes        │  │
  │  └──────────────────────────────────────────────────────────┘  │
  │  ┌──────────────────────────────────────────────────────────┐  │
  │  │ RECORD  …                                                │  │
  │  └──────────────────────────────────────────────────────────┘  │
  │  …  (grows with every write, reset to 0 bytes after flush)     │
  └────────────────────────────────────────────────────────────────┘
```

#### Group Commit — how throughput is multiplied

```
  WITHOUT GROUP COMMIT               WITH GROUP COMMIT  (default: 5 ms window)
  ─────────────────────────          ──────────────────────────────────────────
                                      Writer threads                  Syncer thread
  T=0 ms  write A → fsync()          T=0 ms  A appends ──┐
  T=1 ms  write B → fsync()          T=1 ms  B appends ──┤  (sleeping)
  T=2 ms  write C → fsync()          T=2 ms  C appends ──┤
  T=3 ms  write D → fsync()          T=3 ms  D appends ──┤
          │                          T=5 ms              └──▶  fsync()
          4 fsyncs = 4 ms overhead                             signal all 4 writers
          caps at ~250 writes/sec                       │
                                     1 fsync for 4 writes
                                     → up to 4× throughput
                                     → bounded latency: max groupCommitMaxDelayMillis
```

#### WAL lifecycle

```
  Startup ──▶  wal.replay()  ──▶  rebuild MemTable from log entries
     │
  writes ──▶  wal.append(record)  ──▶  background syncer  ──▶  fsync
     │
  flush  ──▶  persistManifest()   ──▶  wal.truncate()  ──▶  file reset to 0 bytes
                   ▲                         ▲
                   └── happens FIRST ────────┘
                       (crash between these two is safe:
                        manifest has SSTable, WAL replay is idempotent)
```

---

### 7.2 MemTable

The MemTable is the in-memory write buffer — the first destination for every write,
and the first place checked on every read.

#### Skip List structure

```
  ConcurrentSkipListMap<byte[], Value>
  ═══════════════════════════════════════════════════════════════

  HEAD
   │
   │  Level 3 (express lane) ────────────────────────────────────────────────────▶
   │    │                                                          │
   │    ▼                                                          ▼
   │  [apple]─────────────────────────────────────────────────[mango]────────────▶
   │    │                                                          │
   │  Level 2 ───────────────────────────────────────────────────────────────────▶
   │    │                                          │               │
   │    ▼                                          ▼               ▼
   │  [apple]────────────────────────────────[grape]────────────[mango]──────────▶
   │    │                                          │               │
   │  Level 1 ───────────────────────────────────────────────────────────────────▶
   │    │               │               │          │               │
   │    ▼               ▼               ▼          ▼               ▼
   │  [apple]────────[cherry]────────[fig]──────[grape]─────────[mango]──────────▶
   │    │               │               │          │               │
   │  Level 0  (base layer — all keys)
   │    │       │       │       │       │  │       │       │       │
   │    ▼       ▼       ▼       ▼       ▼  ▼       ▼       ▼       ▼
   │  [apple][banana][cherry][date][elderb][fig][grape][honey][mango] …
   │
   ▼
  NULL

  O(log n) insert / lookup / delete
  O(n) sorted iteration  ← used at flush time to produce sorted SSTable
```

Each `Value` stored in the map carries:

```
  ┌─────────────────────────────────────────┐
  │  Value                                  │
  │  ─────────────────────────────────────  │
  │  byte[]  valueBytes   (null if deleted) │
  │  boolean tombstone    (true = deleted)  │
  │  long    sequence     (monotonic)       │
  │                                         │
  │  Value.put(bytes, seq)   → live entry   │
  │  Value.tombstone(seq)    → deletion     │
  └─────────────────────────────────────────┘
```

---

### 7.3 SSTable (Sorted String Table)

An SSTable is an **immutable, sorted file on disk**. Once written, it is never modified.
All writes produce a new file; old files are replaced only by compaction.

#### Complete file layout

```
  sstable-00007.sst
  ┌═══════════════════════════════════════════════════════════════════════╗
  ║                         DATA  SECTION                                 ║
  ║  (records written sequentially during flush, sorted by key)           ║
  ╠═══════════════════════════════════════════════════════════════════════╣
  ║                                                                       ║
  ║  ┌─────────────────────────────────────────────────────────────────┐  ║
  ║  │ RECORD  (repeated for every key-value pair)                     │  ║
  ║  │                                                                 │  ║
  ║  │  Offset 0       │ keyLen   │ 4 bytes │ big-endian int           │  ║
  ║  │  Offset 4       │ valLen   │ 4 bytes │ big-endian int           │  ║
  ║  │                 │          │         │ (-1 = tombstone)         │  ║
  ║  │  Offset 8       │ seq      │ 8 bytes │ big-endian long          │  ║
  ║  │  Offset 16      │ key      │ N bytes │ raw key bytes            │  ║
  ║  │  Offset 16+N    │ value    │ M bytes │ raw value bytes          │  ║
  ║  │                 │          │         │ (absent if tombstone)    │  ║
  ║  │  Offset 16+N+M  │ crc32    │ 4 bytes │ checksum of all above    │  ║
  ║  └─────────────────────────────────────────────────────────────────┘  ║
  ║  ┌─────────────────────────────────────────────────────────────────┐  ║
  ║  │ RECORD  …                                                       │  ║
  ║  └─────────────────────────────────────────────────────────────────┘  ║
  ║  …  (N total records)                                                 ║
  ║                                                                       ║
  ╠═══════════════════════════════════════════════════════════════════════╣
  ║                      SPARSE INDEX  SECTION                            ║
  ║  (one entry per sparseIndexEveryN records, written after data)        ║
  ╠═══════════════════════════════════════════════════════════════════════╣
  ║                                                                       ║
  ║  ┌─────────────────────────────────────────────────────────────────┐  ║
  ║  │ INDEX ENTRY  (repeated for each sampled record)                 │  ║
  ║  │                                                                 │  ║
  ║  │  keyLen  │ 4 bytes │ big-endian int                             │  ║
  ║  │  key     │ N bytes │ key of the sampled record                  │  ║
  ║  │  offset  │ 8 bytes │ byte offset of that record in DATA section │  ║
  ║  └─────────────────────────────────────────────────────────────────┘  ║
  ║  …  (⌈N / sparseIndexEveryN⌉ entries)                                 ║
  ║                                                                       ║
  ╠═══════════════════════════════════════════════════════════════════════╣
  ║                      FOOTER  (always last 16 bytes)                   ║
  ╠═══════════════════════════════════════════════════════════════════════╣
  ║                                                                       ║
  ║  ┌─────────────────────────────────────────────────────────────────┐  ║
  ║  │  magic       │ 4 bytes │ 0x4C534D31  =  "LSM1" in ASCII         │  ║
  ║  │  indexOffset │ 8 bytes │ byte offset where index section starts │  ║
  ║  │  indexCount  │ 4 bytes │ number of index entries                │  ║
  ║  └─────────────────────────────────────────────────────────────────┘  ║
  ╚═══════════════════════════════════════════════════════════════════════╝

  Opening an SSTable:
    1.  seek to  (fileSize − 16)  →  read footer  →  validate magic
    2.  read indexCount entries from indexOffset  →  load into memory
    3.  ready for binary-search + scan lookups
```

---

### 7.4 Sparse Index

A sparse index trades a small amount of read efficiency for a large reduction in memory.

```
  ┌─────────────────────┬───────────────────────┬───────────────────────┐
  │    Full Index       │    Sparse Index       │     No Index          │
  │   (1 entry/record)  │  (1 entry / 32 recs)  │                       │
  ├─────────────────────┼───────────────────────┼───────────────────────┤
  │  1M entries in RAM  │  31 250 entries in RAM│  0 bytes in RAM       │
  │  ~50 MB RAM/SSTable │  ~1.5 MB RAM/SSTable  │  0 MB                 │
  │  O(log n) exact     │  O(log n) + 31 reads  │  O(n) full scan       │
  └─────────────────────┴───────────────────────┴───────────────────────┘

  SSTable with 256 records,  sparseIndexEveryN = 32:

  ┌─── In-memory index (8 entries) ────────────────────────────────────┐
  │                                                                    │
  │   idx[0]  "apple"    ──▶  offset    0    (record 0)                │
  │   idx[1]  "banana"   ──▶  offset  512    (record 32)               │
  │   idx[2]  "cherry"   ──▶  offset 1024    (record 64)               │
  │   idx[3]  "date"     ──▶  offset 1536    (record 96)               │
  │   idx[4]  "elderb"   ──▶  offset 2048    (record 128)              │
  │   idx[5]  "fig"      ──▶  offset 2560    (record 160)              │
  │   idx[6]  "grape"    ──▶  offset 3072    (record 192)              │
  │   idx[7]  "honey"    ──▶  offset 3584    (record 224)              │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘

  Lookup "coconut":
    ① binary search index  →  largest key ≤ "coconut"  =  "cherry" (idx[2])
    ② seek to offset 1024
    ③ linear scan records 64, 65, 66 … until key ≥ "coconut"
    ④ at most 31 extra record reads  (cheap sequential I/O)
```

---

### 7.5 Bloom Filter

A Bloom filter is a **probabilistic bit-array** that can definitively rule out the presence
of a key in an SSTable — with **zero false negatives**.

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                  BLOOM FILTER  GUARANTEE                           │
  │                                                                    │
  │   mightContain(key) == false  →  key is DEFINITELY NOT in file     │
  │                                   → skip SSTable, zero disk I/O    │
  │                                                                    │
  │   mightContain(key) == true   →  key is PROBABLY in file           │
  │                                   → proceed to sparse index+scan   │
  │                                   (false positive rate ≈ 1%)       │
  └────────────────────────────────────────────────────────────────────┘

  HOW IT WORKS  (k = 7 hash functions, m = 2²⁰ bits ≈ 128 KB per SSTable)

  ADD "apple":
  ┌──────────────────────────────────────────────────────────────────┐
  │  h1 = murmur3("apple", seed= 0)   →  set bit  42                 │
  │  h2 = murmur3("apple", seed=h1)   →  set bit 107                 │
  │  h3 = h1 + 2·h2  (mod m)          →  set bit 271                 │
  │  h4 = h1 + 3·h2  (mod m)          →  set bit 438                 │
  │  h5 = h1 + 4·h2  (mod m)          →  set bit 519                 │
  │  h6 = h1 + 5·h2  (mod m)          →  set bit 603                 │
  │  h7 = h1 + 6·h2  (mod m)          →  set bit 711                 │
  │                                                                  │
  │  bit array: … 1 … 1 … 1 … 1 … 1 … 1 … 1 …                        │
  │               42  107 271 438 519 603 711                        │
  └──────────────────────────────────────────────────────────────────┘

  CHECK "mango"  (never added):
  ┌──────────────────────────────────────────────────────────────────┐
  │  Compute 7 bit positions for "mango"                             │
  │  Check position 88  →  0   ← ANY zero means DEFINITELY ABSENT    │
  │  → return false  (no disk read needed)                           │
  └──────────────────────────────────────────────────────────────────┘

  CHECK "apple"  (was added):
  ┌──────────────────────────────────────────────────────────────────┐
  │  Compute 7 bit positions for "apple"                             │
  │  Bits 42, 107, 271, 438, 519, 603, 711  →  all 1                 │
  │  → return true  (proceed to disk read)                           │
  └──────────────────────────────────────────────────────────────────┘

  Double-hashing:  h_i = h1 + i·h2  for i = 0..k-1
  Hash function:   Murmur3 x86-32  (fast, good distribution, non-crypto)
  Stored in:       sstable-NNNNN.bf  (serialized BitSet with 16-byte header)
```

---

### 7.6 Manifest

The manifest is the durable catalog of all SSTables — the source of truth used to reconstruct
engine state after a restart, without scanning any SSTable files.

```json
{
  "sstables": [
    {
      "sstablePath": "/data/lsm/sstable-00003.sst",
      "bloomPath":   "/data/lsm/sstable-00003.bf",
      "minKey":      "6170706c65",
      "maxKey":      "6d616e676f"
    },
    {
      "sstablePath": "/data/lsm/sstable-00004.sst",
      "bloomPath":   "/data/lsm/sstable-00004.bf",
      "minKey":      "6e6f6465",
      "maxKey":      "7a65726f"
    }
  ]
}
```

- Keys are **hex-encoded** so binary keys serialize safely as JSON strings.
- Written after every **flush** and every **compaction**.
- Read on **startup** → reload SSTable handles + warm bloom filters.
- Written **before** `wal.truncate()` — crash between the two is safe and idempotent.

---

### 7.7 Tiered Storage

```
  LOCAL DISK  (fast NVMe)              REMOTE DIR  (slow HDD / object store)
  ───────────────────────────          ──────────────────────────────────────
  sstable-00010.sst  ← newest
  sstable-00009.sst
  sstable-00008.sst
  sstable-00007.sst  ← keepLocalSstables = 4
  - - - - - - - - - -
  sstable-00006.sst ─────────────────────────────▶  sstable-00006.sst
  sstable-00005.sst ─────────────────────────────▶  sstable-00005.sst
  sstable-00004.sst ─────────────────────────────▶  sstable-00004.sst
  …                              move

  SSTableHandle tracks current path regardless of tier.
  Reads work transparently — the path in the handle is always correct.
  Move is best-effort atomic (rename; fallback to copy+delete).
```

---

## 8. On-Disk File Formats

### WAL Record

| Offset | Size | Field |
|---|---|---|
| 0 | 1 byte | `type` — `0x01` = PUT, `0x02` = DELETE |
| 1 | 8 bytes | `seq` — sequence number (big-endian long) |
| 9 | 4 bytes | `keyLen` — key length (big-endian int) |
| 13 | 4 bytes | `valLen` — value length (big-endian int, `0` for DELETE) |
| 17 | N bytes | `key` — raw key bytes |
| 17+N | M bytes | `value` — raw value bytes (absent for DELETE) |
| 17+N+M | 4 bytes | `crc32` — CRC32 over all preceding bytes |

### SSTable Record

| Offset | Size | Field |
|---|---|---|
| 0 | 4 bytes | `keyLen` — key length (big-endian int) |
| 4 | 4 bytes | `valLen` — value length; `-1` = tombstone (big-endian int) |
| 8 | 8 bytes | `seq` — sequence number (big-endian long) |
| 16 | N bytes | `key` — raw key bytes |
| 16+N | M bytes | `value` — raw value bytes (absent if tombstone) |
| 16+N+M | 4 bytes | `crc32` — CRC32 over all preceding bytes |

### SSTable Footer (last 16 bytes of every `.sst` file)

| Offset | Size | Field |
|---|---|---|
| 0 | 4 bytes | `magic` = `0x4C534D31` ("LSM1" in ASCII) |
| 4 | 8 bytes | `indexOffset` — byte offset where index section begins |
| 12 | 4 bytes | `indexCount` — number of sparse index entries |

### Bloom Filter File (`.bf`)

| Offset | Size | Field |
|---|---|---|
| 0 | 4 bytes | `magic` = `0x424C4F4D` ("BLOM" in ASCII) |
| 4 | 4 bytes | `bits` — total bit count (big-endian int) |
| 8 | 4 bytes | `k` — number of hash functions (big-endian int) |
| 12 | 4 bytes | `dataLen` — byte length of serialized BitSet |
| 16 | N bytes | serialized BitSet (raw byte array) |

---

## 9. Durability Guarantees

| Scenario | What can go wrong | Recovery |
|---|---|---|
| STRICT mode (EVERY_WRITE) | Crash after fsync but before MemTable update | WAL replayed on startup. MemTable rebuilt. Sequence numbers prevent duplicates. |
| GROUP_COMMIT mode | Crash inside the 5 ms batch window before fsync | Up to 5 ms of writes lost. Trade-off: throughput vs. RPO. |
| After flush (post-flush crash) | Crash between manifest write and WAL truncation | Manifest written FIRST. On restart: SSTable found in manifest, WAL replay is idempotent. |
| Disk corruption | Bit-flip corrupts an SSTable record | CRC32 mismatch detected. IOException thrown on reader. Scanner stops gracefully. |

---

## 10. Deletion — Tombstones

Deleting in an LSM engine cannot remove data in-place (SSTables are immutable). Instead, a **tombstone** is written — a marker that says "this key was deleted."

```
  TIMELINE

  t=1: put("user:123", "Alice")    → SSTable-0: user:123 → "Alice"   (seq=1)
  t=2: delete("user:123")          → MemTable:  user:123 → TOMBSTONE (seq=2)

  get("user:123"):
    1. MemTable → finds TOMBSTONE → return empty  ✓

  After MemTable flushes:
    SSTable-1: user:123 → TOMBSTONE (seq=2)
    SSTable-0: user:123 → "Alice"   (seq=1)

  get("user:123"):
    1. SSTable-1 (newest) → TOMBSTONE → stop → return empty  ✓
    (never reaches SSTable-0)

  After compaction (bottommost):
    Both SSTables merged. seq=2 (TOMBSTONE) beats seq=1 ("Alice").
    isBottommost=true → tombstone dropped entirely.
    Result: user:123 completely gone from disk.  ✓
```

---

## 11. Configuration Reference

### EngineConfig

| Field | Description |
|---|---|
| `dataDir` | Directory for all SSTable, WAL, bloom filter, and manifest files |
| `memtableMaxBytes` | Flush threshold in bytes (e.g. `4 * 1024 * 1024` = 4 MB) |

### LSMOptions (tuning knobs)

| Field | Default | Description |
|---|---|---|
| `bloomBits` | 2²⁰ (1M) | Bloom filter bit array size per SSTable (~128 KB) |
| `bloomK` | 7 | Hash functions per key. 7 → ~1% false positive rate |
| `sparseIndexEveryN` | 32 | One index entry per N records. Lower = faster reads, more RAM |
| `compactionThresholdSstables` | 4 | Trigger compaction when SSTable count reaches this |
| `keepLocalSstables` | 4 | Newest N SSTables stay on local disk; older ones are tiered |
| `compactionSleepMillis` | 250 | Background compaction loop wake interval in ms |
| `walSyncPolicy` | GROUP_COMMIT | `EVERY_WRITE` = max durability · `GROUP_COMMIT` = max throughput |
| `walGroupCommitMaxDelayMillis` | 5 | Max fsync batch window in ms (GROUP_COMMIT only) |

---

## 12. Technology Stack

| Technology | Version | Purpose |
|---|---|---|
| **Java** | 21 | Language — records, sealed interfaces, pattern matching |
| **Gradle** | 9.0 | Build system |
| **JUnit 5** | 5.10 | Unit + integration testing |
| **java.nio.FileChannel** | JDK | Direct file I/O, `force()` for fsync |
| **ConcurrentSkipListMap** | JDK | Thread-safe sorted MemTable |
| **java.util.zip.CRC32** | JDK | Checksum for SSTable and WAL record integrity |
| **java.util.BitSet** | JDK | Bloom filter bit array |
| **Murmur3** (inline impl) | — | Fast non-cryptographic hash for bloom filter |

**Zero external database libraries.** Every layer — WAL, MemTable, SSTable format, compaction, bloom filters, sparse index, checksums, tiering — is implemented from scratch.

---

## 13. Project Structure

```
LSM-Engine/
├── src/
│   ├── main/java/lsm/
│   │   ├── Main.java                               ← minimal usage example
│   │   ├── common/
│   │   │   ├── ByteArrayComparator.java            ← lexicographic byte[] ordering (unsigned)
│   │   │   ├── Hex.java                            ← hex encode/decode for manifest JSON
│   │   │   └── Value.java                          ← value wrapper: bytes + sequence + tombstone
│   │   ├── engine/
│   │   │   ├── StorageEngine.java                  ← public API interface
│   │   │   ├── EngineConfig.java                   ← dataDir + memtableMaxBytes
│   │   │   ├── LSMOptions.java                     ← all tuning parameters
│   │   │   ├── LSMStorageEngine.java               ← full LSM implementation
│   │   │   ├── WALStorageEngine.java               ← WAL-only engine (intermediate)
│   │   │   ├── InMemoryStorageEngine.java          ← in-memory only (no persistence)
│   │   │   ├── SSTableHandle.java                  ← SSTable metadata + bloom filter ref
│   │   │   ├── Manifest.java                       ← manifest.json read + write
│   │   │   └── Bench.java                          ← write throughput benchmark
│   │   ├── memtable/
│   │   │   ├── Memtable.java                       ← interface
│   │   │   └── SkipListMemtable.java               ← ConcurrentSkipListMap implementation
│   │   ├── wal/
│   │   │   ├── WAL.java                            ← interface
│   │   │   ├── WALRecord.java                      ← PUT | DELETE record with CRC
│   │   │   └── FileWAL.java                        ← binary WAL + group commit thread
│   │   ├── sstable/
│   │   │   ├── SSTableReader.java                  ← interface: lookup → LookupResult
│   │   │   ├── SSTableWriter.java                  ← interface: writeAll(sortedIterator)
│   │   │   ├── LookupResult.java                   ← sealed: Found | Deleted | NotFound
│   │   │   ├── SimpleSSTableFormat.java            ← footer constants
│   │   │   ├── SimpleSSTableWriter.java            ← writes data + index + footer + CRC32
│   │   │   ├── SimpleSSTableReader.java            ← loads index, binary search, CRC verify
│   │   │   └── SimpleSSTableScanner.java           ← full sequential scan (compaction)
│   │   ├── bloom/
│   │   │   ├── BloomFilter.java                    ← interface
│   │   │   ├── SimpleBloomFilter.java              ← BitSet + Murmur3 double-hashing
│   │   │   └── BloomFilterIO.java                  ← .bf file read + write
│   │   ├── index/
│   │   │   └── SparseIndex.java                    ← concept documentation
│   │   ├── compaction/
│   │   │   └── Compactor.java                      ← interface
│   │   └── tiering/
│   │       ├── StorageBackend.java                 ← pluggable file operations
│   │       ├── TieredStorageManager.java           ← interface
│   │       └── LocalDirectoryTieredStorageManager  ← moves cold SSTables to remote dir
│   └── test/java/lsm/
│       ├── engine/
│       │   ├── InMemoryStorageEngineTest.java      ← CRUD, overwrite, close behavior
│       │   ├── WALRecoveryTest.java                ← crash replay, sequence recovery
│       │   ├── SSTableFlushTest.java               ← flush, read-after-flush, bloom
│       │   ├── ManifestAndCompactionTest.java      ← compaction, WAL truncation, tombstones
│       │   └── RangeScanTest.java                  ← bounds, tombstones, multi-layer
│       └── sstable/
│           └── ChecksumTest.java                   ← CRC32 mismatch → IOException
└── scripts/
    └── bench.sh                                    ← compile + run Bench.java
```

---

## 14. Building and Running

**Prerequisites:** Java 21+, Gradle wrapper included.

```bash
# Build
./gradlew build

# Run all tests
./gradlew test

# Run a specific test class
./gradlew test --tests "lsm.engine.RangeScanTest"

# Benchmark — group commit, 200 000 puts, 1 KB values
./scripts/bench.sh

# Strict mode (one fsync per write)
./scripts/bench.sh 200000 strict

# Group commit with 10 ms batch window
./scripts/bench.sh 200000 group 10
```

### Benchmark Results

Measured on macOS, Apple M-series, NVMe SSD, 200 000 puts with 1 KB values each:

| Mode                            | Throughput | Avg Latency | Notes |
|---------------------------------|---|---|---|
| **Group Commit** (5 ms window)  | **1,000–4,000 ops/sec** | 250 µs – 1 ms | One fsync per batch of writes |
| **Group Commit** (10 ms window) | **1,000–1,200 ops/sec** | ~860 µs | Larger batch, slightly better throughput |
| **Strict** (EVERY_WRITE)        | **68–84 ops/sec** | 12–15 ms | One fsync per write |

Group commit delivers **15–50× higher throughput** over strict mode by batching multiple writes into a single `fsync` call. The trade-off is a bounded RPO of up to `groupCommitMaxDelayMillis` (5–10 ms). Strict mode guarantees zero data loss per acknowledged write at the cost of throughput.

>  The benchmark uses a 256 MB MemTable to isolate WAL behaviour — no flushes or compaction occur during the run. Numbers vary with system load and disk saturation.

**Minimal usage:**

```java
Path dir = Path.of("/tmp/my-db");
EngineConfig cfg = new EngineConfig(dir, 4 * 1024 * 1024);

try (WAL wal = FileWAL.open(dir.resolve("wal.log"), 5);
     StorageEngine engine = new LSMStorageEngine(cfg, wal)) {

    engine.put("user:1".getBytes(), "{name:Alice}".getBytes());

    Optional<byte[]> val = engine.get("user:1".getBytes());
    // → Optional.of("{name:Alice}")

    engine.delete("user:1".getBytes());

    // Range scan — keys "user:1" through "user:9" inclusive
    Iterator<Map.Entry<byte[], byte[]>> it =
        engine.scan("user:1".getBytes(), "user:9".getBytes());
    while (it.hasNext()) {
        Map.Entry<byte[], byte[]> e = it.next();
        System.out.printf("%s → %s%n",
            new String(e.getKey()), new String(e.getValue()));
    }
}
```

---

## 15. Test Coverage

| Test Class | What it covers |
|---|---|
| `InMemoryStorageEngineTest` | Basic put / get / delete, overwrite, missing key, use-after-close |
| `WALRecoveryTest` | WAL replay after simulated crash; sequence numbers survive restart |
| `SSTableFlushTest` | MemTable → SSTable flush; correct reads after flush; bloom filter populated |
| `ManifestAndCompactionTest` | Compaction correctness; WAL truncated after flush; tombstones dropped at bottommost level |
| `RangeScanTest` | Bounded range; tombstone skipping; scan across MemTable + multiple SSTables; overwrite semantics; null bounds |
| `ChecksumTest` | Corrupt a single byte on disk → CRC32 mismatch → `IOException` on lookup |

---

## 16. Design Decisions & Trade-offs

**Why `byte[]` keys and values?**
Production storage engines are byte-agnostic. `String` forces UTF-8 and disallows binary keys. `byte[]` gives the same flexibility as RocksDB — callers encode their own types.

**Why `ConcurrentSkipListMap` for MemTable?**
O(log n) concurrent reads and writes with sorted iteration — exactly what flush needs. A `TreeMap` requires external locking. A `HashMap` loses sort order.

**Why sparse index instead of a full index?**
A full index stores one entry per key — millions of records means hundreds of MB in RAM per SSTable. Sparse index with `indexEveryN = 32` cuts that 32× at the cost of at most 31 extra sequential record reads per lookup. Sequential reads on SSDs are near-free; RAM is not.

**Why Murmur3 for bloom filters?**
Fast, non-cryptographic, excellent distribution. SHA/MD5 would be 5–10× slower with no benefit — bloom filters need speed and uniform bit distribution, not cryptographic security.

**Why tiered compaction instead of leveled?**
Tiered is simpler and has better write amplification. Leveled offers better read amplification and space amplification — a meaningful next step for this engine.

**Why truncate WAL after the manifest, not before?**
The manifest is persisted first. If the process crashes between manifest write and WAL truncation, recovery finds the SSTable in the manifest, replays the WAL on top (idempotent — sequence numbers deduplicate), and reaches consistent state. Truncating before the manifest would risk losing the flush on crash.

**Why group commit by default?**
A single `fsync` takes 1–5 ms. One per write caps throughput at ~200–1000 ops/sec. Group commit batches many writes into one fsync, multiplying throughput by the batch size with only a bounded latency increase equal to `groupCommitMaxDelayMillis`.