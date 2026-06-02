# Delta Lake

## Purpose of this lesson

Parquet (lesson 13) gives you efficient columnar files, but a *pile of Parquet files is not a reliable table*. There's no transaction boundary, no protection against a half-written job, no safe concurrent writes, no schema enforcement, no record of versions, and no atomic updates or deletes. **Delta Lake** is the table format that adds all of this on top of Parquet, turning a directory of files into a database-grade table while keeping the open, decoupled-storage lakehouse model. This lesson explains the transaction log (`_delta_log`), ACID transactions, optimistic concurrency, schema enforcement/evolution, time travel, the DML operations (MERGE/UPDATE/DELETE), maintenance (compaction, vacuum), data skipping, and Delta's relationship to Spark and to Fabric. It is the keystone of lakehouse reliability and underpins the medallion architecture (lesson 15).

## Core mental model

```text
Delta table = Parquet data files  +  a transaction log that defines the table's TRUTH

Delta table/
  ├── part-0001.parquet           ← actual data (columnar, lesson 13)
  ├── part-0002.parquet
  └── _delta_log/
        ├── 000000.json           ← commit 0: "added files A, B"
        ├── 000001.json           ← commit 1: "removed A, added C" (an UPDATE/DELETE/MERGE)
        ├── 000002.json
        └── ...                   ← each commit is an atomic, ordered transaction
```

The **log is the table**, not the files. A reader determines the table's current state by replaying the log to learn *which* files are currently valid. This indirection is what enables ACID, time travel, and safe concurrency.

## Main explanation

### What Delta Lake is

Delta Lake is an open-source **table format**: a specification for organizing Parquet data files plus a transaction log that together provide database-like guarantees over object storage. It is *not* a compute engine and *not* a storage system — it's the metadata-and-protocol layer that makes a set of files behave like a transactional table. Engines (Spark, and others) implement the Delta protocol to read and write it. In Microsoft Fabric, Lakehouse tables are Delta tables stored in OneLake; the same is true on Databricks and elsewhere. Delta is what turns "compute over a data lake" into "a lakehouse."

### Why Parquet alone is not enough for reliable tables

Plain Parquet in a directory has serious gaps for production tables:

- **No atomicity.** A job writing many files can fail halfway, leaving partial/garbage files; readers might see an inconsistent mix. There's no "all-or-nothing."
- **No isolation.** If a reader scans while a writer adds/removes files, the reader can see a torn state. Concurrent writers can corrupt each other.
- **No update/delete.** Parquet files are immutable; there's no transactional way to update or delete specific rows — you'd rewrite directories manually and unsafely.
- **No schema enforcement.** Nothing stops a job from appending files with the wrong schema, silently corrupting the table.
- **No history/versioning.** Once files are overwritten, the old state is gone; no audit, no rollback.

Delta closes all of these gaps with the transaction log.

### The transaction log and `_delta_log`

The heart of Delta is the **transaction log**, stored in the `_delta_log/` directory as an ordered sequence of JSON commit files (`000000.json`, `000001.json`, ...), periodically compacted into Parquet **checkpoints** for fast replay. Each commit records, atomically, the **actions** that constitute one transaction: which data files were **added**, which were **removed**, and metadata (schema, partitioning, statistics, commit info). To read the table at a given version, an engine replays the log up to that commit and computes the set of currently-valid files (added minus removed). Crucially, the **data files are never modified in place** — operations work by *adding new files and marking old ones removed in the log*. The log, not a file listing, is the source of truth: a Parquet file physically present but not referenced by the current log state is simply *not part of the table*.

### ACID transactions

The log gives Delta full **ACID** guarantees:

- **Atomicity.** A transaction is committed by a single atomic action: writing the next log file (`00000N.json`). Either that commit succeeds (all its added/removed files take effect together) or it doesn't (no partial effect). A failed job that wrote data files but never committed leaves *untracked* files that are invisible to readers and cleaned up later — the table is never half-updated.
- **Consistency.** Schema enforcement and constraints keep the table valid across commits.
- **Isolation.** Readers see a consistent snapshot (the log state at the version they started reading) — snapshot isolation. A concurrent write that commits later doesn't affect an in-progress read.
- **Durability.** Once committed to the log in durable object storage, the change persists.

This is the leap from "files in a lake" to "a real table": you can write, update, and delete with the same all-or-nothing, isolated semantics you expect from a database — over cheap open storage.

### Optimistic concurrency control

How does Delta let multiple writers proceed safely without heavyweight locks? **Optimistic concurrency control (OCC)**: a writer reads the current table version, does its work assuming no conflict, and attempts to commit as the *next* log version (e.g., it read version 5, so it tries to write `000006.json`). If another writer committed `000006.json` first, the commit fails (the version is taken); the writer **detects the conflict, re-reads the new state, checks whether the conflict is genuine** (did the other commit touch overlapping data?), and **retries** if safe. This works well because conflicts are rare in typical append/partition-isolated workloads — most writers touch different files/partitions and commit without contention. OCC gives serializable-style safety without locking, fitting the distributed, multi-writer lakehouse.

## Key concepts

- **Transaction log (`_delta_log`)** — ordered commits defining the table's valid file set; the source of truth.
- **ACID** — atomic, consistent, isolated, durable table operations over object storage.
- **Optimistic concurrency** — conflict-detect-and-retry instead of locking.
- **Schema enforcement** — rejecting writes that don't match the table schema.
- **Time travel** — querying or restoring previous table versions via the log.
- **MERGE/UPDATE/DELETE** — transactional DML implemented by rewriting affected files and committing.
- **Data skipping** — using log-stored file statistics to skip files that can't match a filter.

## Detailed technical sections

### Schema enforcement and schema evolution

**Schema enforcement** (schema-on-write) means Delta checks every write against the table's declared schema and **rejects** incompatible data (wrong types, missing required columns, unexpected columns) — preventing silent corruption that plain Parquet allows. This makes Delta tables trustworthy *contracts*. **Schema evolution** is the controlled way to *change* the schema when you intend to (e.g., adding a column): you opt in (`mergeSchema`, `ALTER TABLE`, or auto-merge settings), and Delta records the new schema in the log so future reads/writes use it, while old data files (lacking the new column) read back nulls. The combination — strict by default, evolvable on purpose — is exactly what production tables need: protection against accidents, flexibility for intentional change. This is far safer than the ad-hoc schema inference of text formats (lesson 13).

### Time travel and table versions

Because every commit is an immutable, ordered log entry and old data files aren't deleted immediately, Delta supports **time travel**: you can query the table *as of* a previous **version number** or **timestamp** (`VERSION AS OF 41`, `TIMESTAMP AS OF '2026-01-01'`). The engine replays the log to that point to reconstruct the historical valid file set and reads those files. Uses: auditing (what did the table look like last Tuesday?), debugging (compare versions to find when bad data entered), reproducibility (train/report against a fixed version), and **rollback** (`RESTORE` to a prior version after a bad load). Time travel is bounded by retention — files needed by old versions remain until `VACUUM` removes them (below), so very old versions become unqueryable after vacuuming. **Delta table versions** are simply the sequence of log commits; each write produces a new version.

### MERGE, UPDATE, and DELETE

Delta supports transactional DML that plain Parquet cannot:

- **UPDATE** — change values in matching rows. Delta finds files containing matching rows, **rewrites those files** with the updated values (and unchanged rows from those files), adds the new files, and removes the old ones in one commit.
- **DELETE** — remove matching rows by rewriting the affected files without those rows (and committing the swap).
- **MERGE** (upsert) — the workhorse for incremental pipelines: given a source of changes and a match condition, MERGE can *update* matched rows, *insert* new rows, and *delete* others, all in one atomic transaction. It's the standard way to apply change-data-capture (CDC) or incremental loads into Silver/Gold tables (lesson 15).

The key mechanism is **copy-on-write**: because Parquet files are immutable, modifying any rows means rewriting the *files* containing them (not editing in place) and atomically swapping file references in the log. This means a small update touching rows scattered across many files can rewrite many files — a cost to be aware of, mitigated by good partitioning/clustering so changes localize to fewer files. (Some Delta versions also support *deletion vectors* / merge-on-read to mark deleted rows without immediately rewriting, deferring the rewrite — an optimization layered on the same log model.)

### Compaction (OPTIMIZE) and the small-file problem

Frequent writes — especially streaming or many small incremental loads — produce **many small files** (lesson 13's small-file problem), which degrade read performance. **Compaction** (`OPTIMIZE`) rewrites many small files into fewer, larger, right-sized files in a transaction (adding the big files, removing the smalls, committing the swap) without changing the data. Many engines also support **Z-ordering** (or liquid clustering) during/with optimize: physically co-locating rows by the values of chosen columns so that data skipping (below) becomes far more effective for queries filtering on those columns — a *finer-grained* alternative to physical partitioning that avoids the high-cardinality-partition trap (lesson 10). Regular compaction is essential maintenance for any actively-written Delta table.

### Vacuum and retention

Because operations leave old data files around (for time travel and safety), storage accumulates obsolete files. **VACUUM** physically deletes data files that are no longer referenced by the current table state *and* are older than a **retention period** (default commonly 7 days). The retention window protects in-flight readers and time-travel access: vacuuming too aggressively (short retention) reclaims space but **destroys the ability to time-travel** to versions whose files were removed, and can break long-running readers. So VACUUM trades storage cost against history depth and safety. The discipline: compact regularly, vacuum periodically with a retention long enough to cover your time-travel and audit needs.

### Data skipping

Delta stores **per-file statistics** (min/max values per column, row counts, null counts) in the transaction log. When a query filters on a column, Delta consults these stats to **skip entire files** whose min/max range can't satisfy the predicate — *before* reading them. This is predicate pushdown at the **table/file** granularity (complementing Parquet's row-group skipping, lesson 13), and it's what makes selective queries over large Delta tables fast without needing physical partitioning on every filtered column. **Z-ordering/clustering** amplifies it by physically sorting data so that values are clustered into fewer files, tightening each file's min/max range and letting more files be skipped. Data skipping + clustering is the modern alternative to over-partitioning (lesson 10): you get fine-grained skipping without the high-cardinality directory explosion.

### Batch and streaming interoperability

A major Delta strength: the *same* Delta table is simultaneously a **batch table and a streaming source/sink**. A streaming job can write into a Delta table (each micro-batch is a transaction), and another job — batch or streaming — can read it; a Delta table can serve as a streaming *source* (reading new commits incrementally) or *sink*. The transaction log provides the offsets/versions that make exactly-once streaming writes possible (lessons 16, 18). This unification means you don't need separate systems for batch and streaming storage — one Delta table serves both, which simplifies lakehouse architecture enormously (the same Silver table can be batch-rebuilt or streamed into).

### Why Delta is central in lakehouse systems

The lakehouse promise (lesson 01) is "data-lake economics and openness with data-warehouse reliability." Delta delivers the reliability half: ACID transactions, schema enforcement, time travel, and DML over open Parquet on cheap object storage, readable by multiple engines. Without a table format, a lakehouse is just a data lake with all of MapReduce-era reliability problems. Delta (and its peers Iceberg/Hudi) is the component that makes the lakehouse trustworthy enough to run production analytics and even some operational workloads. It's why "lakehouse" and "Delta-style table format" are nearly synonymous in practice.

### How Delta relates to Spark, and to Fabric Lakehouse

**Delta relates to Spark** as *table format to compute engine*: Spark implements the Delta protocol to read the log, skip files, write data, and commit transactions. Delta is *not* part of Spark's engine — it's a format Spark (and other engines) speak. You write `df.write.format("delta")...` and Spark performs the Delta commit; you read a Delta table and Spark replays the log to find valid files. **In Fabric**, Lakehouse tables *are* Delta tables in OneLake; when you `saveAsTable` from a Fabric notebook, you create/update a Delta table whose log lives in OneLake, and that same table is queryable by the Fabric SQL endpoint, Power BI (via Direct Lake), and other engines — the decoupled, open lakehouse model made concrete. Fabric abstracts provisioning and storage location but the Delta format and its guarantees are exactly as described here.

### Why Delta is a table format, not a compute engine

This distinction matters architecturally. A **compute engine** (Spark) *executes computations*. A **table format** (Delta) *defines how a table's data and metadata are organized and what guarantees they provide*. Delta has no executors, no query planner, no shuffle — it's a specification plus a log. Conflating them leads to confusion like "Delta makes my queries faster" (it can, via data skipping, but it doesn't *execute* anything) or "I need Delta to run Spark" (you don't — Spark reads many formats; Delta adds *reliability* to tables). Keep the four-box model (lesson 01): Spark = compute, Delta = the table/metadata layer over storage, object storage = bytes, orchestration = scheduling.

## PySpark and SQL perspective

```python
# Write a Delta table (a transaction: add files + commit to _delta_log)
df.write.format("delta").mode("overwrite").saveAsTable("silver_events")

# Incremental upsert via MERGE — the core of incremental pipelines
from delta.tables import DeltaTable
target = DeltaTable.forName(spark, "silver_events")
(target.alias("t")
   .merge(updates.alias("s"), "t.event_id = s.event_id")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute())
```

The `write` is one Delta transaction: Spark writes new Parquet files and atomically commits a log entry; readers see either the old table or the fully new one, never a torn state. The `MERGE` finds files containing matching `event_id`s, rewrites them with updates/inserts applied (copy-on-write), and commits the file swap atomically — applying a CDC/incremental batch with database semantics over the lake. In SQL the same operations read naturally:

```sql
MERGE INTO silver_events t USING updates s ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM silver_events VERSION AS OF 41;        -- time travel
OPTIMIZE silver_events;                               -- compaction (+ ZORDER BY (col))
DELETE FROM silver_events WHERE study_id = 'S999';    -- transactional delete
```

Each statement is a Delta transaction recorded in the log. The `VERSION AS OF` query replays the log to version 41 and reads that historical file set — auditability and rollback for free. These capabilities are precisely what plain Parquet (lesson 13) cannot offer, and they're why Silver/Gold tables (lesson 15) are Delta.

## Architectural implications

- **Make Bronze/Silver/Gold tables Delta** to get ACID, schema enforcement, history, and DML (lesson 15).
- **Use MERGE for incremental/CDC loads** instead of full rewrites where possible — but watch copy-on-write file rewrite cost.
- **Compact regularly (`OPTIMIZE`) and cluster/Z-order** on common filter columns instead of over-partitioning (lesson 10).
- **Set VACUUM retention to cover your time-travel/audit needs**; don't vacuum so aggressively that you lose required history.
- **Lean on data skipping** (log statistics + clustering) for selective queries rather than high-cardinality partitioning.
- **Remember Delta is the reliability layer, not the engine** — performance still comes from Spark execution and good layout.

## Common misconceptions

- *"Delta is a faster version of Parquet."* Delta *uses* Parquet; it adds reliability (ACID, history, DML) and can speed queries via skipping, but it's a format, not a faster file.
- *"The data files are the table."* No — the `_delta_log` defines the valid file set; untracked files aren't part of the table.
- *"Delta locks the table for writes."* No — it uses optimistic concurrency (detect-and-retry), not locks.
- *"Time travel is free forever."* It's bounded by retention; `VACUUM` removes old files and ends access to those versions.
- *"UPDATE/DELETE edit files in place."* No — copy-on-write rewrites affected files and swaps references atomically.
- *"I need Delta to use Spark."* No — Spark reads many formats; Delta adds table reliability on top.

## Summary

Delta Lake is an open table format that makes a directory of immutable Parquet files behave like a transactional database table, via an ordered transaction log (`_delta_log`) that is the table's source of truth. The log delivers ACID transactions (atomic commits, snapshot isolation, durability), optimistic concurrency (detect-and-retry instead of locks), schema enforcement with controlled evolution, time travel to prior versions, transactional MERGE/UPDATE/DELETE (via copy-on-write file rewrites), and per-file data skipping from log-stored statistics. Maintenance — OPTIMIZE compaction, clustering/Z-ordering, and VACUUM with a retention window — keeps tables fast and bounded. Delta unifies batch and streaming on one table and is the reliability foundation of the lakehouse and the medallion architecture. Critically, Delta is a *table format*, not a compute engine: Spark executes; Delta defines the table and its guarantees over open storage.

## Terms to remember

- **Delta Lake** — open table format adding ACID/history/DML over Parquet.
- **Transaction log (`_delta_log`)** — ordered commits defining the valid file set; the table's truth.
- **ACID / optimistic concurrency** — database-grade guarantees via detect-and-retry commits.
- **Schema enforcement / evolution** — reject incompatible writes / change schema intentionally.
- **Time travel** — query/restore prior versions by number or timestamp.
- **MERGE (upsert)** — atomic update/insert/delete; the incremental-load workhorse (copy-on-write).
- **OPTIMIZE / Z-order / VACUUM** — compaction / clustering for skipping / retention-bounded file cleanup.
- **Data skipping** — skipping whole files via log statistics.
