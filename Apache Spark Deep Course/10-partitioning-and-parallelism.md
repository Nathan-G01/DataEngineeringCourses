# Partitioning and Parallelism

## Purpose of this lesson

Partitioning is how Spark divides data so it can be processed in parallel, and parallelism is the benefit you get when partitioning is right. Get partitioning wrong and you either starve your cores (too few partitions), drown in overhead (too many), or produce a storage layout that cripples future reads (small files, bad partition columns). This lesson untangles the several distinct meanings of "partition" in Spark — input partitions, shuffle partitions, output files, and table/file partitioning — and explains how each relates to tasks, cores, and performance. It connects the execution model (lesson 04) and shuffles (lesson 09) to the concrete levers `repartition`, `coalesce`, and `partitionBy`, and to lakehouse table design.

## Core mental model

```text
Data is split into PARTITIONS.   One TASK processes one partition.
Tasks run on SLOTS (cores).      Parallelism = min(partitions, total cores) per stage.

Too FEW partitions  → idle cores, huge tasks, spills, OOM.
Too MANY partitions → scheduling overhead, tiny tasks, tiny files.
Just right          → all cores busy, tasks sized to fit memory, reasonable files.
```

Partitioning is the dial that converts "amount of data" into "amount of parallel work."

## Main explanation

### What partitions are

A **partition** is a contiguous slice of a distributed dataset — the chunk of data that a single task processes on a single executor slot. A DataFrame with 200 partitions is, physically, 200 independent pieces of data that can be processed concurrently. Partitions are the *unit of parallelism*: Spark's ability to do work in parallel is bounded by how many partitions exist (you can't run more parallel tasks than you have partitions) and how many slots are available to run tasks. The word "partition" is overloaded in Spark, and disambiguating its uses is half this lesson.

### The several meanings of "partition"

| Term | What it is | Set by |
|------|------------|--------|
| **Input partitions** | Splits of source data when read | File sizes / split size config |
| **Shuffle partitions** | Partitions produced by a shuffle | `spark.sql.shuffle.partitions` (default 200) / AQE |
| **In-memory partitions** | Current partitioning of a DataFrame mid-pipeline | Inherited/changed by transformations |
| **Output files** | Files written per output partition | Number of partitions at write time / `partitionBy` |
| **Table/file partitioning** | Directory layout of a stored table by column | `partitionBy` on write (a storage concept) |

The critical confusion to avoid: **Spark in-memory partitioning** (how data is split for parallel computation right now) is *not the same* as **table/file partitioning** (how a stored table's files are laid out in directories). They interact but are different concerns, discussed below.

### Input partitions

When Spark reads source data, it splits it into **input partitions** based on file sizes and a target split size. A large Parquet file may be split into several partitions; many small files may each become a partition (or be grouped). The number of input partitions determines the parallelism of the *first* stage. This is why **file sizing matters**: thousands of tiny files create thousands of tiny input partitions (and tasks), while a few enormous files create too few partitions to fill your cores. The ideal is a moderate number of reasonably sized files (commonly targeted around 128 MB–1 GB) that split into a partition count comfortably exceeding your core count.

### Shuffle partitions

After any shuffle (lesson 09), the data is repartitioned into **shuffle partitions**, whose count defaults to `spark.sql.shuffle.partitions` = 200. This count determines the parallelism (and task count) of every post-shuffle stage. A fixed 200 is frequently wrong: too many for small data (tiny tasks, tiny files), too few for very large data (huge tasks, spills). **AQE** (lesson 19) dynamically coalesces shuffle partitions to a sensible count based on actual post-shuffle data size, which is why enabling AQE removes much of the historical need to hand-tune this number.

### Output files

When you write a DataFrame, Spark writes roughly **one file per partition** at write time (per output directory, if table-partitioned). So the number of in-memory partitions at the moment of `write` controls how many output files you get. 200 partitions writing a small result yields 200 tiny files — the small-file problem (lessons 9, 13). This is why you often `coalesce` or `repartition` *before* writing, to control output file count and size.

## Key concepts

- **Partition count ≈ task count ≈ parallelism** (per stage, capped by cores).
- **Cores/slots** bound how many partitions process at once; extra partitions run in waves.
- **Over-partitioning** = overhead and tiny files; **under-partitioning** = idle cores, huge tasks, spills.
- **`repartition`** = full shuffle to a target count or by column (balanced, expensive).
- **`coalesce`** = merge partitions without a full shuffle (cheap, can be uneven).
- **`partitionBy`** = storage-layout partitioning of a written table by column.
- **Partition pruning** = skipping table partitions/files that can't match a filter.

## Detailed technical sections

### Relationship between partitions, tasks, and cores

The chain from lesson 04, made quantitative: a stage runs **one task per partition**. Tasks execute on **slots**, where total slots = (executors) × (cores per executor). If a stage has 200 partitions and you have 40 cores, the 200 tasks run in 5 waves of 40. If a stage has 20 partitions and 40 cores, only 20 cores work and 20 sit idle — wasted capacity (under-partitioning). The practical guideline is to have **at least as many partitions as cores, and usually a small multiple** (e.g., 2–4×), so all cores stay busy and waves are balanced even if some tasks finish early. This is the core arithmetic of Spark parallelism.

### Over-partitioning

Too many partitions means too many tasks, each processing very little data. The costs: **scheduling overhead** (the driver must launch and track each task; per-task fixed costs add up when tasks are tiny), poor amortization of setup (opening files, initializing operators per task), and — at write time — **too many tiny output files**, which hurts every future reader (lesson 13) and bloats metadata. A symptom in the UI: thousands of tasks each running in milliseconds with total overhead dominating useful work. Over-partitioning often comes from a too-high shuffle-partition setting on small data, or from reading a sea of small files.

### Under-partitioning

Too few partitions means a few large tasks that don't fill your cores and may not fit in memory. The costs: **idle cores** (parallelism wasted), **long task durations**, and **spills or OOM** when a single partition's data exceeds execution memory (lesson 08). A symptom: a stage with far fewer tasks than you have cores, each running a long time, possibly with spill metrics. Under-partitioning often comes from reading a few huge files, or from `coalesce`-ing too aggressively before a heavy computation.

### `repartition()`

`repartition(n)` performs a **full shuffle** to produce exactly `n` evenly sized partitions; `repartition(col)` (or `repartition(n, col)`) shuffles data so rows with the same `col` value are co-located (hash-partitioned by `col`). Because it's a full shuffle, it is **expensive**, but it produces **balanced** partitions and can set up data for a subsequent operation (e.g., repartition by the join key before a join, or repartition to increase parallelism before a heavy stage). Use `repartition` when you need to *increase* partition count, *rebalance* skewed partitions, or *co-locate by a key*. It is the right tool when balance matters more than the shuffle cost.

### `coalesce()`

`coalesce(n)` **reduces** the partition count to `n` by *merging* existing partitions **without a full shuffle** — it combines adjacent partitions on the same executor where possible, avoiding network movement. It is therefore **cheap** but can produce **uneven** partitions (merging doesn't rebalance) and only goes *down* in count (asking `coalesce` to increase count won't add a shuffle and won't increase parallelism). The classic use is **before writing**: after a heavy computation that left you with 200 partitions but only a little data, `coalesce(8)` merges them into 8 reasonably sized output files cheaply. Caveat: `coalesce` can also reduce the parallelism of the *upstream* computation (because it removes the shuffle boundary that would have let the upstream run at higher partition count), so for that case `repartition` may be better despite its cost.

```text
repartition(n): full shuffle → n BALANCED partitions (expensive, can go up or down)
coalesce(n):    merge only   → n partitions, possibly UNEVEN (cheap, down only)
```

### `partitionBy()` on write — table/file partitioning

`df.write.partitionBy("year", "month")...` writes the table's files into a **directory structure** by the partition column values:

```text
table/
  year=2025/month=01/part-*.parquet
  year=2025/month=02/part-*.parquet
  year=2026/month=01/part-*.parquet
```

This is **storage-layout partitioning**, entirely distinct from Spark's in-memory compute partitioning. Its purpose is **partition pruning** on read: a query filtering `year = 2026 AND month = 1` can skip every directory that doesn't match, reading only the relevant files — a massive I/O saving on large tables. But it is a double-edged tool (see high-cardinality danger below).

### Spark partitioning vs table/file partitioning

This distinction deserves its own emphasis. **Spark partitioning** is how a DataFrame is split for parallel *computation* in memory right now; it changes constantly as transformations and shuffles occur, and it governs task parallelism. **Table/file partitioning** is the persistent *directory layout* of stored data; it governs how much data future reads must scan (via pruning). You can have a DataFrame with 200 in-memory partitions that you write to a table partitioned by `year` into 3 directories — the two numbers are unrelated. Confusing them leads to mistakes like assuming `partitionBy("id")` improves compute parallelism (it doesn't; it creates a storage layout) or assuming `repartition` changes the stored directory structure (it doesn't).

### Hash partitioning and range partitioning

These are the two ways Spark redistributes data during a shuffle (lesson 09). **Hash partitioning** sends a row to `hash(key) % n`, spreading distinct keys roughly evenly and co-locating equal keys — used for `groupBy`, joins, and `repartition(col)`. **Range partitioning** sorts data into globally ordered ranges by sampling boundaries — used for `orderBy`/global sort, so partition *i* holds a contiguous key range. Hash is balanced when keys are well-distributed but vulnerable to skew (a hot key overwhelms its partition, lesson 11); range produces ordered partitions but needs a sampling pass.

### File sizes and the small-file problem

Storage performance is dominated by file count and size. **Too many small files** (e.g., thousands of few-KB files) hurt Spark on *read* (each file is metadata overhead and may become a tiny input partition → many tiny tasks) and bloat the table's metadata and listing time. **Too few huge files** hurt *parallelism* (few input partitions → idle cores) and can cause memory pressure. The remedy on write is to control output partition count (`coalesce`/`repartition`) to target reasonable file sizes (commonly ~128 MB–1 GB), and on existing tables, **compaction** (lesson 14, Delta `OPTIMIZE`) to merge small files. The small-file problem is one of the most common real-world lakehouse performance issues, and it originates in partition/output-file count.

### How partitioning affects reads and writes

On **read**, input partitioning (file count/size) sets first-stage parallelism, and table partitioning enables pruning. On **write**, in-memory partition count sets the number of output files, and `partitionBy` sets the directory layout. So partitioning decisions ripple: the number of partitions you write today becomes the number of files (and thus the read parallelism and pruning behavior) for every consumer tomorrow. Designing output partitioning is designing the read performance of downstream jobs.

### How partition pruning works

When a table is partitioned by a column on disk and a query filters on that column, Catalyst applies **partition pruning** (a form of predicate pushdown specialized to the directory layout): it consults the partition values encoded in the directory structure and **only reads directories that can match the filter**, skipping the rest before reading any data files. In the explain plan you see `PartitionFilters: [year = 2026]` on the scan, and the number of files read drops accordingly. Pruning is the entire payoff of table partitioning, and it only works when queries filter on the partition column(s).

### Why high-cardinality partition columns are dangerous

`partitionBy` on a high-cardinality column (e.g., `user_id`, `event_id`, a timestamp to the second) creates an enormous number of directories, each containing very little data — the small-file problem at industrial scale, plus massive metadata overhead (listing millions of directories), plus tiny files that destroy read performance. A table partitioned by `customer_id` with a million customers creates a million directories of a few rows each. The guideline: **partition only on low-to-moderate cardinality columns that queries actually filter on** (commonly date components like `year`/`month`, or a region/category), aiming for partitions large enough to hold reasonably sized files. For finer-grained skipping, rely on Delta's **data skipping** and **Z-ordering / clustering** (lesson 14) rather than physical partitioning. This is one of the most consequential lakehouse design decisions (lessons 15, 22).

## PySpark and SQL perspective

```python
# Increase parallelism / rebalance before a heavy stage (full shuffle, balanced)
df = df.repartition(400, "customer_id")

# Reduce output files cheaply before writing a small result (merge, no shuffle)
result.coalesce(8).write.format("delta").saveAsTable("gold_summary")

# Storage-layout partitioning for pruning — low-cardinality column only
events.write.partitionBy("year", "month").format("delta").saveAsTable("silver_events")
```

`repartition(400, "customer_id")` triggers a shuffle (`Exchange hashpartitioning(customer_id, 400)`) producing 400 balanced partitions co-located by customer — useful before a join or window on `customer_id`. `coalesce(8)` adds no shuffle; it merges the existing partitions into 8 before writing 8 files. `partitionBy("year","month")` does **not** change compute partitioning; it writes files into `year=.../month=.../` directories so that a later `WHERE year=2026 AND month=1` prunes to one directory (`PartitionFilters` in the plan). In SQL, `CREATE TABLE ... PARTITIONED BY (year, month)` expresses the same storage layout; there is no SQL equivalent of in-memory `repartition`/`coalesce` because those are compute-layout hints, reinforcing that table partitioning and compute partitioning are different layers.

## Architectural implications

- **Size partitions to your cores and memory:** enough partitions to fill all slots (a small multiple of core count), each small enough to fit in execution memory.
- **Control output file count** with `coalesce`/`repartition` before writing to avoid tiny or giant files.
- **Partition tables only on low-cardinality, frequently-filtered columns**; never on high-cardinality keys.
- **Distinguish compute partitioning from table partitioning** in every design discussion — they solve different problems.
- **Lean on AQE** to coalesce shuffle partitions automatically (lesson 19) and on Delta `OPTIMIZE` to compact small files (lesson 14).
- **Design output partitioning for the next reader**, since today's files are tomorrow's read parallelism and pruning.

## Common misconceptions

- *"More partitions is always more parallel/faster."* Only up to your core count; beyond that it's overhead and tiny files.
- *"`coalesce` and `repartition` are interchangeable."* No — `coalesce` merges cheaply (uneven, down-only); `repartition` shuffles (balanced, any direction).
- *"`partitionBy` makes my job run more in parallel."* No — it's a storage layout for pruning, not a compute-parallelism setting.
- *"Partitioning by a unique ID gives the best pruning."* No — high cardinality creates millions of tiny files and destroys performance.
- *"The 200 shuffle partitions default is fine."* Often wrong for your data size; tune it or rely on AQE.

## Summary

Partitioning converts data volume into parallel work: one task per partition, tasks run on cores, so parallelism is bounded by partition count and core count. Too few partitions waste cores and risk spills; too many add overhead and create tiny files. Spark distinguishes input partitions (from file sizes), shuffle partitions (default 200, often AQE-coalesced), and output files (one per partition at write), all of which are *compute* partitioning — distinct from *table/file partitioning*, the on-disk directory layout set by `partitionBy` that enables partition pruning. Use `repartition` for balanced redistribution (full shuffle) and `coalesce` to cheaply reduce file count; partition tables only on low-cardinality, filtered columns to avoid the small-file catastrophe of high-cardinality partitioning. Right-sizing partitions and files is among the most impactful and most commonly mishandled aspects of real pipelines.

## Terms to remember

- **Partition** — slice of distributed data processed by one task; unit of parallelism.
- **Input / shuffle / output partitions** — read-time / post-shuffle / write-time partitioning.
- **`repartition`** — full-shuffle redistribution to a balanced target count or by column.
- **`coalesce`** — cheap merge to fewer partitions, possibly uneven.
- **`partitionBy`** — on-disk directory partitioning of a written table.
- **Partition pruning** — skipping non-matching table partitions/files on read.
- **Small-file problem** — too many tiny files degrading read parallelism and metadata.
