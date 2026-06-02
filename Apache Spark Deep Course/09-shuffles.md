# Shuffles

## Purpose of this lesson

The shuffle is the single most important performance concept in Spark. It is the mechanism by which data moves across the cluster, and it is the dominant cost in most non-trivial jobs. Almost every "why is my Spark slow?" question is, at bottom, a shuffle question. This lesson explains exactly what a shuffle is, why it exists, why it is expensive (network, disk, serialization), how the map and reduce sides work, which operations cause shuffles, how shuffle partitions are configured, and how to recognize shuffles in explain plans and the Spark UI. It is the hinge between the execution model (lesson 04) and the lessons on partitioning, skew, and joins that follow.

## Core mental model

```text
Before shuffle: data is partitioned ARBITRARILY (by how it was read/produced)
                P0: [CH, US, CH]   P1: [US, FR, CH]   P2: [FR, US, FR]

A shuffle REDISTRIBUTES data by a key so all matching keys land together:
                ┌─ all CH ─┐ ┌─ all US ─┐ ┌─ all FR ─┐
                  P0'         P1'          P2'

Cost: every executor may send data to every other executor (network),
      writing/reading intermediate files (disk), serializing rows (CPU).
```

A shuffle is an **all-to-all redistribution of data by key**, and it is expensive precisely because it is all-to-all.

## Main explanation

### What a shuffle is

A **shuffle** is the process of redistributing data across partitions (and therefore across executors and machines) so that rows sharing some property — usually a key value — end up in the same partition. It is Spark's implementation of the "wide dependency" from lesson 04: an operation whose output partitions depend on many input partitions, requiring data to physically move between executors. The shuffle is the boundary between two stages: the **map side** (the stage before the shuffle, which produces and writes the redistributed data) and the **reduce side** (the stage after, which reads its assigned share).

### Why shuffles exist

Many operations are *fundamentally* impossible without bringing related data together. To count rows per `country`, every row with `country = 'CH'` must be processed together — but those rows start scattered across all input partitions (because the input was partitioned by file layout, not by country). To join two tables on `customer_id`, rows with the same `customer_id` from both tables must meet. To sort globally, data must be arranged across partitions in order. To deduplicate, all copies of a value must reach the same place to be compared. In every case, the data's *current* partitioning doesn't match what the operation *needs*, so Spark must redistribute it. The shuffle exists because **correct results for key-based operations require co-locating data by key**, and the input rarely arrives that way.

### Why shuffles are expensive

A shuffle is expensive because it combines three of the slowest things a distributed system does:

- **Network I/O.** In the worst case, every map-side task sends data to every reduce-side task — an all-to-all transfer across the cluster network, which is orders of magnitude slower than reading local memory. The volume moved can be a large fraction of the dataset.
- **Disk I/O.** Map-side tasks write their output to **local disk** (shuffle files) before it is fetched; reduce-side tasks read those files over the network. This materialization to disk (rather than keeping everything in memory) is what makes shuffles durable and recoverable, but it adds substantial disk read/write.
- **Serialization.** To send rows over the network and write them to disk, Spark must **serialize** them to bytes (and deserialize on the other side). For large volumes this is significant CPU cost on top of the I/O.

There is also **synchronization cost**: the reduce side cannot start until the map side's shuffle output is ready, so a shuffle is a barrier that serializes the job's stages (lesson 04). Together, these make the shuffle the most expensive routine operation in Spark — which is why reducing shuffles, and reducing the data each shuffle moves, is the highest-leverage optimization.

## Key concepts

- **Shuffle** — all-to-all redistribution of data by key across executors.
- **Map side / reduce side** — the stage that writes shuffle output / the stage that reads it.
- **Shuffle write / shuffle read** — bytes written to local disk by the map side / fetched by the reduce side.
- **Exchange** — the physical operator in the plan representing a shuffle.
- **Shuffle partitions** — the number of partitions the data is redistributed into (configurable).
- **Wide vs narrow** — wide transformations shuffle; narrow ones don't (lesson 04).

## Detailed technical sections

### The mechanics: shuffle write and shuffle read

A shuffle proceeds in two phases across the stage boundary:

```text
MAP SIDE (stage N, one task per input partition):
  for each row: compute the partition it belongs to = hash(key) % numShufflePartitions
  group rows by target partition, (often) pre-aggregate/combine,
  WRITE the grouped output to local disk as shuffle files, indexed by target partition.
        ↓  (shuffle files on map-side executors' local disks)
REDUCE SIDE (stage N+1, one task per shuffle partition):
  each reduce task FETCHES, over the network, its assigned partition's bytes
  from EVERY map-side task's shuffle files,
  then merges/sorts/aggregates/joins the gathered rows.
```

**Shuffle write** is the map side serializing and writing its partitioned output to local disk. **Shuffle read** (or fetch) is the reduce side pulling the relevant pieces from all map outputs over the network. The UI reports both as metrics (lesson 20); large shuffle read/write numbers are a direct signal of an expensive shuffle.

### How keys map to partitions

For most shuffles, Spark uses **hash partitioning**: a row's destination partition is `hash(shuffleKey) % numShufflePartitions`. This deterministically sends all rows with the same key to the same reduce partition (necessary for correctness) and spreads distinct keys roughly evenly (good for balance — *unless* the data is skewed, lesson 11). For `orderBy`/global sort, Spark uses **range partitioning** instead: it samples the data to compute range boundaries, then sends each row to the partition for its range, so partitions are globally ordered. Range partitioning requires an extra sampling job (one reason `orderBy` can launch extra jobs, lesson 04).

### Map-side combine reduces shuffle volume

For aggregations, Spark performs a **partial (map-side) aggregate** *before* the shuffle (lesson 04). Counting rows per country: each map task first counts within its own partition, emitting one partial count per country, and only those partials are shuffled — not every row. The reduce side then sums the partials. This **map-side combine** can shrink the shuffled volume enormously (from billions of rows to a few partials per key per partition). It's why aggregations are often far cheaper than their naive cost suggests, and why aggregations generally shuffle far less than joins (which usually must move the actual rows).

### Which operations cause shuffles

Any **wide** transformation triggers a shuffle. The common ones:

| Operation | Why it shuffles |
|-----------|-----------------|
| `groupBy(...).agg(...)` | Rows with the same group key must meet (after map-side combine of partials). |
| `join` (non-broadcast) | Rows with the same join key from both sides must meet (lesson 12). |
| `distinct` / `dropDuplicates` | All copies of a value must reach one place to compare. |
| `orderBy` / `sort` (global) | Global ordering needs range-partitioned redistribution. |
| `repartition(n)` / `repartition(col)` | Explicitly redistributes data into `n` partitions / by column. |
| Window functions with `PARTITION BY` | Rows of each window partition must be co-located. |
| `reduceByKey` / `groupByKey` (RDD) | Key-based redistribution. |

Operations that **do not** shuffle (narrow): `select`, `filter`/`where`, `withColumn`, `drop`, `union` (just concatenates partitions), and `coalesce` to *fewer* partitions (which merges without full redistribution — see lesson 10). Knowing this list lets you predict shuffle count from the code before running anything.

### Narrow vs wide, restated through cost

The narrow/wide distinction (lesson 04) is ultimately a *cost* distinction. Narrow transformations are cheap because each output partition is computed locally from one input partition — no network, no shuffle files, no serialization for transfer. Wide transformations are expensive because they require the all-to-all redistribution above. The entire art of Spark performance is, in large part, **arranging your computation so that as much as possible stays narrow, and the unavoidable wide operations move as little data as possible.**

### How shuffles create stage boundaries

Because the reduce side cannot begin until the map side has written all its shuffle output, the shuffle is a hard synchronization barrier — exactly a stage boundary (lesson 04). Every `Exchange` in the physical plan is one shuffle and one stage boundary. This is why "count the exchanges" tells you the number of shuffles and (plus one) the number of stages. It also means shuffles *serialize* your job: stages on either side of a shuffle cannot overlap, so a slow map-side tail directly delays the reduce side.

### How shuffle partitions are configured

The number of partitions produced by a shuffle is controlled by `spark.sql.shuffle.partitions`, which **defaults to 200**. This single setting has outsized impact:

- If it's **too high** for your data (e.g., 200 partitions for a small post-shuffle dataset), you get many tiny tasks, high scheduling overhead, and tiny output files — the "small files" problem (lessons 10, 13).
- If it's **too low** for a large dataset, partitions are huge, tasks run long, and you risk spills and OOM.

Historically you tuned this by hand per job. **Adaptive Query Execution (AQE, lesson 19)** now dynamically *coalesces* shuffle partitions at runtime based on actual data size, largely automating this — which is why AQE matters so much for shuffle-heavy lakehouse workloads. Still, understanding the setting explains a lot of partition-count behavior you'll see in the UI.

### Why reducing shuffles is often the most important optimization

Because the shuffle is usually the dominant cost, the highest-leverage optimizations are shuffle-related: eliminating unnecessary shuffles (e.g., turning a shuffle join into a **broadcast join** so the big side isn't redistributed, lesson 12), reducing shuffled volume (filtering and pruning *before* the shuffle so less data moves; relying on map-side combine for aggregations), avoiding repeated shuffles (not re-shuffling data that's already partitioned the way you need), and fixing skew so the shuffle's reduce side is balanced (lesson 11). Tuning per-row CPU (Tungsten already does most of this) or adding executors rarely helps as much as cutting a shuffle.

### Why shuffles are not inherently wrong

It is important not to over-correct into "shuffles are bad, avoid them all." Shuffles are *necessary* for correct key-based operations — you cannot group, join, dedupe, or globally sort without one (unless a special case like a broadcast join applies). A job with shuffles is normal and often unavoidable. The goal is not zero shuffles; it is **no unnecessary shuffles, and minimal data through the necessary ones.** A correct join that shuffles is fine; shuffling the same data three times because you didn't reuse a partitioning, or shuffling a billion rows you could have filtered to a million first, is the problem.

### How to recognize shuffles in explain plans and the Spark UI

- **Explain plan:** each `Exchange` node is a shuffle. `Exchange hashpartitioning(country, 200)` is a hash shuffle into 200 partitions on `country`; `Exchange rangepartitioning(...)` is a sort's range shuffle; `Exchange SinglePartition` collapses everything to one partition (e.g., a global aggregate or a sort feeding a single output). Count the exchanges to count the shuffles.
- **Spark UI (lesson 20):** the **SQL/DataFrame tab** shows the plan graph with `Exchange` nodes annotated with shuffle read/write bytes. The **Stages tab** shows, per stage, **Shuffle Write** (bytes the stage wrote for downstream) and **Shuffle Read** (bytes it fetched) — large values mark expensive shuffles. The boundary between two stages in the DAG visualization *is* a shuffle. Long shuffle-read times, or a few reduce tasks reading far more than others, point to volume or skew problems.

## PySpark and SQL perspective

```python
# Narrow — NO shuffle: each output partition from one input partition
df.filter("country = 'CH'").select("study_id")
```

```python
# Wide — ONE shuffle (Exchange hashpartitioning(country)):
df.groupBy("country").count()
# Map side: scan + filter (if any) + PARTIAL count per partition; write shuffle files.
# Reduce side (200 partitions by default, or AQE-coalesced): fetch partials, SUM per country.
```

```python
# Two shuffles → three stages: a join (shuffle on the key) then a groupBy (shuffle on country)
(orders.join(customers, "customer_id")     # Exchange on customer_id (both sides) — unless broadcast
       .groupBy("country").count())          # Exchange on country
```

In the last example, reading the plan you'd expect two `Exchange` nodes and three stages — *unless* `customers` is small enough that Catalyst/AQE chooses a **broadcast hash join**, replacing the first shuffle with a broadcast of `customers` to every executor (no redistribution of the large `orders` side; lesson 12). That single change can remove the most expensive shuffle in the job. The SQL equivalents (`GROUP BY`, `JOIN ... ON`) produce the same exchanges, because the plan, not the syntax, decides.

## Architectural implications

- **Filter and prune before wide operations** so shuffles move less data.
- **Prefer broadcast joins** for large-fact-to-small-dimension joins to avoid shuffling the large side (lesson 12).
- **Avoid redundant shuffles** — reuse an existing partitioning; don't `repartition` then immediately `groupBy` on a different key needlessly.
- **Tune/trust shuffle partition count** — let AQE coalesce, or set `spark.sql.shuffle.partitions` to match data size (lessons 10, 19).
- **Treat each `Exchange` as a budget item** — when reading a plan, justify every shuffle.

## Common misconceptions

- *"Shuffles should always be avoided."* No — they're required for correct key-based ops; avoid *unnecessary* ones and minimize volume.
- *"groupBy shuffles every row."* Usually not — map-side combine shuffles partial aggregates, far less data.
- *"More shuffle partitions is always safer."* No — too many means tiny tasks and small files; match to data size (or use AQE).
- *"A shuffle happens in memory."* The map side writes shuffle files to *local disk*; the reduce side fetches over the network.
- *"Adding executors fixes a slow shuffle."* Only if the shuffle is balanced and parallelism-bound; volume and skew aren't fixed by more executors.

## Summary

A shuffle is the all-to-all redistribution of data by key that Spark performs whenever an operation needs related rows co-located — grouping, joining, deduplicating, global sorting, repartitioning, windowing. It is expensive because it combines network transfer, local-disk materialization of shuffle files, and serialization, and because it is a synchronization barrier (a stage boundary). The map side writes shuffle output partitioned by `hash(key) % numShufflePartitions` (with map-side combine for aggregations); the reduce side fetches and merges. Shuffle partition count (default 200) strongly affects task granularity and is increasingly managed by AQE. Shuffles are necessary, not evil; the goal is to eliminate unnecessary ones and minimize the data moved through the rest. Recognize them as `Exchange` nodes in the plan and as shuffle read/write metrics and stage boundaries in the UI.

## Terms to remember

- **Shuffle** — all-to-all redistribution of data by key across the cluster.
- **Map side / reduce side** — pre-shuffle producer stage / post-shuffle consumer stage.
- **Shuffle write / read** — bytes written to local disk / fetched over the network.
- **Exchange** — physical operator denoting a shuffle (and a stage boundary).
- **Hash / range partitioning** — key-modulo redistribution / sampled-range redistribution (for sorts).
- **`spark.sql.shuffle.partitions`** — number of post-shuffle partitions (default 200; often AQE-coalesced).
- **Map-side combine** — partial aggregation before the shuffle to cut volume.
