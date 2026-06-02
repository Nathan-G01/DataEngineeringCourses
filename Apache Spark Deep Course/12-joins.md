# Joins

## Purpose of this lesson

Joins are where Spark performance is most often won or lost, because a join is usually the most expensive operation in a pipeline and because the *same logical join* can execute as wildly different physical strategies with wildly different costs. This lesson separates the **logical join** (what result you want — inner, left, semi, etc.) from the **physical join strategy** (how Spark executes it — broadcast hash, sort-merge, shuffle hash, nested loop), explains how Spark chooses, what statistics and hints do, how AQE can change the choice at runtime, and how to reason about cardinality, selectivity, and null behavior *before* you write the join. It synthesizes shuffles (09), partitioning (10), and skew (11) into the operation where they all converge.

## Core mental model

```text
LOGICAL join  = the result semantics  (inner / left / right / full / semi / anti)
        ↓  Catalyst chooses a strategy based on table sizes & stats
PHYSICAL join = how it runs:
   Broadcast hash join : small side copied to every executor — NO shuffle of big side  (cheapest)
   Sort-merge join     : both sides shuffled by key + sorted, then merged             (default for big+big)
   Shuffle hash join   : both sides shuffled; build a hash table on one side
   (Broadcast) nested loop / Cartesian : no/weak join key — every row vs every row     (avoid)
```

The logical join says *what*; the physical strategy decides *how much it costs*. Optimizing joins is mostly about steering toward the cheap strategy.

## Main explanation

### Logical joins vs physical join strategies

A **logical join** specifies the *semantics* of combining two datasets on a condition: which rows pair up and which non-matching rows are kept. This is the SQL-level concept (`INNER JOIN`, `LEFT JOIN`, etc.) and is purely about the result. A **physical join strategy** is the *algorithm* Spark uses to compute that result across a cluster. One logical inner join can be executed as a broadcast hash join (fast) or a sort-merge join (slower) — same result, very different mechanics and cost. Catalyst chooses the physical strategy; you influence it through table sizes, statistics, and hints. The single most important performance fact about joins is that **this choice can swing cost by orders of magnitude.**

### The logical join types

- **Inner join** — keep only rows with a match on both sides. Non-matching rows from both sides are dropped.
- **Left (outer) join** — keep all left rows; matched right columns where they match, nulls where they don't.
- **Right (outer) join** — symmetric: keep all right rows.
- **Full (outer) join** — keep all rows from both sides; nulls where the other side has no match.
- **Left semi join** — keep left rows that *have* a match, but return only left columns (a filtered existence check). Does not duplicate left rows even if multiple right matches exist.
- **Left anti join** — keep left rows that have *no* match (the complement of semi); return only left columns.

Semi and anti joins are valuable for "exists / not-exists" logic and are cheaper than a full join followed by filtering, because Spark can stop at the first match (semi) and never materializes right-side columns.

## Key concepts

- **Join key** — the column(s) the join condition equates; determines how data is co-located.
- **Join cardinality** — how many rows on each side share a key (1:1, 1:many, many:many).
- **Join selectivity** — what fraction of rows survive the join; drives output size.
- **Broadcast** — copying a small table to every executor to avoid shuffling the large one.
- **Statistics** — row counts and sizes Catalyst uses to choose a strategy.
- **Hints** — explicit instructions (e.g., `BROADCAST`) overriding the optimizer's choice.

## Detailed technical sections

### Sort-merge join (the default for large–large)

A **sort-merge join (SMJ)** is Spark's default strategy when both sides are large. It works in three steps:

```text
1. SHUFFLE both sides by the join key (Exchange hashpartitioning(key)) so matching keys co-locate.
2. SORT each partition by the join key on both sides.
3. MERGE the two sorted streams within each partition, pairing matching keys.
```

SMJ requires shuffling **both** tables (expensive — two large datasets moved across the network) and sorting them, but it scales to arbitrarily large inputs because it never needs either side fully in memory. It is robust and the safe default for big-to-big joins. Its costs are exactly the shuffle costs of lesson 09, doubled (both sides), plus sorting. In the plan you see `SortMergeJoin` with two `Exchange` + `Sort` operators feeding it.

### Broadcast hash join (the cheap strategy for large–small)

A **broadcast hash join (BHJ)** applies when one side is small enough to fit in memory. Instead of shuffling both sides, Spark:

```text
1. COLLECT the small side to the driver, then BROADCAST a copy to EVERY executor.
2. Each executor builds an in-memory HASH TABLE from the small side.
3. Each executor streams its LOCAL partitions of the large side and probes the hash table — NO shuffle of the large side.
```

Because the large side is never shuffled, BHJ avoids the dominant cost entirely — this is usually the single biggest join optimization available. The trade-off: the small side must fit in executor memory (and be sent over the network once to each executor). Spark auto-broadcasts a side estimated to be below `spark.sql.autoBroadcastJoinThreshold` (commonly ~10 MB by default, often raised in practice). You can force it with a `broadcast()` hint when you know a side is small but statistics underestimate it. In the plan you see `BroadcastHashJoin` fed by a `BroadcastExchange`. This is the canonical **large fact + small dimension** pattern.

### Shuffle hash join

A **shuffle hash join (SHJ)** shuffles both sides by key (like SMJ) but then builds a hash table on one side (the smaller of the two post-shuffle partitions) and probes with the other, *without* sorting. It can be faster than SMJ when one side is much smaller than the other but still too big to broadcast, and when there's enough memory for the hash table. Historically Spark preferred SMJ for stability (no risk of OOM from a too-large hash table); SHJ is chosen in narrower circumstances. You'll see it less often than BHJ or SMJ, but it's the third member of the family.

### Broadcast nested loop join and Cartesian join

When there is **no equality join key** (e.g., a join on an inequality, or a cross join), Spark cannot hash-partition by a key. It falls back to:

- **Broadcast nested loop join** — broadcast one side and, for each row of the other, scan the broadcast side. Tolerable only if the broadcast side is tiny.
- **Cartesian product (cross join)** — every row of the left paired with every row of the right: `m × n` output rows. This is catastrophically expensive for large inputs and almost always a mistake (often an accidental missing join condition). Spark requires explicit `crossJoin` (or config) to allow it, precisely because it's so dangerous.

Seeing `CartesianProduct` or `BroadcastNestedLoopJoin` in a plan over large tables is a red flag — usually a forgotten or non-equi join condition.

### Why joins cause shuffles

A join must bring together rows that share a join key from two separately-partitioned datasets. Unless one side is broadcast, the only way to co-locate matching keys is to **shuffle both sides by the join key** (lesson 09) so that, for any key value, all its rows from both tables land in the same reduce partition where they can be paired. This is why non-broadcast joins are inherently shuffle-heavy — and why broadcast joins, which avoid shuffling the large side, are such a large win. The join *is* the reason for the shuffle.

### How broadcast joins avoid shuffling the large side

The insight: if the small side is replicated to *every* executor, then each executor already has *all* the small-side rows it could possibly need to match against whatever large-side partition happens to be local to it. No large-side row needs to move, because wherever it is, the matching small-side rows are already present (the whole small side is present everywhere). Replication of the small side substitutes for redistribution of the large side. You trade one cheap broadcast (small data, sent once per executor) for avoiding an expensive shuffle (large data, all-to-all). When the small side genuinely is small, this is almost always worth it.

### How Spark chooses a join strategy

Catalyst's decision logic, roughly:

```text
1. Is there an equi-join key?  No → nested loop / Cartesian (unavoidable, expensive).
2. Is one side's estimated size below the broadcast threshold?  Yes → BROADCAST HASH JOIN.
3. Otherwise, can a shuffle hash join's hash table fit?  Sometimes → SHUFFLE HASH JOIN.
4. Otherwise → SORT-MERGE JOIN (the robust default for big–big).
```

The pivotal input is the **estimated size** of each side, which comes from **statistics**. If statistics are missing or stale, Spark may misjudge — e.g., not broadcast a side that actually is small, defaulting to an expensive SMJ. This is a major reason for both maintaining statistics and for AQE.

### The role of statistics

Catalyst's cost-based decisions depend on **statistics**: table/row counts, column sizes, and (with the cost-based optimizer enabled) per-column stats like distinct counts. Accurate stats let Catalyst correctly identify a broadcastable side and order multi-table joins to keep intermediates small (lesson 07). Stats can come from table metadata (Delta maintains file-level statistics) or be computed (`ANALYZE TABLE ... COMPUTE STATISTICS`). When stats are wrong, planning is wrong — the classic symptom being a sort-merge join over a table that *should* have been broadcast. AQE addresses this by using *actual* runtime sizes rather than estimates.

### The role of hints

When you know something the optimizer doesn't (or its stats are stale), you can override its choice with **hints**: `broadcast(df)` in PySpark or `/*+ BROADCAST(t) */` in SQL forces a broadcast hash join; other hints (`MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`) force other strategies. Hints are an escape hatch — useful when you're certain a side is small but statistics underestimate it, or to prevent a too-large broadcast. Use them deliberately: forcing a broadcast of a side that's actually too large causes driver/executor OOM (the small side must fit in memory). Prefer fixing statistics and trusting AQE; reach for hints when those aren't enough.

### How AQE may change the join strategy at runtime

**Adaptive Query Execution** (lesson 19) can revise the join strategy *during* execution using real statistics measured after the input stages run. The headline case: a join initially planned as **sort-merge** (because pre-execution estimates thought both sides large) can be **switched to a broadcast hash join** at runtime when AQE discovers one side, after filtering, is actually small enough to broadcast. AQE also performs **skew join handling**, splitting an oversized skewed partition into sub-partitions so the join's long-tail task (lesson 11) is parallelized. This is why the *final* plan in the SQL UI can differ from the initial `explain` output, and why AQE is so valuable for joins specifically: it fixes the most common and most expensive planning mistake — a missed broadcast — using ground truth.

### Null behavior in joins

Nulls do not match nulls under standard SQL equality (`NULL = NULL` is *unknown*, not true). Consequences: in an **inner join** on a key, rows with a null key match nothing and are dropped from both sides; in a **left join**, left rows with a null key are kept but get nulls for right columns (no match). This matters for both correctness and performance: a flood of null keys can't produce inner-join matches yet still gets shuffled to the same (null-hashing) partition, creating skew that does no useful work (lesson 11) — hence the "handle nulls separately" mitigation. Always reason about what nulls in your join keys mean semantically *and* what they do to data distribution.

### Duplicate keys and cardinality explosion

If a join key is not unique on a side, matches multiply. A key with `m` rows on the left and `k` rows on the right produces `m × k` output rows for that key. When both sides have duplicates on a hot key, the output can explode far beyond either input size — and all of it lands in one partition (skew). This **cardinality explosion** is a frequent cause of jobs that suddenly produce vastly more rows than expected, blow up memory, or run forever. Before joining, you must know the **cardinality** of the key on each side: is it 1:1 (unique both sides), 1:many (a dimension joined to facts), or many:many (the dangerous case)? Many:many joins on non-unique keys should be scrutinized — often they indicate a missing additional key or a need to deduplicate first.

### Join cardinality, selectivity, and choosing wisely

- **Cardinality** (how many rows per key on each side) predicts output size and explosion risk.
- **Selectivity** (what fraction of rows find a match / survive) predicts how much data flows downstream — a highly selective join shrinks data and should be done *early*; a low-selectivity or exploding join grows data and should be deferred or restructured.
Reasoning about these *before* writing the join lets you place joins optimally in the pipeline (do shrinking joins first), choose broadcast where a side is small, and avoid surprise explosions.

## PySpark and SQL perspective

```python
# Large fact + small dimension → broadcast hash join (no shuffle of the large fact)
orders.join(F.broadcast(dim_customer), "customer_id")
```

Here `orders` is huge and `dim_customer` is small. The `broadcast` hint (or Spark's auto-broadcast if stats are good, or AQE at runtime) makes this a `BroadcastHashJoin`: `dim_customer` is sent to every executor and hashed; each executor probes its local `orders` partitions. No `Exchange` on `orders` — the most expensive shuffle is eliminated. Contrast:

```python
# Large fact + large fact → sort-merge join (both sides shuffled and sorted)
orders.join(line_items, "order_id")
```

With two large tables and no broadcastable side, this is a `SortMergeJoin`: two `Exchange hashpartitioning(order_id)` operators (shuffling both sides), two `Sort` operators, then the merge — three stages minimum. If `order_id` is duplicated on `line_items` (one order, many line items — a 1:many), output is larger than `orders` but bounded; if it were many:many you'd risk explosion. In SQL the equivalents (`SELECT ... FROM orders JOIN dim_customer USING(customer_id)` with `/*+ BROADCAST(dim_customer) */`) compile to the same physical joins — the strategy is decided by Catalyst/AQE from sizes and stats, not by whether you wrote PySpark or SQL.

A skew-aware example tying to lesson 11: if `orders` is skewed on a hot `customer_id` and `dim_customer` is small, broadcasting `dim_customer` not only avoids the big shuffle but also **eliminates the skew** (no shuffle by `customer_id` means no oversized reduce partition). The same broadcast solves both the shuffle cost and the skew at once — a recurring, high-value pattern.

## Architectural implications

- **Identify large+small joins and broadcast the small side** — usually the biggest single win.
- **Know each join key's cardinality and selectivity before writing the join** — to predict output size, explosion, and placement.
- **Place selective (shrinking) joins early; defer or restructure exploding joins.**
- **Maintain statistics and enable AQE** so Catalyst/runtime picks broadcast when it should.
- **Handle null/sentinel keys explicitly** to avoid useless skew (lesson 11) and correctness surprises.
- **Treat `CartesianProduct`/`BroadcastNestedLoopJoin` over large data as a bug** — usually a missing equi-condition.

## Common misconceptions

- *"A join is a join; the type only affects the result."* No — the *physical strategy* (broadcast vs sort-merge) dominates cost.
- *"Spark always picks the best strategy."* Only with good statistics; stale stats cause missed broadcasts — AQE helps fix this at runtime.
- *"Broadcast is always better."* Only when the small side fits in memory; broadcasting a too-large side causes OOM.
- *"Joining can't produce more rows than the inputs."* Duplicate keys cause cardinality explosion (`m × k`).
- *"Null keys just don't match, no big deal."* They can create useless skew and silently drop/retain rows depending on join type.

## Summary

A logical join (inner/left/right/full/semi/anti) defines result semantics; the physical strategy (broadcast hash, sort-merge, shuffle hash, nested-loop/Cartesian) defines cost. Sort-merge is the robust default for large–large joins but shuffles and sorts both sides; broadcast hash join avoids shuffling the large side by replicating a small side to every executor — usually the biggest optimization, and one that also defeats skew on the join key. Spark chooses based on estimated sizes from statistics; stale stats cause missed broadcasts, which AQE corrects at runtime (and AQE also splits skewed join partitions). Before writing any join, reason about the key's cardinality (explosion risk), selectivity (placement), null behavior (correctness and skew), and which side is small enough to broadcast. Joins are where shuffles, partitioning, and skew converge, so they are where careful architectural thinking pays off most.

## Terms to remember

- **Broadcast hash join** — replicate small side to all executors; no shuffle of the large side.
- **Sort-merge join** — shuffle + sort both sides, then merge; default for large–large.
- **Shuffle hash join** — shuffle both sides, build a hash table on the smaller.
- **Cartesian / nested-loop join** — no equi-key; every-row-vs-every-row; avoid on large data.
- **Cardinality explosion** — `m × k` output from duplicate keys on both sides.
- **Selectivity** — fraction of rows surviving the join; drives placement.
- **Broadcast hint** — forcing a broadcast when statistics underestimate a small side.
