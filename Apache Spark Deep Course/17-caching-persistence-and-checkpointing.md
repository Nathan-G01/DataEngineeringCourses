# Caching, Persistence, and Checkpointing

## Purpose of this lesson

Because Spark is lazy and recomputes plans on each action (lesson 05), reusing an expensive intermediate result naively re-runs the whole pipeline. **Caching/persistence** and **checkpointing** are the mechanisms to *materialize* a result so it isn't recomputed — but each has different mechanics, costs, and correct use cases, and applying them carelessly often makes things *slower*. This lesson explains why Spark recomputes, how `cache`/`persist` work (and their storage levels, memory pressure, and spill behavior), when caching helps and when it hurts, what checkpointing does and how it differs from caching (lineage truncation), streaming checkpoints, and the architectural alternative of materializing intermediates to Delta. The goal is a clear decision framework, because "cache everything" is a classic anti-pattern (lesson 22).

## Core mental model

```text
Default: a DataFrame is a PLAN. Each action re-runs it from sources.

cache()/persist(): materialize and KEEP the result (memory/disk) so later actions REUSE it.
   → still relies on lineage for fault recovery; does NOT truncate the plan.

checkpoint(): materialize to RELIABLE storage AND TRUNCATE lineage.
   → the plan now starts from the checkpoint, not the original sources.

Delta intermediate write: persist a layer's result durably as a table (lesson 14/15).
   → the strongest form of materialization; reusable, replayable, survives the session.
```

Materialization trades **storage/memory** for **avoided recomputation**. The art is paying that trade only when reuse makes it worthwhile.

## Main explanation

### Why Spark recomputes

A DataFrame holds a *plan*, not data (lessons 05, 06). When an action fires, Spark executes the plan from its recoverable sources; when *another* action fires on the same (or a derived) DataFrame, Spark executes the plan *again* — it does not remember the previous result. This is by design: laziness plus lineage gives optimization and fault tolerance, but the cost is recomputation on reuse. For a cheap plan this is fine; for an expensive one (big scan, heavy join, multiple downstream branches), recomputing it several times is wasteful. Caching exists to break this recomputation for *reused* results.

### Cache and persist

`cache()` and `persist()` mark a DataFrame so that, once computed, its result is **kept** in executor memory (and optionally disk) and **reused** by subsequent actions instead of being recomputed. `cache()` is shorthand for `persist()` with the default storage level (memory, spilling to disk); `persist(level)` lets you choose the storage level explicitly. Both are **lazy markers**: calling them does *not* materialize anything — the data is computed and stored on the **first action** that forces the DataFrame. So the canonical pattern is "`cache()` then trigger one action to populate, then reuse cheaply":

```python
df = expensive_pipeline()      # plan only
df.cache()                     # marks for caching — nothing stored yet
df.count()                     # ACTION: runs the pipeline AND fills the cache
df.show(); df.write(...)       # reuse from cache — no re-run
```

### Storage levels

`persist` supports several **storage levels** controlling *where* and *how* cached data is held:

- **MEMORY_ONLY** — keep deserialized objects in memory; if it doesn't fit, the excess partitions are **not** cached (recomputed when needed). Fastest reads, highest memory use, risk of partial caching.
- **MEMORY_AND_DISK** (the `cache()` default for DataFrames) — keep in memory; **spill** partitions that don't fit to local disk. No recomputation, but disk-resident partitions are slower to read.
- **MEMORY_ONLY_SER / MEMORY_AND_DISK_SER** — store serialized (compact) form, saving memory at the cost of CPU to deserialize on read. (DataFrames already use the compact Tungsten format, so the distinction matters more for RDDs.)
- **DISK_ONLY** — store only on disk; lowest memory use, slowest reads.
- **`_2` variants** (e.g., MEMORY_AND_DISK_2) — replicate cached partitions on two nodes for resilience, at double the storage.

The choice trades memory, CPU, disk, and resilience. For most DataFrame caching, the default (memory with disk spill) is reasonable; tune toward disk/serialized levels under memory pressure.

### Memory pressure, disk spill, and eviction

Cached data competes for the same executor memory as execution (operator state, shuffle buffers — lesson 08). Caching a large DataFrame **reduces memory available for computation**, which can *cause spills and slowdowns in other operations* — a frequently overlooked cost. If cached data exceeds available storage memory, Spark either **spills** it to disk (MEMORY_AND_DISK), making reads slower, or **evicts** partitions (MEMORY_ONLY), causing recomputation when they're next needed — partly defeating the purpose. So caching is not free memory you're not using; it's memory you're *taking from execution*. This is exactly why "cache everything" backfires: it inflates memory pressure, triggers spills/evictions, and can make a pipeline slower than if nothing were cached.

## Key concepts

- **Cache / persist** — mark a result to be kept and reused; lazy until first action.
- **Storage level** — where/how cached data lives (memory/disk, serialized, replicated).
- **Materialization** — actually computing and storing a result (cache fill, checkpoint, or Delta write).
- **Checkpointing** — materialize to reliable storage *and* truncate lineage.
- **Lineage truncation** — replacing a long plan with a short "read from checkpoint."
- **Unpersist** — explicitly release cached data to free memory.

## Detailed technical sections

### When caching helps

Caching pays off when a computed result is **reused multiple times** and is **expensive to compute**:

- **Multiple actions on the same DataFrame** — e.g., `count()` for validation, then `write()`, then a `display()` for inspection. Without caching, each re-runs the pipeline.
- **A DataFrame feeding multiple downstream branches** — one cleaned intermediate consumed by several different aggregations. Cache it once; all branches reuse it.
- **Iterative algorithms** — the same base reused across iterations (more common in ML, lesson-adjacent).

The rule: cache when the *number of reuses × recomputation cost* exceeds the *memory cost + cache-fill cost*. If a DataFrame is used **once**, caching only adds overhead (you store something you immediately discard).

### When caching hurts

- **Caching a DataFrame used only once** — pure overhead (cache-fill + memory taken) with no reuse benefit.
- **Caching very large data** that doesn't fit — causes spills/evictions, taking memory from execution and possibly recomputing anyway.
- **Caching cheap-to-recompute data** — if recomputation is cheaper than the memory pressure caching causes, don't.
- **Forgetting to unpersist** — stale cached data lingers, occupying memory for the rest of the session and degrading everything else.
- **Caching when the optimizer would have pruned the work anyway** — caching a pre-filter/pre-prune state can *prevent* pushdown optimizations from applying downstream, because the cached result is fixed.

A subtle one: caching can **defeat Catalyst optimizations**. If you cache a DataFrame and then apply a filter, Spark must read the *whole* cached result and filter it, rather than pushing the filter into the original scan (the cache is already materialized at a fixed shape). So caching too early — before filters/pruning — can lock in more data than necessary.

### Why `cache()` needs an action

Because `cache()`/`persist()` are **lazy markers**, not computations. They register the *intent* to cache, but the data is only computed and stored when an action forces the DataFrame's materialization. This means: if you `cache()` and never trigger an action before the reuse, the "reuse" itself triggers the computation (so the *first* use still pays full cost, and only *subsequent* uses are cheap). The common idiom is to deliberately trigger a lightweight action (`df.count()`) right after `cache()` to populate the cache, so all real uses afterward are fast. Forgetting this leads to confusion ("I cached it but it's still slow") — the first action after caching always pays.

### Unpersisting

`unpersist()` explicitly releases a cached DataFrame's storage, freeing executor memory. Spark will eventually evict caches under memory pressure (LRU), but **explicit `unpersist()` when you're done with a cached result is good hygiene**, especially in long notebooks/jobs where many cached DataFrames would otherwise accumulate and starve execution memory. The discipline: cache deliberately, use the cached result, then unpersist when its reuse window is over.

### Checkpointing

**Checkpointing** materializes a DataFrame/RDD to **reliable storage** (e.g., object storage) and — crucially — **truncates its lineage**: after checkpointing, the DataFrame's plan becomes "read from the checkpoint file," discarding the long chain of transformations that produced it. There are two flavors: *reliable* checkpoint (to durable storage, survives executor loss) and *local* checkpoint (to executor disk, faster but not fault-tolerant). Checkpointing requires an action to execute, and like caching, it computes and writes the result.

### Difference between cache and checkpoint

They look similar (both materialize) but differ in a key way:

| | `cache()` / `persist()` | `checkpoint()` |
|---|---|---|
| Where stored | Executor memory/disk (ephemeral) | Reliable storage (durable) |
| Lineage | **Preserved** (used for fault recovery) | **Truncated** (plan starts from checkpoint) |
| Fault recovery | Recompute lost cached partitions from lineage | Re-read from durable checkpoint (no recompute) |
| Survives executor loss | Not reliably (must recompute) | Yes (durable) |
| Main purpose | Speed up *reuse* | *Truncate lineage* + durable materialization |

So **cache** is about *avoiding recomputation for reused results*, keeping lineage intact for recovery. **Checkpoint** is about *cutting an unboundedly-growing or fragile lineage* and persisting durably. They solve different problems and can even be combined.

### Lineage truncation and why it matters

Lineage is the recorded transformation chain (lesson 05). Usually it's beneficial (optimization + fault recovery). But lineage can become a **problem** when it grows very long — notably in **iterative** computations (each iteration appends to the plan, so after many iterations the lineage is enormous) — causing slow planning, deep recovery recomputation, and even stack-overflow-style issues. **Checkpointing truncates** this: it materializes the current result durably and resets the lineage to "read the checkpoint," so the plan no longer carries the entire history. This bounds planning cost and makes recovery cheap (re-read vs recompute a giant chain). Lineage truncation is the *distinctive* capability of checkpointing that caching lacks.

### Streaming checkpoints

In **Structured Streaming** (lesson 16), checkpointing is mandatory and serves a related but distinct role: it durably records the query's **progress (offsets)** and **state**, enabling fault-tolerant restart and exactly-once semantics. A streaming query without a checkpoint location cannot recover its position or state after a failure/restart. This is conceptually the same idea (durable materialization of essential state to survive failure) applied to the never-ending streaming job rather than to a batch DataFrame's lineage. Streaming checkpoints are not optional hygiene; they're a correctness requirement for any production stream.

### Intermediate writes to Delta as architectural materialization

The most robust form of materialization in a pipeline is often **writing an intermediate result to a Delta table** (lessons 14, 15) rather than caching it. A Silver table *is* a materialization of cleaned data; a Gold table *is* a materialization of business aggregates. Compared to cache/checkpoint, a Delta write is **durable** (survives the session and cluster teardown), **reusable across jobs and engines** (not just within one application's cache), **replayable** (lesson 15), and **queryable independently**. It also naturally **truncates lineage** for downstream jobs (they read the table, not the upstream plan). The trade-off is the write/read I/O cost and that it's a persistent object to manage. For *cross-job* or *long-pipeline* materialization, a Delta intermediate is usually better than in-memory caching; for *within-job, short-lived* reuse, caching is lighter.

### When to persist intermediate results in a pipeline

A practical hierarchy of materialization choices:

```text
Reuse within one job, short-lived, fits memory          → cache()/persist()
Long/iterative lineage that needs truncating, within job → checkpoint()
Reuse across jobs, durable, replayable, queryable        → write to a Delta table (Silver/Gold)
Streaming progress/state                                  → streaming checkpoint (mandatory)
```

The deciding questions: *How many times is this reused? Across one job or many? Does the lineage need truncating? How expensive is recomputation vs the storage/memory cost?* (lesson 22 folds these into the broader design checklist).

## Decision guidelines

- **Cache only if reused** (≥2 expensive uses) and the result **fits comfortably** without starving execution memory. Trigger one action to populate; **unpersist** when done.
- **Cache after filters/pruning**, not before — caching early can lock in more data and block pushdown.
- **Don't cache single-use or cheap-to-recompute data** — it's net overhead.
- **Checkpoint to truncate long/iterative lineage** or for durable fault tolerance within a job.
- **Prefer Delta intermediate writes** for cross-job reuse, durability, replayability, and lineage truncation across a long medallion pipeline.
- **Always checkpoint streaming queries** — it's a correctness requirement, not an optimization.
- **Measure** (Spark UI Storage tab, lesson 20) — verify caches are actually used and not evicting/spilling.

## PySpark and SQL perspective

```python
clean = (spark.table("bronze_events")
            .filter("event_id IS NOT NULL")
            .join(F.broadcast(spark.table("dim_study")), "study_id"))   # expensive

clean.cache()            # mark — lazy
clean.count()            # populate cache (one action runs the join once)
# Now several reuses are cheap (no re-join):
a = clean.groupBy("study_id").count()
b = clean.groupBy("country").count()
a.write.format("delta").saveAsTable("gold_by_study")
b.write.format("delta").saveAsTable("gold_by_country")
clean.unpersist()        # release memory when done
```

Without `cache()`, building both `a` and `b` (and writing each) would re-run the expensive join multiple times — each action re-executes `clean`'s plan from `bronze_events`. With caching, the join runs once (during `count()`), and both branches reuse the materialized result. Note the cache is placed *after* the filter and join (so it stores the smaller, cleaned result, not the raw table) and is unpersisted afterward. For a *long medallion pipeline*, the better pattern is often to **write `clean` to a Silver Delta table** instead of caching — then `gold_by_study` and `gold_by_country` read the Silver table (durable, replayable, lineage-truncated) in separate jobs. In SQL, caching is available via `CACHE TABLE clean AS SELECT ...` (which materializes immediately, eagerly) and `UNCACHE TABLE clean`; the same reuse logic and cautions apply.

## Architectural implications

- **Treat caching as taking memory from execution** — budget it; don't cache reflexively.
- **Use Delta intermediates for durable, cross-job materialization** (the medallion layers are this).
- **Reserve checkpointing for lineage truncation** (iterative/long plans) and mandatory streaming state.
- **Place caches after pruning/filtering** to avoid storing — and locking in — more data than needed.
- **Manage cache lifecycle explicitly** (populate, reuse, unpersist) in long jobs/notebooks.

## Common misconceptions

- *"`cache()` stores the data immediately."* No — it's lazy; the first action populates it.
- *"Caching is always faster."* No — it takes memory from execution; single-use or oversized caches hurt.
- *"Cache and checkpoint are the same."* No — cache keeps lineage (ephemeral); checkpoint truncates lineage (durable).
- *"More caching = better performance."* The opposite past a point — memory pressure causes spills/evictions.
- *"Cached data survives the cluster."* No — caches are application-scoped and ephemeral; only durable writes (Delta) survive.
- *"Streaming checkpoints are optional tuning."* No — they're required for recovery and exactly-once.

## Summary

Spark recomputes plans on each action, so reusing an expensive result naively re-runs the pipeline. `cache()`/`persist()` mark a result to be materialized (lazily, on the first action) and reused, with storage levels trading memory, disk, CPU, and resilience — but caching takes memory from execution, so it helps only for genuinely reused, reasonably-sized results and hurts when applied to single-use, oversized, or cheap data, or placed before pruning. `checkpoint()` additionally **truncates lineage** and persists durably, solving the distinct problem of unboundedly-growing/iterative plans and fragile recovery; streaming checkpoints are a mandatory correctness mechanism for progress and state. The strongest, most architectural materialization is **writing intermediates to Delta tables** (the medallion layers), which are durable, reusable across jobs and engines, replayable, and lineage-truncating. Choose the materialization mechanism by reuse pattern, scope, lineage length, and the recomputation-vs-storage trade-off — and never default to "cache everything."

## Terms to remember

- **Cache / persist** — keep a reused result in memory/disk; lazy until first action.
- **Storage level** — where/how cached data is held (memory/disk, serialized, replicated).
- **Materialization** — actually computing and storing a result.
- **Checkpoint** — durable materialization that truncates lineage.
- **Lineage truncation** — replacing a long plan with "read from checkpoint."
- **Unpersist** — explicitly free cached data.
- **Delta intermediate** — durable, reusable, replayable materialization as a table.
