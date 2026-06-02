# Reading the Spark UI and Observing Execution

## Purpose of this lesson

Every previous lesson in this course described what Spark does *internally*: it builds logical plans, optimizes them with Catalyst, splits them into jobs, stages, and tasks, shuffles data across the network, and executes tasks on partitions across executors. The Spark UI is where that internal machinery becomes *visible*. It is the single most important diagnostic surface for understanding why a job behaves the way it does.

This lesson teaches you to read the Spark UI not as a dashboard of green checkmarks, but as a window onto the execution model. The goal is not to memorize tabs. The goal is to map what you see on the screen back to the mental model:

```
User code → Logical plan → Optimized logical plan → Physical plan → Jobs → Stages → Tasks → Partitions → Executors → Storage
```

A job that *succeeds* is not the same as a job that is *healthy*. A job can complete correctly while shuffling ten times more data than necessary, spilling gigabytes to disk, running one straggler task for twenty minutes, or reading ten thousand tiny files. The UI is where you see the difference. By the end of this lesson you should be able to look at a slow or expensive job and form a concrete hypothesis about *which* part of the execution model is the bottleneck.

## Core mental model

**The Spark UI is a rendered view of the execution model. Each tab corresponds to a layer of the hierarchy you already understand.**

| UI tab | Layer of the model it exposes |
|---|---|
| **Jobs** | Actions → jobs. One job per action (roughly). |
| **Stages** | The stage boundaries created by shuffles. Tasks per stage. |
| **SQL / DataFrame** | The physical plan as a visual DAG, with row counts and timings per operator. |
| **Executors** | The cluster: how many executors, cores, memory, and how work and failures distribute across them. |
| **Storage** | Cached/persisted datasets and how much of each is in memory vs on disk. |
| **Environment** | The effective configuration (every `spark.*` setting actually in force). |

If you internalize that mapping, the UI stops being a foreign tool and becomes a direct readout of the concepts in lessons 04 (execution model), 07 (Catalyst), 09 (shuffles), 10 (partitioning), and 11 (skew).

## Main explanation

When you submit an action — `count()`, `write`, `collect()`, `display()` — Spark launches one or more **jobs**. The driver hands the optimized physical plan to the DAG scheduler, which breaks it into **stages** at every shuffle boundary. Each stage is a set of **tasks**, one per partition, dispatched to **executors** by the task scheduler. The UI records timing, data volume, and resource use at every one of these levels and presents them after the fact (and, while a job runs, in near-real-time).

The crucial interpretive skill is *triangulation*. No single number tells you what is wrong. A long stage duration could mean too much data, too few partitions, skew, spill, or slow storage. You distinguish between these by cross-referencing: stage duration against task-time distribution, shuffle read size against spill size, number of tasks against number of cores, input size against number of input files. The UI gives you all of these; the diagnosis comes from reading them together.

A second skill is knowing the *healthy baseline*. A balanced stage has tasks of roughly equal duration, little or no spill, and a task count that is a comfortable multiple of available cores. Deviations from that baseline are the signals. The rest of this lesson catalogs the most important deviations and what they mean.

### The Jobs tab

The Jobs tab lists every job triggered by an action, with its duration, the stages it contained, and a timeline. Its first diagnostic value is simply *how many jobs ran*. If you expected one write but see four jobs, something is triggering extra actions — often a `count()` for logging, a schema inference read, or a repeated action on an uncached DataFrame (lesson 05). Each job that recomputes an uncached lineage is wasted work that the Jobs timeline makes obvious.

The timeline view also shows whether jobs ran sequentially or overlapped, and where the gaps are. Long gaps between jobs with no active tasks often indicate **driver-side work**: planning a huge query, listing files in object storage, or collecting results back to the driver. That time is invisible in the Stages tab because no executor task is running — the Jobs timeline is where you catch it.

### The Stages tab

This is the most diagnostically rich tab. For each stage it shows the number of tasks, total duration, input/output sizes, shuffle read and write volumes, and — most importantly — the **task metrics distribution** (min, 25th percentile, median, 75th percentile, max). 

The summary metrics table is where skew becomes undeniable. In a balanced stage, the min, median, and max task durations are close together. In a skewed stage (lesson 11), the max is many times the median — one or a few tasks process a hot key while the rest finish quickly. You do not infer skew from a guess; you read it directly from the gap between the median and max columns of *Duration*, *Shuffle Read Size*, and *Records*.

The Stages tab also exposes **spill** (memory spill and disk spill columns). Spill means a task could not fit its working set — typically the build side of a hash aggregate or join, or sort buffers — in executor memory and had to write to disk and re-read it. Spill is not an error; the job still completes correctly. But it is a strong signal that the stage is memory-pressured, and it is often the hidden reason a stage is slow. Large disk-spill numbers point you toward more partitions (smaller per-task working sets), a broadcast join instead of a sort-merge join, or more executor memory.

### The SQL / DataFrame tab

This tab renders the **physical plan as a visual DAG**, annotated with per-operator metrics: number of output rows, data size, and time spent. It is the bridge between `explain()` (lesson 07) and the runtime reality. Where `explain()` shows the plan statically, this tab shows the plan *with the actual numbers that flowed through each operator*.

This is where you confirm whether the optimizer's decisions matched reality. Did the filter actually push down to the scan, reducing rows read (lesson 13)? Did the `BroadcastHashJoin` you expected actually appear, or did Spark fall back to `SortMergeJoin` because the "small" side was larger than the broadcast threshold (lesson 12)? Did an `AQEShuffleRead` node coalesce 200 shuffle partitions down to 8 (lesson 19)? The `Exchange` nodes in this DAG are your shuffles — counting them tells you how many times data crossed the network.

The per-operator row counts also catch **cardinality explosions**. If a join's output row count is dramatically larger than either input, you have a many-to-many join or a missing join key (lesson 12) — visible here as a single number, long before it shows up as an out-of-memory failure.

### The Executors tab

This tab shows the cluster as the scheduler sees it: each executor's cores, memory, active/completed/failed task counts, input and shuffle volumes, and GC time. 

Two patterns matter most. First, **uneven work distribution**: if one executor has processed far more data or tasks than others, you may have skew *or* poor partitioning that concentrated work on few executors. Second, **high GC time**: if executors spend a large fraction of their time in garbage collection, they are memory-pressured — too many objects, too little heap, or a working set that does not fit (which connects to Tungsten and off-heap memory in lesson 08). High GC time often co-occurs with spill and is a leading cause of mysteriously slow tasks.

The Executors tab is also where **executor failures** appear. A handful of failed-then-retried tasks (lesson 18) is normal resilience. A steady stream of executor losses points to memory exhaustion, node preemption, or a code path that crashes the JVM — and the failure asymmetry from lesson 03 applies: executor loss triggers recomputation, driver loss kills the application.

### The Storage tab

The Storage tab lists every DataFrame or RDD you have **cached or persisted** (lesson 17), showing the chosen storage level, the fraction cached in memory vs on disk, and the total size. Its primary diagnostic use is verifying that caching did what you intended. A DataFrame you called `.cache()` on but that shows **0% cached** means the cache was never materialized (no action triggered it) or was evicted under memory pressure — in which case every downstream "reuse" is silently recomputing the full lineage. If the cached fraction is partial, you are paying for caching but still recomputing the uncached partitions, often the worst of both worlds.

### The Environment tab

This tab lists the *effective* configuration — every `spark.*` property actually in force, including defaults and runtime overrides. It is the ground truth for settings discussed throughout this course: `spark.sql.shuffle.partitions` (lesson 09/10), `spark.sql.adaptive.enabled` (lesson 19), `spark.sql.autoBroadcastJoinThreshold` (lesson 12). When behavior surprises you — AQE didn't kick in, a broadcast didn't happen — the Environment tab tells you whether the config you *think* is set actually is. In managed runtimes like Fabric (lesson 21), this is how you discover what the platform configured on your behalf.

## Key concepts

- **Triangulation over single metrics.** No one number diagnoses a job. Cross-reference task-time distribution, shuffle volume, spill, partition count, and input file count.
- **Healthy baseline.** Balanced task durations, minimal spill, task count a comfortable multiple of cores, low GC time. Deviations are the signals.
- **Stage boundaries are shuffle boundaries.** The number of stages tells you how many shuffles your job performed (lesson 09).
- **The SQL DAG is the physical plan with real numbers.** Use it to confirm pushdown, join strategy, and AQE actually happened — not just that they were planned.
- **Success ≠ health.** A correct result can hide massive inefficiency. Observability is about cost and reliability, not just correctness.

### Key metrics and what they tell you

| Metric | Where | What a bad value means |
|---|---|---|
| Stage duration | Stages | Bottleneck stage; cross-check with task distribution |
| Task duration (median vs max) | Stages summary | Large gap ⇒ **skew** (lesson 11) |
| Shuffle read / write size | Stages, SQL DAG | Volume of data crossing the network; large ⇒ reconsider wide ops |
| Memory / disk spill | Stages | Working set exceeded memory; add partitions, broadcast, or memory |
| Input size & file count | Stages, SQL scan node | Many tiny files ⇒ **small-file problem** (lesson 13) |
| Records read / written | SQL DAG operators | Cardinality explosion in joins (lesson 12) |
| GC time | Executors | Memory pressure; correlates with spill and slow tasks |
| Cached fraction | Storage | <100% ⇒ silent recomputation (lesson 17) |
| Number of tasks vs cores | Stages / Executors | Too few ⇒ idle cores; too many tiny tasks ⇒ scheduling overhead (lesson 10) |

## Detailed technical sections

### Diagnosing a slow stage: a worked procedure

Suppose one stage dominates the job's runtime. Work through the layers:

1. **Open the Stages tab, sort by duration, open the slow stage.** Look at the task-time summary. Is the *max* close to the *median*, or far larger?
2. **If max ≫ median:** you have skew. The stage is gated by a few straggler tasks processing hot keys. Confirm by checking the *Records* and *Shuffle Read* percentiles — the slow tasks read far more rows. Mitigations: enable AQE skew handling (lesson 19), salt the hot key, or broadcast the other side (lesson 11/12).
3. **If all tasks are uniformly slow and spill is high:** the stage is memory-bound. Each task's working set doesn't fit. Increase the partition count so each task handles less data, or switch the join/aggregate strategy.
4. **If all tasks are fast but there are very few of them:** you are under-partitioned and leaving cores idle. Check task count against total executor cores (Executors tab). Repartition upstream or raise `spark.sql.shuffle.partitions`.
5. **If there are tens of thousands of tiny tasks:** you are over-partitioned, paying scheduling overhead per task. Coalesce, or — for input — you are hitting the small-file problem; compact upstream (lesson 13/14 OPTIMIZE).
6. **If the slow "stage" is actually a gap with no running tasks:** return to the Jobs timeline; the cost is driver-side (planning, file listing, `collect()`).

### Recognizing shuffles, skew, spills, overhead, and small files

These five pathologies have distinct UI signatures:

- **Excessive shuffling:** many `Exchange` nodes in the SQL DAG, large shuffle read/write in Stages. Trace each shuffle to the wide operation that caused it (join, groupBy, distinct, window, repartition).
- **Skew:** max task duration / shuffle read far exceeds the median within a single stage.
- **Spill:** non-zero memory/disk spill columns. Disk spill is the more expensive kind.
- **Scheduling overhead:** huge task counts with very short per-task durations; total time dominated by scheduling rather than compute.
- **Small files:** scan node reads a large *number* of files with small total *size*; the input stage has thousands of tiny tasks (lesson 13).

### Observability beyond the UI: production jobs

The Spark UI is interactive and ephemeral — ideal for development and ad-hoc debugging, less so for unattended production pipelines (the live UI disappears when the application ends, though a **history server** can persist event logs for after-the-fact inspection). Production observability requires layering additional signals on top:

- **Structured logging** at pipeline boundaries: which input partitions were read, row counts in and out, runtime, config version. This turns a "job ran" into "job processed N rows from date X in T seconds."
- **Metrics emission**: durations, row counts, shuffle/spill volumes pushed to a monitoring system so you can see trends and alert on regressions — not just inspect one run.
- **Data-quality checks**: row counts, null rates, schema conformance, referential integrity (lesson 15 quality gates). A job can succeed mechanically while producing wrong or empty output; only data-quality assertions catch that.
- **Lineage and run metadata**: in Delta-based pipelines (lesson 14), the transaction log records what each commit added, giving an audit trail independent of the UI.

The governing principle: **successful completion is not enough.** A green job that wrote zero rows, or doubled rows via a bad join, or silently dropped a malformed input file, is a failed pipeline that merely didn't raise an exception. Observability is the discipline of being able to *assert* correctness and efficiency, not merely *observe* exit codes.

## PySpark and SQL perspective

The UI is engine-level and therefore identical whether you wrote PySpark or Spark SQL — both compile to the same physical plan (lesson 02), so the same SQL DAG, the same stages, and the same shuffles appear regardless of the surface API. This is a useful reminder: there is no "PySpark performance" separate from "SQL performance" for equivalent logic; they are the same plan.

The one place the surface API matters in the UI is **Python UDFs**. A Python UDF (lesson 02) shows up as a distinct operator (`BatchEvalPython` or `ArrowEvalPython`) in the SQL DAG, and the time spent there is time spent shipping rows out of the JVM to a Python worker process and back. If a stage is slow and the DAG shows a Python-eval node consuming most of the operator time, the diagnosis is the UDF boundary, not Spark itself — and the remedy is a built-in expression or a vectorized (Pandas/Arrow) UDF.

From `explain()` to the UI is a natural workflow: use `df.explain("formatted")` (lesson 07) *before* running to predict the plan, then open the SQL DAG *after* running to confirm the prediction held and to see the actual row counts and timings. When prediction and reality diverge — a broadcast became a sort-merge, a pushdown didn't happen — that divergence is your most valuable diagnostic clue.

## Architectural implications

Observability is not a debugging afterthought; it is an architectural property you design in. A pipeline built for observability emits row counts and quality metrics at every medallion boundary (lesson 15), uses Delta so every write is an auditable commit (lesson 14), and is structured so that each stage's purpose is legible in the SQL DAG. A pipeline built without it is a black box that you can only inspect when it fails — and by then the live UI may be gone.

The deeper architectural point is that the UI rewards designs that match the execution model. A job whose stages, shuffles, and partition counts are *intentional* is easy to read in the UI; a job assembled without regard for wide operations and partitioning produces a tangled DAG full of unexplained exchanges and skewed stages. **Legibility in the UI is a downstream consequence of architectural clarity.** If your job is hard to read in the UI, that is usually a signal that the job itself is poorly structured, not that the UI is inadequate.

Finally, the UI defines what "tuning" means. You do not tune by changing configs at random; you tune by reading the UI, forming a hypothesis about which layer of the model is the bottleneck, changing one thing, and re-reading the UI to confirm. The UI makes performance work empirical rather than superstitious.

## Common misconceptions

- **"The job succeeded, so it's fine."** Success means no exception was raised. It says nothing about shuffle volume, spill, skew, or whether the output is correct. (See: success ≠ health.)
- **"A long stage means too much data."** It might — or it might be skew, spill, under-partitioning, or slow storage. Only the task-time distribution and spill metrics distinguish these.
- **"More executors will fix a slow stage."** Only if the stage is parallelism-bound. If it is skewed, the straggler task runs on a single core regardless of cluster size (lesson 11).
- **"The Storage tab confirms my cache is helping."** Only if it shows 100% cached. A partially cached or 0% dataset is recomputing silently.
- **"PySpark is slower than SQL, so the UI will differ."** For equivalent logic they compile to the same plan and produce the same UI. The exception is Python UDFs, which appear as a distinct, often costly operator.
- **"The UI is only for debugging failures."** It is primarily for diagnosing *inefficiency* in jobs that succeed — that is where most of the value is.

## Summary

The Spark UI is a rendered view of the execution model you have studied throughout this course. The Jobs tab maps to actions; Stages to shuffle boundaries and task distributions; the SQL/DataFrame tab to the physical plan with real row counts; Executors to the cluster and its failures; Storage to caching; Environment to effective config. Diagnosis comes from *triangulating* these metrics against a healthy baseline — balanced task times, minimal spill, sensible task-to-core ratios — rather than reading any single number. The five recurring pathologies (excessive shuffles, skew, spill, scheduling overhead, small files) each have a distinct UI signature. And because a successful job can still be wildly inefficient or silently wrong, production observability layers structured logging, metrics, data-quality checks, and lineage on top of the ephemeral UI. The recurring theme: **successful completion is not enough**, and legibility in the UI is the reward for architectural clarity in the job itself.

## Terms to remember

- **Job / stage / task:** the execution hierarchy the UI is organized around; one job per action, one stage per shuffle boundary, one task per partition.
- **Task-time distribution (min/median/max):** the summary metrics that reveal skew when max ≫ median.
- **Spill (memory/disk):** working set exceeded executor memory and was written to disk; a key signal of memory pressure.
- **Exchange:** the physical-plan operator representing a shuffle; counting them counts your network round-trips.
- **AQEShuffleRead:** the operator showing Adaptive Query Execution coalescing or splitting shuffle partitions at runtime (lesson 19).
- **GC time:** executor time spent in garbage collection; high values indicate memory pressure.
- **History server:** persists event logs so the UI survives after an application ends.
- **Small-file signature:** a scan reading many files with small total size and producing thousands of tiny input tasks.
- **Triangulation:** the diagnostic method of cross-referencing multiple metrics rather than trusting one.
- **Success ≠ health:** the principle that a job completing without error guarantees neither efficiency nor correctness.
