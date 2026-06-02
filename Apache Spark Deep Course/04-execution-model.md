# Execution Model

## Purpose of this lesson

This lesson explains how Spark turns a high-level DataFrame or SQL expression into concrete distributed work. The vocabulary — application, job, stage, task, partition — and the relationships between them are the operational core of Spark. Once you can look at a transformation and say "this is one job, two stages split by a shuffle, with N tasks per stage," you can predict execution, read the Spark UI fluently, and diagnose performance. This lesson is the bridge between the *plan* (lessons 05–08) and the *machine* (lesson 03).

## Core mental model

```text
Application
   └── Job            (one per action)
        └── Stage     (bounded by shuffles)
             └── Task (one per partition, runs on a slot)
                  └── Partition (the data the task processes)
```

And the dependency rule that creates stage boundaries:

```text
Narrow dependency  → stays in the same stage (pipelined, no data movement)
Wide dependency    → forces a shuffle → new stage boundary
```

## Main explanation

### The hierarchy of execution units

Spark organizes execution into a strict hierarchy. Understanding each level and how one level produces the next is the whole lesson.

**Application.** The top-level unit (lesson 03): one driver and its executors for a session. An application runs many jobs over its lifetime.

**Job.** A job is **all the work triggered by a single action**. Every time you call an action (`count`, `write`, `collect`, `show`), the driver launches one or more jobs to produce that result. No action, no job — this is laziness made concrete. A job corresponds to "compute this result now."

**Stage.** A job is divided into **stages**. A stage is a maximal set of operations that can run **without shuffling data** — i.e., a pipeline of narrow transformations. Stage boundaries occur exactly where a **shuffle** is required (a wide dependency). If a job needs to redistribute data twice (say, a join's shuffle then a groupBy's shuffle), it will have multiple stages separated by those shuffles.

**Task.** A stage is executed as a set of **tasks**, one task **per partition** of the stage's data. All tasks in a stage run the *same* code on *different* partitions. Tasks are the units actually scheduled onto executor slots. A stage with 200 partitions launches 200 tasks; with 8 slots available, they run in waves of 8.

**Partition.** A partition is a chunk of the distributed dataset — the data one task consumes. Partition count drives task count drives parallelism. Partitions come from input splits (file sizes) initially and from shuffle configuration after a shuffle (lesson 10).

### Action-triggered execution

Nothing computes until an action. When you chain transformations, you are building a logical plan (lesson 05). The action is the trigger that says "materialize this." At that moment the driver:

1. Finalizes the logical plan and runs Catalyst to get a **physical plan**.
2. Builds a **DAG** of stages from the physical plan, inserting stage boundaries at each shuffle (exchange).
3. Submits stages in dependency order; for each stage, creates one task per partition.
4. Schedules tasks onto executor slots, tracks completion, retries failures, and assembles the result.

This is why two identical transformation chains with different actions can produce different work, and why calling an action twice recomputes everything (unless cached — lessons 05, 17).

### The DAG: directed acyclic graph

Spark represents the computation as a **DAG** — nodes are operations/stages, edges are data dependencies, and it is *acyclic* because data flows forward through transformations without loops. The DAG is what lets Spark generalize beyond MapReduce's fixed shape: an arbitrary pipeline of stages with arbitrary dependencies. Two scheduler components operate on it:

- **DAG scheduler.** Operates at the stage level. It takes the physical plan, splits it into stages at shuffle boundaries, determines stage dependencies, and submits stages when their parents complete. It also handles stage-level fault recovery (resubmitting a stage if its shuffle output was lost).
- **Task scheduler.** Operates at the task level. It takes the tasks of a ready stage and assigns them to executor slots, considering data locality, and handles task-level retries (re-running a failed task, possibly on another executor).

Think of the DAG scheduler as planning *which stages run in what order* and the task scheduler as placing *individual tasks on cores*.

## Key concepts

- **Job** = work for one action.
- **Stage** = pipeline of narrow ops between shuffles.
- **Task** = stage logic over one partition; the scheduled unit.
- **Partition** = data unit; partition count ≈ task count.
- **Narrow dependency** = no shuffle; stays in stage.
- **Wide dependency** = shuffle; new stage.
- **DAG** = the staged execution graph the schedulers operate on.

## Detailed technical sections

### Narrow vs wide dependencies

The distinction that creates stages:

- A **narrow dependency** means each output partition depends on **at most one** input partition. The computation for an output partition can be done entirely from one input partition, *on the same executor, with no data movement*. Examples: `filter`, `select`, `withColumn`, `map`. These pipeline together within a stage.

- A **wide dependency** means an output partition depends on **many** input partitions. To produce an output partition you must gather data from across the cluster — a **shuffle**. Examples: `groupBy`, `join` (non-broadcast), `distinct`, `repartition`, `orderBy`. Each wide dependency forces a stage boundary.

```text
Narrow (within one stage, pipelined):
  P0 ──► P0'
  P1 ──► P1'      each output from one input, no cross-talk
  P2 ──► P2'

Wide (shuffle, stage boundary):
  P0 ─┐
  P1 ─┼─► [ shuffle: redistribute by key ] ─► P0''  P1''  P2''
  P2 ─┘                                       each output from many inputs
```

### Stage boundaries and shuffle boundaries are the same thing

A stage *ends* precisely where a shuffle is needed, and the next stage *begins* on the other side of that shuffle. The first stage is the **map side** (it reads input, applies narrow operations, and writes shuffle files partitioned by the shuffle key); the next stage is the **reduce side** (it reads the shuffle files for its assigned key range and continues). The shuffle is the data-movement step *between* the two stages. So "how many stages?" reduces to "how many shuffles, plus one." A job with no shuffles is a single stage; a job with two shuffles has three stages.

### Pipeline execution within a stage

Within a stage, narrow transformations are **fused**: Spark does not materialize the intermediate result of each operation separately. Thanks to whole-stage code generation (lesson 08), a chain like `filter` → `select` → `withColumn` becomes a single compiled loop over each partition's rows. A row is read, filtered, projected, and transformed in one pass, then emitted — there is no intermediate "filtered DataFrame" sitting in memory. This pipelining is why narrow chains are cheap and why keeping operations narrow (avoiding shuffles) is the central performance goal.

### How a DataFrame operation becomes distributed tasks

Tracing `df.filter(...).groupBy("k").count()` end to end:

```text
1. Transformations build a logical plan: Aggregate(count) ← Filter ← Scan.
2. Action (e.g. .write or .show) triggers execution.
3. Catalyst optimizes; physical plan inserts an Exchange (shuffle) before the final aggregate.
4. DAG scheduler splits into two stages:
      Stage 1 (map side): scan + filter + PARTIAL aggregate, write shuffle files by key k.
      Stage 2 (reduce side): read shuffle files, FINAL aggregate (sum partials), produce result.
5. Stage 1 has one task per INPUT partition (say 100 → 100 tasks).
   Stage 2 has one task per SHUFFLE partition (default 200, or AQE-coalesced → say 8 tasks).
6. Task scheduler places tasks on slots; they run in waves bounded by total cores.
7. Result is written/returned.
```

Notice Spark does a **partial aggregate** on the map side (counting within each input partition first) so the shuffle moves far less data — a per-key partial count rather than every row. This map-side combine is an automatic optimization for aggregations.

### Why one action can create multiple jobs

Although the simple rule is "one action = one job," some actions internally trigger several jobs. Common causes: reading with **schema inference** on JSON/CSV launches a job to sample/scan for the schema *before* the main job; certain operations that need a preliminary pass (e.g., computing range boundaries for `orderBy`'s range partitioning, or building a broadcast) run their own job; and some writes do extra coordination jobs. So in the Spark UI you may see one `.write` produce two or three jobs. The mental rule still holds at the level of *your* logic; the extra jobs are Spark's internal preparation.

### Why one job can create multiple stages

Because each shuffle splits the job. A job's stage count equals (number of shuffles in its physical plan) + 1. A pure filter-and-write job is one stage. A `groupBy` job is two. A job that joins two tables (one shuffle) and then aggregates (another shuffle) is three or more. Reading the number of stages off the explain plan (count the `Exchange` operators) is a fast way to anticipate cost.

### Why one stage can create many tasks

Because a stage runs one task per partition, and datasets typically have many partitions. A stage over a 100-partition input has 100 tasks. A reduce-side stage has one task per shuffle partition (default 200, often adjusted by AQE). Task count is therefore a direct function of partition count, and partition count is a function of input file splitting and shuffle configuration (lesson 10). This is the lever for parallelism: too few partitions starves your cores; too many adds scheduling overhead.

### Why task count usually follows partition count

The invariant "one task per partition per stage" is the bridge from the data view (partitions) to the execution view (tasks). When you ask "how parallel is this stage?", the answer is "as parallel as it has partitions, capped by available slots." When you see thousands of tiny tasks, you have too many partitions (small-file or over-partitioning problem); when you see a handful of huge tasks, too few. Controlling partition count *is* controlling task granularity.

### How Spark parallelism emerges

Parallelism is the product of three numbers: **partitions** (how many independent pieces of work exist), **slots/cores** (how many can run at once), and **stages** (which run sequentially in dependency order, since a reduce stage waits for its map stage's shuffle). Within a stage, up to `total cores` tasks run simultaneously; the stage finishes when its slowest task does (which is why **skew** — one giant partition — ruins parallelism, lesson 11). Across stages, work is serialized at shuffle boundaries. So total runtime ≈ sum over stages of (waves of tasks × wave duration), and your goal is balanced partitions, enough of them to fill cores, and as few shuffle boundaries as possible.

## PySpark and SQL perspective

**Example 1 — narrow, single stage:**

```python
df.filter("country = 'CH'").select("id").count()
```

`filter` and `select` are narrow: each output partition comes from one input partition, no data moves. This is a pipeline within **one stage**. (`count` adds a tiny final aggregation of per-partition counts, which is a cheap shuffle of single numbers — in practice you'll see one main scan/filter stage plus a trivial final step.) The map side does almost all the work per partition independently; parallelism = min(partitions, cores). There is essentially no cross-cluster data movement of the rows themselves.

**Example 2 — wide, two stages:**

```python
df.groupBy("country").count()
```

`groupBy` is wide: all rows with the same `country` must end up together, which requires redistributing data by `country` across the cluster — a **shuffle**. This creates a **stage boundary**. Stage 1 scans and computes partial counts per partition; the shuffle redistributes partial counts keyed by `country`; Stage 2 sums the partials per country. Two stages, a shuffle between them, task counts following input partitions (stage 1) and shuffle partitions (stage 2).

The contrast is the lesson in miniature: **the first is narrow (no movement, one stage); the second is wide (shuffle, two stages).** The same distinction holds for the SQL equivalents (`WHERE ... ` vs `GROUP BY ...`), because both compile to the same physical plans. You can confirm by reading the explain output and counting `Exchange` nodes, or by watching the Jobs/Stages tabs in the Spark UI.

## Architectural implications

- **Count the shuffles to count the stages.** Fewer shuffles ⇒ fewer stage boundaries ⇒ less data movement and serialization.
- **Match partition count to cores.** Enough partitions to fill all slots, not so many that scheduling overhead dominates (lesson 10).
- **Stages are synchronization points.** A reduce stage cannot start until its map stage's shuffle completes; long map-side tails delay everything downstream.
- **Skew attacks the task level.** One oversized partition means one long task that holds up the whole stage, regardless of how many executors you add (lesson 11).
- **Partial aggregation is your friend** — design aggregations so Spark can combine map-side and shuffle little.

## Common misconceptions

- *"Each transformation runs when I call it."* No — transformations build the plan; the action runs the job.
- *"One action is always one job."* Usually, but schema inference, range partitioning, and broadcasts can add jobs.
- *"More stages means a bigger/better-parallelized job."* More stages means more shuffles — usually *worse*, not better.
- *"Tasks and stages are interchangeable terms."* No — a stage contains many tasks (one per partition).
- *"Adding executors fixes a slow stage."* Only if the stage is partition-bound and balanced; skew or too-few-partitions won't be helped.

## Summary

Spark execution is a hierarchy: an application runs jobs (one per action), each job splits into stages at shuffle boundaries, each stage runs as tasks (one per partition) on executor slots. Narrow dependencies pipeline within a stage with no data movement; wide dependencies force shuffles that create stage boundaries. The DAG scheduler orders stages; the task scheduler places tasks. Parallelism emerges from partitions × cores across sequential stages. To predict and tune a job, count its shuffles (stages), check its partitioning (task balance and count), and watch for skewed tasks that stall a stage.

## Terms to remember

- **Job** — all work triggered by one action.
- **Stage** — maximal narrow pipeline between shuffles.
- **Task** — stage logic over one partition; the scheduled unit.
- **Partition** — data chunk; one per task per stage.
- **Narrow / wide dependency** — no-shuffle vs shuffle; the stage-boundary rule.
- **DAG scheduler / task scheduler** — stage-level vs task-level scheduling.
- **Exchange** — the physical shuffle operator marking a stage boundary.
