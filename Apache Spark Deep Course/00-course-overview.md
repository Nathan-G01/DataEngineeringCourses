# Spark Deep Course for Data Engineering

## Purpose of this course

This course exists to give a software engineer a *mechanical, architectural* understanding of Apache Spark. The goal is not to teach you how to call `groupBy` and read the result. The goal is to make you able to look at a transformation, predict how Spark will plan and execute it across a cluster, and reason about whether that execution will be fast, slow, correct, reproducible, and operable in production.

Most Spark material teaches Spark as a *Python library*: a thing you import, call methods on, and get DataFrames back from. That framing is actively misleading. Spark is a **distributed query planning and execution engine**. The Python (or SQL, or Scala) you write is a *description of intent*. Spark compiles that description into a logical plan, optimizes it, turns it into a physical plan, breaks that plan into a graph of stages and tasks, schedules those tasks across executors, moves data over the network when required, and reads and writes distributed storage. The Python API is the thinnest possible surface over a sophisticated compiler and scheduler.

This course is deliberately **architecture-heavy and theory-heavy**. It is written for master's-level study and for engineers who design production pipelines. It is *not* exercise-based. There are no tasks to complete, no "now you try" sections, no graded problems. Code appears only to clarify what the engine does. Every code example is accompanied by an explanation of what Spark builds and executes internally.

You are assumed to be a working software engineer comfortable with C#, Azure, microservices, and general data engineering concepts, who runs Spark primarily through Microsoft Fabric notebooks using **PySpark and Spark SQL**. The explanations are **cloud-agnostic** — the architecture described holds on Fabric, Databricks, Synapse, EMR, Kubernetes, and standalone Spark — but Fabric is used as a concrete example where it helps.

## What you should understand by the end

By the end of this course you should be able to:

- Explain Spark as a query engine that produces plans, not as a library that produces results.
- Trace any DataFrame or SQL expression through the full pipeline: user code → logical plan → optimized logical plan → physical plan → jobs → stages → tasks → partitions → executors → storage.
- Predict where shuffles occur, why they are expensive, and how to reduce them.
- Reason about partitioning, parallelism, and data skew *before* writing code.
- Choose and predict join strategies (broadcast hash, sort-merge, shuffle hash) from table sizes and statistics.
- Explain what Catalyst and Tungsten do, and why built-in functions beat Python UDFs.
- Design lakehouse storage using columnar formats and Delta Lake, and explain the transaction log, ACID guarantees, and time travel.
- Reason about batch vs streaming, fault tolerance, lineage, caching, checkpointing, and Adaptive Query Execution.
- Read the Spark UI and move from a symptom (long-tail task, large spill, tiny files) to a root cause.
- Apply all of this inside Microsoft Fabric without being misled by what Fabric abstracts away.

The recurring mental model for the entire course is this:

```text
User code
  ↓
Logical plan
  ↓
Optimized logical plan
  ↓
Physical plan
  ↓
Jobs
  ↓
Stages
  ↓
Tasks
  ↓
Partitions
  ↓
Executors
  ↓
Storage
```

Internalize this chain. Almost every question about Spark performance or correctness is a question about one link in it.

## Full lesson list

| # | File | Topic |
|---|------|-------|
| 00 | `00-course-overview.md` | This overview, dependency map, glossary |
| 01 | `01-what-is-spark.md` | What Spark is, and what it is not |
| 02 | `02-spark-languages-and-apis.md` | Scala, Python, SQL, Java, R; DataFrame/SQL/Dataset/RDD |
| 03 | `03-spark-architecture.md` | Driver, executors, cluster manager, SparkSession |
| 04 | `04-execution-model.md` | Application, job, stage, task, partition, DAG |
| 05 | `05-transformations-actions-lazy-evaluation.md` | Lazy evaluation and lineage |
| 06 | `06-dataframes-sql-datasets-rdds.md` | The four abstractions compared |
| 07 | `07-catalyst-optimizer.md` | Logical planning and optimization |
| 08 | `08-tungsten-and-physical-execution.md` | Binary rows, codegen, memory |
| 09 | `09-shuffles.md` | The most important performance concept |
| 10 | `10-partitioning-and-parallelism.md` | Partitions, tasks, cores, files |
| 11 | `11-data-skew.md` | Hot keys and the long tail |
| 12 | `12-joins.md` | Logical joins vs physical join strategies |
| 13 | `13-file-formats-and-lakehouse-storage.md` | CSV, JSON, Avro, Parquet, ORC |
| 14 | `14-delta-lake.md` | Transaction log, ACID, time travel |
| 15 | `15-medallion-architecture.md` | Bronze, Silver, Gold |
| 16 | `16-batch-vs-streaming.md` | Bounded vs unbounded, Structured Streaming |
| 17 | `17-caching-persistence-and-checkpointing.md` | Materialization decisions |
| 18 | `18-fault-tolerance-and-lineage.md` | Recomputation and reliability |
| 19 | `19-adaptive-query-execution.md` | Runtime replanning |
| 20 | `20-spark-ui-and-observability.md` | Reading execution from symptoms |
| 21 | `21-spark-in-microsoft-fabric.md` | Spark inside Fabric |
| 22 | `22-architectural-thinking-for-spark-jobs.md` | Design questions and anti-patterns |
| 23 | `23-final-synthesis.md` | One coherent narrative |

## Dependency map between lessons

The course is layered. Each layer assumes the previous ones. You can read out of order, but the dependencies below describe what genuinely builds on what.

```text
Spark basics (01, 02, 05, 06)
  → Architecture (03)
  → Execution model (04)
  → Catalyst / Tungsten (07, 08)
  → Shuffles / joins / partitioning / skew (09, 10, 11, 12)
  → Storage / lakehouse / Delta (13, 14, 15)
  → Streaming / fault tolerance / AQE (16, 17, 18, 19)
  → Observability / Fabric / architecture (20, 21, 22)
  → Synthesis (23)
```

A more detailed view of the strongest dependencies:

- **Architecture (03)** and **execution model (04)** are the spine. Almost everything else is a refinement of how plans become tasks on executors.
- **Catalyst (07)** explains *why* DataFrame/SQL beats RDD; it depends on understanding transformations and plans (05, 06).
- **Tungsten (08)** explains *how* the optimized plan executes physically; it pairs with Catalyst.
- **Shuffles (09)** is the single most load-bearing performance concept. **Partitioning (10)**, **skew (11)**, and **joins (12)** are all elaborations of "what happens when data must move."
- **Storage (13)**, **Delta (14)**, and **medallion (15)** form the persistence and design layer.
- **AQE (19)** depends on understanding shuffles, joins, and skew, because AQE's job is to fix planning mistakes at runtime that those lessons describe.
- **Observability (20)** depends on nearly everything, because the Spark UI is just the execution model made visible.

## How to study this course

**Compressed path (6–8 hours).** Read 00 for orientation. Read 01, 03, 04 carefully — these give you the engine model. Read 05 and 07 to understand laziness and optimization. Read 09 (shuffles) and 12 (joins) as the performance core. Read 14 (Delta) and 15 (medallion) for storage and design. Finish with 22 and 23. Skim the rest. In this path you are building a *working mental model* and trusting that the detailed lessons exist for reference when a real problem appears.

**Deepened path (several weeks).** Read in order, one or two lessons per study session. After each lesson, force yourself to trace a real example through the full mental-model chain. When you read the architecture and execution lessons, open the Spark UI on a real Fabric notebook and match each tab to the concept. When you read shuffles, joins, and skew, deliberately write queries that *cause* each behavior and inspect the explain plan and the UI. When you read Delta, inspect a real `_delta_log`. The course is designed so that the theory becomes verifiable against a live engine. The fastest way to make this knowledge permanent is to predict what Spark will do, then check.

A useful discipline throughout: before running anything, write down (a) where you expect shuffles, (b) how many stages you expect, and (c) which side of each join you expect to be broadcast. Then verify with `explain` and the UI. The gap between your prediction and reality is exactly your remaining learning.

## Glossary of the 20 most important terms

1. **Driver** — the process that runs your code, builds plans, and schedules work. One per application.
2. **Executor** — a JVM process on a worker that runs tasks and holds data partitions in memory/disk. Many per application.
3. **SparkSession** — the entry point object representing one Spark application and its configuration and catalog.
4. **Partition** — a contiguous chunk of a distributed dataset; the unit of parallelism. One task processes one partition.
5. **Task** — the smallest unit of execution; runs the stage's logic over one partition on one executor slot.
6. **Stage** — a set of tasks that can run without shuffling; stage boundaries are shuffle boundaries.
7. **Job** — all the work triggered by one action. One action can trigger multiple jobs.
8. **DAG** — directed acyclic graph of stages/RDD operations that Spark schedules.
9. **Transformation** — a lazy operation that builds a plan (e.g. `filter`, `groupBy`). Produces a new DataFrame, runs nothing.
10. **Action** — an operation that triggers execution (e.g. `count`, `write`, `collect`).
11. **Lazy evaluation** — Spark defers computation until an action, enabling whole-query optimization.
12. **Lineage** — the recorded chain of transformations used to recompute lost data.
13. **Shuffle** — redistribution of data across partitions/executors by key; expensive (network, disk, serialization).
14. **Narrow dependency** — each output partition depends on one input partition; no shuffle.
15. **Wide dependency** — output partitions depend on many input partitions; requires a shuffle.
16. **Catalyst** — Spark's query optimizer that transforms and optimizes logical and physical plans.
17. **Tungsten** — the execution backend: binary row format, memory management, whole-stage code generation.
18. **DataFrame** — a distributed, schema-bearing relational dataset; a *plan*, not materialized data.
19. **Delta Lake** — an open table format adding a transaction log, ACID, schema enforcement, and time travel on top of Parquet.
20. **AQE (Adaptive Query Execution)** — runtime re-planning using actual statistics (coalescing partitions, switching joins, handling skew).

## Summary

Spark is a distributed compiler and scheduler with a thin language surface. This course teaches the compiler and the scheduler, not the surface. Keep the user-code-to-storage chain in mind at all times; nearly every lesson is an expansion of one of its links. Read for the compressed mental model first, then deepen by predicting and verifying against a live engine.
