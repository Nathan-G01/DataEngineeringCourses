# What Is Spark

## Purpose of this lesson

This lesson establishes the single most important idea in the course: Spark is a **distributed query planning and execution engine**, not a Python library and not "big pandas." Everything else — performance, correctness, lakehouse design — follows from understanding what Spark fundamentally *is* and what category of system it belongs to. We will define Spark precisely, contrast distributed compute with local compute, and position Spark against databases, message brokers, ETL tools, and lakehouses so you know exactly which problems it solves and which it does not.

## Core mental model

Spark takes a *declarative description of a computation over data* and turns it into a *distributed physical execution* across many machines. You describe **what** you want; Spark decides **how** and **where** to compute it.

```text
You describe intent  →  Spark plans it  →  Spark executes it across a cluster  →  Results land in storage
```

The crucial consequence: when you write a line of PySpark, **nothing computes**. You are appending a node to a query plan. Computation happens only when an *action* forces it. Hold this thought; it reorganizes how you read every program.

## Main explanation

Apache Spark is a general-purpose engine for large-scale data processing. Its job is to let you express a computation once, in a high-level API, and have that computation run in parallel across a cluster of machines, with the engine handling distribution, scheduling, data movement, memory management, and fault recovery.

### Why Spark exists

Before Spark, large-scale distributed processing was dominated by Hadoop MapReduce. MapReduce was powerful but had two structural problems. First, it forced every computation into a rigid map-then-reduce shape, writing intermediate results to disk between every step. A multi-stage pipeline meant many round trips to disk, which is slow. Second, the programming model was low-level and verbose; expressing a join or an iterative algorithm was painful.

Spark was designed to fix both. It keeps intermediate data **in memory** across stages when possible, avoiding constant disk round-trips, and it exposes a high-level API (RDDs originally, then DataFrames and SQL) that lets you express rich pipelines concisely. Critically, Spark generalized the execution model from "map then reduce" to an arbitrary **DAG (directed acyclic graph)** of stages, so a single application can chain many transformations and the engine schedules them efficiently. Spark is, in effect, MapReduce's successor: same distributed philosophy, far better execution model and developer experience.

### Distributed compute vs local compute

Local compute runs on one machine: one process, shared memory, data that fits in RAM or on a local disk, and a single point of failure that simply means "the program crashed." Reasoning is simple because everything is in one place.

Distributed compute spreads data and computation across many machines. This buys **scale** (you can process datasets far larger than any one machine's memory) and **parallelism** (many cores work simultaneously), but it introduces three hard realities that shape everything Spark does:

- **Data is partitioned.** No single machine holds all the data. Each machine holds a slice (a partition). Operations that need to combine data across slices require moving data between machines — the **shuffle**, the dominant cost in distributed processing.
- **The network is slow and failure is normal.** Moving data between machines is orders of magnitude slower than reading local memory. Machines and processes fail routinely at scale, so the engine must tolerate partial failure without losing correctness.
- **Coordination has overhead.** Splitting work, scheduling it, and collecting results all cost time. For small data, that overhead can exceed the work itself — which is exactly why Spark is the wrong tool for small jobs.

Spark exists to give you the *benefits* of distributed compute (scale, parallelism) while hiding most of the *mechanics* (partitioning, scheduling, fault recovery) behind a high-level API. But "hiding" is not "eliminating": the mechanics still govern performance, and an engineer who ignores them writes slow, fragile pipelines.

### Spark as an execution engine

The clearest way to understand Spark is as a **compiler plus scheduler for data computations**:

- The **compiler** part (Catalyst) takes your DataFrame/SQL expressions, builds a logical plan, optimizes it, and produces a physical plan of concrete operators.
- The **scheduler** part breaks the physical plan into jobs, stages, and tasks, and dispatches tasks to executors, handling data movement and failures.

This is why the course insists you think in terms of the user-code-to-storage chain. Spark is not "running your Python." It is *translating* your Python into a distributed program and running that.

## Key concepts

- Spark is an **engine**, not a storage system. It computes over data that lives elsewhere (object storage, lakehouse tables, files).
- Spark is **declarative at the surface** (DataFrame/SQL) and **imperative underneath** (a scheduled DAG of tasks).
- Spark separates **compute** from **storage**, **metadata**, and **orchestration** (defined below).
- Spark computation is **lazy**: plans are built, then executed on an action.

### Compute vs storage vs metadata vs orchestration

A modern data platform has four distinct concerns, and confusing them is a common source of architectural mistakes:

| Concern | What it does | Examples |
|---------|--------------|----------|
| **Compute** | Executes transformations over data | **Spark**, SQL warehouse engines |
| **Storage** | Persists bytes durably and cheaply | Object storage (ADLS/OneLake/S3), Parquet files |
| **Metadata** | Tracks tables, schemas, versions, statistics | Hive metastore, Unity Catalog, Delta log, Fabric catalog |
| **Orchestration** | Schedules and sequences jobs, handles dependencies/retries | Fabric Data Pipelines, Airflow, ADF |

Spark is squarely in the **compute** box. It *reads from* and *writes to* storage, *consults and updates* metadata, and is *triggered by* orchestration. Keeping these boxes distinct is the foundation of lakehouse thinking: the same data in storage can be read by Spark, by a SQL warehouse, or by a BI tool, because storage and compute are decoupled.

## Detailed technical sections

### Spark compared with a database

A relational database (e.g. SQL Server) bundles compute, storage, metadata, indexing, transactions, and a query engine into one tightly integrated system, optimized for many concurrent, low-latency reads and writes of relatively current data, with strong transactional guarantees on its *own* storage.

Spark is **only the query/compute engine**, and it operates over **external, decoupled storage** it does not own. It is optimized for **high-throughput batch and streaming computation over large datasets**, not low-latency point lookups or high-concurrency OLTP. Spark has no built-in durable storage, no native indexing in the database sense, and (by itself) no transactional table semantics — those come from a *table format* like Delta layered on top. Use a database for serving and transactions; use Spark for large-scale transformation.

### Spark compared with a message broker

A message broker (Kafka, Event Hubs, Service Bus) **transports** events between producers and consumers. It is concerned with durable, ordered, partitioned delivery of messages — moving data, not transforming it. It does not run joins, aggregations, or query optimization.

Spark **processes** data. In streaming scenarios, Spark *consumes from* a broker and computes over the stream (aggregations, joins, stateful logic), then writes results to a sink. The broker is the pipe; Spark is the processor attached to the pipe. Confusing "we have Kafka" with "we have stream processing" is a classic mistake — transport and processing are different layers (covered in lesson 16).

### Spark compared with an ETL tool

Traditional ETL tools (SSIS, drag-and-drop pipeline designers) provide a visual, component-based way to move and transform data, often row-oriented and tied to specific connectors. Spark is a *programmable* engine: you express transformations in code (or SQL), and it compiles them into a distributed plan. Spark scales to far larger data and gives you full control and optimization, at the cost of requiring you to think like an engineer rather than wire boxes together. Many modern "ETL tools" (including Fabric pipelines) actually *orchestrate* Spark or SQL under the hood — orchestration and compute again being distinct.

### Spark compared with a lakehouse

A **lakehouse** is an *architecture*, not an engine. It combines cheap, open object storage (like a data lake) with database-like table semantics (ACID transactions, schema, time travel) provided by a **table format** such as Delta Lake, and queried by multiple compute engines. Spark is one of the **compute engines** that operates within a lakehouse. Delta provides the table reliability; Spark provides the computation; object storage provides the bytes; a catalog provides metadata. Spark is a component *of* a lakehouse, not the lakehouse itself.

### Why Spark is not "big pandas"

Pandas executes **eagerly**, **in one process**, over data that **fits in memory**, using row/column structures in local RAM. Every operation runs immediately and mutates or returns concrete data.

Spark is **lazy**, **distributed**, and operates over data far larger than one machine's memory. A Spark DataFrame is **not data** — it is a *plan* describing how to produce data. The API resembles pandas superficially (you call `filter`, `groupBy`), but the execution model is entirely different. Code that works in pandas can be catastrophic in Spark precisely because the pandas mental model (everything is local and immediate) is wrong for a distributed lazy engine. The clearest tell: calling `collect()` in Spark tries to pull the *entire distributed dataset* into the driver's memory — the exact opposite of distributed thinking.

### Why Spark is not "Python on many machines"

You might imagine Spark as "my Python function, run on each machine." That is also wrong. When you use the DataFrame/SQL API, your logic is **not executed as Python at all** on the executors. It is translated into a JVM-side logical plan, optimized by Catalyst, and executed by Tungsten as generated JVM code over a binary row format. The Python is a *description* that gets compiled away. This is why built-in DataFrame functions are dramatically faster than Python UDFs: built-ins become optimized JVM operators, while a Python UDF forces Spark to ship each row out to a Python process and back (lessons 02, 07, 08). Spark is not parallel Python; it is a query engine with a Python front end.

### Why Spark is useful for data engineering

Data engineering is fundamentally about moving and transforming large volumes of data reliably between systems and shaping it into usable products. Spark fits because it: scales to arbitrarily large data; expresses complex transformations (joins, aggregations, windows) concisely in SQL or PySpark; integrates with open storage and table formats (Parquet, Delta); supports both batch and streaming with a unified API; and provides fault tolerance so long pipelines survive partial failures. It is the workhorse compute layer of the medallion/lakehouse pattern (lesson 15).

### How Spark fits into modern lakehouse architecture

```text
        Orchestration  (Fabric Pipelines / Airflow)
               │ triggers
               ▼
        ┌─────────────┐      consults/updates     ┌────────────┐
        │   SPARK     │ ◄───────────────────────► │  Metadata  │
        │  (compute)  │                            │  (catalog, │
        └─────────────┘                            │ Delta log) │
           │      ▲                                 └────────────┘
     reads │      │ writes
           ▼      │
        ┌──────────────────────────────────┐
        │   Storage: object store / OneLake │
        │   Parquet data + _delta_log       │
        └──────────────────────────────────┘
```

Spark sits in the middle, computing over storage, coordinating with metadata, triggered by orchestration. Other engines (SQL warehouse, BI) read the *same* storage, which is the whole point of the decoupled lakehouse.

## PySpark and SQL perspective

Consider the canonical example:

```python
df = spark.read.parquet("Files/events")
result = df.groupBy("study_id").count()
result.write.format("delta").saveAsTable("gold_counts")
```

Read this as a sequence of *plan construction* followed by *one execution*, not three computations:

- `spark.read.parquet("Files/events")` does **not** read all the data. It resolves the schema (reading file metadata/footers) and returns a DataFrame that *represents* "scan these Parquet files." No rows are processed.
- `df.groupBy("study_id").count()` does **not** compute counts. It appends an aggregation to the logical plan. Because grouping by `study_id` requires bringing all rows with the same `study_id` together, Spark *knows this will need a shuffle* — but it still hasn't run anything. `result` is a new plan, not numbers.
- `result.write.format("delta").saveAsTable("gold_counts")` is an **action**. *Now* Spark compiles the accumulated plan: Catalyst optimizes it (e.g. pruning to only the `study_id` column since that's all that's needed), produces a physical plan with an exchange (shuffle) for the grouping, breaks it into stages (a scan-and-partial-aggregate stage, then a shuffle, then a final-aggregate-and-write stage), launches tasks across executors, moves data during the shuffle, and writes Parquet files plus a Delta transaction log entry.

The same logic in Spark SQL produces essentially the same plan:

```sql
CREATE TABLE gold_counts USING delta AS
SELECT study_id, COUNT(*) AS cnt
FROM parquet.`Files/events`
GROUP BY study_id;
```

PySpark and SQL are two front ends to the *same* Catalyst engine. They are not two different execution systems — a point developed fully in lesson 02.

## Architectural implications

- Because Spark is decoupled compute, **storage layout is your responsibility and your lever**. File format, file size, partitioning, and table format (Delta) determine how well Spark can read and prune.
- Because Spark is lazy, **the whole pipeline is optimized together**, so writing clear declarative transformations lets the optimizer help you.
- Because Spark is distributed, **data movement (shuffle) is the primary cost**, and avoiding unnecessary movement is the primary optimization.
- Because Spark is "just compute," **reliability of your tables comes from the table format (Delta), not from Spark itself**.

## Common misconceptions

- *"Spark runs my Python on a cluster."* No — DataFrame/SQL logic is compiled to JVM operators; your Python is a plan description.
- *"A DataFrame holds my data."* No — it holds a plan. Data exists only after an action materializes it.
- *"Spark is a database."* No — it is compute over external storage, without OLTP, indexing, or (by itself) transactions.
- *"Spark is just faster pandas."* No — different execution model (lazy, distributed) that breaks the pandas mental model.
- *"More machines always means faster."* No — coordination and shuffle costs, and skew, can dominate; small jobs are slower on Spark, not faster.

## Summary

Spark is a distributed query planning and execution engine: a compiler and scheduler that turns declarative transformations into parallel work over partitioned data in external storage. It is not storage, not a broker, not an ETL designer, not a lakehouse, not pandas, and not parallel Python. It occupies the **compute** role in a four-part architecture (compute, storage, metadata, orchestration). Everything in this course elaborates how your declarative code becomes distributed execution and how to reason about that execution.

## Terms to remember

- **Execution engine** — system that plans and runs computations; Spark's true nature.
- **Declarative API** — you state intent (what), engine decides mechanism (how).
- **Decoupled storage and compute** — Spark computes over storage it does not own.
- **Shuffle** — cross-machine data movement; the dominant distributed cost.
- **Lazy evaluation** — plans build until an action triggers execution.
- **Lakehouse** — architecture combining open storage, table formats, and multiple engines; Spark is one engine within it.
