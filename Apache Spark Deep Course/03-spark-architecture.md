# Spark Architecture

## Purpose of this lesson

This lesson describes the physical and logical structure of a running Spark application: the driver, the executors, the cluster manager, and the storage they operate over. These components are the *machine* on which everything else in the course runs. When you understand exactly what lives on the driver, what lives on executors, and how your notebook code becomes work distributed across a cluster, performance problems and failures stop being mysterious. You will also see why this architecture is *invariant* across Fabric, Databricks, Synapse, EMR, Kubernetes, and standalone Spark — only the cluster manager and provisioning change.

## Core mental model

```text
Notebook / Client
      ↓  (you write code here)
Driver               ← builds plans, schedules tasks, holds application state
      ↓  schedules tasks
Executors            ← run tasks in parallel, hold data partitions
      ↓  read/write
Distributed storage  ← durable data (object store / lakehouse)
```

One **driver** coordinates; many **executors** do the work; **storage** is external and durable. The driver is the brain; executors are the hands; storage is the world they act on.

## Main explanation

### The Spark application

A **Spark application** is one instance of Spark doing work on behalf of a user or job. It consists of exactly one driver process and a set of executor processes, allocated for the lifetime of that application. In a Fabric notebook, attaching to a Spark session creates an application: a driver plus executors provisioned from a pool. When the session ends, the application's processes are released. Everything that happens — every plan, job, stage, task — happens *within* one application's lifetime, and application-scoped state (cached data, temporary views, accumulators) lives only that long.

### The SparkSession

The **SparkSession** is the programmatic entry point to a Spark application. It is the object (in Fabric notebooks, usually pre-created as `spark`) through which you read data, build DataFrames, run SQL, and configure the engine. It encapsulates the underlying `SparkContext` (the lower-level connection to the cluster) and the **catalog** (the registry of databases, tables, and temporary views). Conceptually, the SparkSession is your handle on the driver: when you call `spark.read...`, you are instructing the driver to start building a plan. One application has one SparkSession (you can technically create more, but one is the norm).

### The driver

The **driver** is the process that runs your program — the `main` of the application. It is where your notebook cells actually execute (the Python control flow, the plan-building calls). The driver's responsibilities:

- **Build plans.** It runs Catalyst: parsing, analyzing, optimizing, and producing physical plans.
- **Schedule work.** It breaks plans into jobs, stages, and tasks (the DAG scheduler and task scheduler live here, lesson 04) and decides which executor runs which task.
- **Coordinate.** It tracks task progress, handles failures and retries, and aggregates results.
- **Hold application-level state.** The catalog, broadcast variables, accumulators, and any data you `collect()` back all live in driver memory.

There is **exactly one driver per application**, and it is a single point of coordination. If the driver dies, the whole application dies (discussed under failures below). The driver is *not* where the bulk data lives — it should hold plans and small results, not large datasets.

### Executors

**Executors** are JVM processes running on worker nodes that do the actual data processing. Each executor:

- **Runs tasks.** It executes the task code (the generated Tungsten code for a stage) over the partitions assigned to it. An executor has multiple **slots** (one per core) so it runs several tasks concurrently.
- **Holds data.** It stores partitions in memory (and spills to local disk under pressure), including cached/persisted data.
- **Reports back.** It sends task status and results/metrics to the driver, and serves shuffle data to other executors.

There are **many executors per application**, and they are where parallelism and scale come from. Total parallelism ≈ (number of executors) × (cores per executor) = total slots = number of tasks that can run simultaneously.

### The cluster manager

The **cluster manager** allocates the physical resources (CPU, memory) on which the driver and executors run. It is the layer that says "here are machines/containers; run your processes on them." Spark supports several: **standalone** (Spark's built-in manager), **YARN** (Hadoop), **Kubernetes**, and **Mesos** (legacy). Managed platforms wrap this: Fabric, Databricks, Synapse, and EMR each provide their own provisioning over one of these. The crucial conceptual point: **the cluster manager is pluggable and sits below Spark's core**. Driver, executors, jobs, stages, and tasks are identical regardless of which manager allocated the machines. When you move from Fabric to Databricks, you are changing the cluster manager and provisioning, not the Spark execution model.

### Worker nodes

A **worker node** is a physical or virtual machine in the cluster that hosts executor processes (and possibly the driver). One worker node typically runs one or more executors, each consuming a slice of the node's CPU and memory. The distinction matters for resource reasoning: a node has finite memory and cores; executors are sized to fit within nodes; and data locality (running a task on the node that already holds its data) can reduce network cost.

### Tasks, slots, and cores

A **task** is the smallest unit of execution — the stage's logic applied to one partition. A **slot** (or core) is a unit of parallel execution capacity on an executor. An executor with 4 cores has 4 slots and runs up to 4 tasks at once. The relationship — partitions → tasks → slots → cores — is the heart of Spark parallelism (lesson 10): tasks queue up (one per partition) and execute as slots become free. More cores means more tasks run concurrently; more partitions than slots means tasks run in waves.

### Storage systems

Spark computes over **external, distributed storage** — it is (almost always) **stateless compute over durable storage it does not own**. The storage is object storage (ADLS/OneLake/S3/GCS) holding files (Parquet, etc.) and table formats (Delta). Executors read input partitions from storage at the start and write output partitions to storage at the end; between, intermediate data lives in executor memory/disk and shuffle files. This decoupling is why a cluster can be created, do work, and be torn down without losing data: the data was never *in* the cluster, only passing through it.

## Key concepts

- **One driver, many executors, external storage.**
- **Driver = planning + scheduling + coordination + small results.**
- **Executor = task execution + partition storage + shuffle serving.**
- **Cluster manager = resource allocation, pluggable, below Spark core.**
- **Slots/cores = concurrency; tasks = work units; partitions = data units.**
- **Spark is stateless compute over durable external storage.**

## Detailed technical sections

### How code moves from notebook to driver to executors

```text
1. You run a notebook cell (PySpark/SQL).
2. The DRIVER receives it: Python builds a plan via Py4J / SQL is parsed.
3. On an ACTION, the driver runs Catalyst → physical plan → DAG of stages/tasks.
4. The driver asks the cluster manager for / already has executors.
5. The driver SERIALIZES task code (generated code, closures, broadcast vars)
   and SENDS tasks to executors.
6. Each EXECUTOR reads its input partition(s) from STORAGE, runs the task,
   and (if a shuffle follows) writes shuffle output to local disk.
7. Executors REPORT status/metrics back to the driver.
8. Final-stage executors WRITE results to STORAGE (or send small results
   to the driver for collect()).
```

The pattern to internalize: **logic is defined on the driver, but executed on executors over data they read from storage.** Your Python loop runs on the driver; the per-partition computation runs on executors as compiled code.

### What runs on the driver vs on executors

| Runs on the **driver** | Runs on the **executors** |
|------------------------|---------------------------|
| Your notebook Python control flow | The per-partition data processing (generated Tungsten code) |
| Plan building and Catalyst optimization | Reading input partitions from storage |
| Job/stage/task scheduling | Shuffle write and shuffle read |
| The catalog, broadcast var construction | Holding cached/persisted partitions |
| Results from `collect()` / `take()` | Writing output partitions to storage |
| Final aggregation of small results | Python UDF execution (in a Python worker beside the executor) |

A frequent source of bugs: assuming a Python variable or object defined on the driver is automatically available, mutated consistently, inside a task on an executor. It is not — closures are serialized and copied; mutations on executors do not propagate back (except through the controlled accumulator/broadcast mechanisms).

### Why `collect()` can crash the driver

`collect()` pulls **the entire distributed dataset** from all executors into the **driver's** memory as a local list. The driver has finite memory (often smaller than the total executor memory). If the DataFrame is large, `collect()` attempts to materialize gigabytes into one process and triggers an `OutOfMemoryError` that kills the driver — and with it the whole application. This is the canonical "thinking locally in a distributed engine" mistake. Use `collect()` only for small, bounded results (a handful of aggregates, a small lookup). To inspect data, use `show(n)` or `take(n)` (bounded). To process large data, keep it distributed and `write()` it.

### Why executor memory matters

Executor memory holds: input data being processed, shuffle buffers, cached partitions, and intermediate state for operations like aggregations and joins. When memory is insufficient, Spark **spills** to local disk — correct but much slower — and excessive spilling or genuine exhaustion causes task failures and retries (or executor death). Memory pressure also drives **garbage collection** pauses (the executor is a JVM), which show up as GC time in the UI and slow tasks. Sizing executor memory, and reducing per-task memory demand (by partitioning sensibly, avoiding huge skewed partitions, and not caching needlessly), is core performance work (lessons 10, 11, 17, 20).

### What distributed storage means here

The data Spark reads and writes lives across many storage nodes/objects, accessed over the network, addressed by paths/tables. It is **durable** (survives cluster teardown), **shared** (multiple engines can read it), and **partitioned into files**. Spark reads these files as input partitions and writes output as files. Because storage is separate, the same Delta table can be the output of one Spark job and the input of another, or read by a SQL warehouse — the decoupling from lesson 01 made concrete.

### Why Spark is usually stateless compute over external storage

Spark applications generally hold no durable state of their own: their inputs come from storage, their outputs go to storage, and their internal state (partitions, shuffle files, caches) is transient and tied to the application's life. This is a deliberate design that enables elasticity (scale executors up/down), fault tolerance (recompute lost partitions from lineage and re-read from durable storage), and the lakehouse pattern (many ephemeral compute jobs over one persistent data layer). "Stateless" does not mean Spark has *no* state during a run — it has lots (in memory) — but that state is recoverable/derivable and not the system of record.

## How Fabric notebooks abstract cluster management

In a Microsoft Fabric notebook, you do not provision machines, install Spark, or choose a YARN/Kubernetes manager. You attach to a **managed Spark pool**, and Fabric supplies a pre-created `SparkSession` (`spark`), a driver, and executors drawn from the pool, with the Lakehouse mounted so storage paths resolve to OneLake. Fabric abstracts: cluster provisioning, the cluster manager, runtime installation, and storage mounting. What Fabric does **not** abstract: the driver/executor model, jobs/stages/tasks, shuffles, partitioning, memory limits, and the consequences of `collect()` or skew. In other words, Fabric removes the *operations* of running a cluster but leaves the *architecture* fully intact — which is exactly why this lesson is cloud-agnostic. The same reasoning applies whether you are on Fabric, Databricks, Synapse, EMR, Kubernetes, or a laptop standalone cluster; only the box that hands you machines changes.

## PySpark and SQL perspective

```python
spark            # the SparkSession: your handle on the driver
df = spark.read.parquet("Files/events")   # driver builds a scan plan; no executors yet
df.groupBy("study_id").count().write.format("delta").saveAsTable("g")
```

The `spark` object is the driver-side entry point. `spark.read...` and `.groupBy(...)` run **on the driver**, building a plan — no executor is involved and no data moves. Only `.write...` (an action) makes the driver schedule tasks onto executors, which read `Files/events` from storage, perform the grouped aggregation (with a shuffle), and write Delta files back to storage. If you had instead written `rows = df.collect()`, the driver would try to gather every row into its own memory — the dangerous path. In SQL, `spark.sql("SELECT ...")` behaves identically: the driver plans, executors execute on the action.

## Architectural implications

- **Keep the driver light.** Build plans and collect only small results there. Large data stays distributed.
- **Parallelism is bounded by total executor cores.** Provisioning fewer cores than partitions means tasks run in waves; fewer partitions than cores wastes capacity (lesson 10).
- **Memory is per executor.** Per-task memory demand (partition size, skew, joins, caching) must fit, or you spill or fail.
- **Storage layout is a first-class lever**, because executors' first and last acts are reading/writing storage.
- **Portability is real.** Designing against the driver/executor/storage model means your reasoning transfers across platforms.

## Common misconceptions

- *"The driver does the heavy lifting."* No — executors do; the driver coordinates.
- *"`collect()` is how you get your data."* Only for tiny results; otherwise it crashes the driver.
- *"Fabric is a different Spark."* No — same architecture; Fabric only manages provisioning and storage mounting.
- *"A variable defined in my notebook is shared with executors."* No — closures are serialized/copied; mutations don't flow back.
- *"More executors always means faster."* Only up to the parallelism your partition count and workload allow, and not if skew concentrates work on one task.

## Detailed explanation of driver and executor failure implications

Failure behavior reveals the architecture's structure:

- **Executor failure.** If an executor dies (crash, OOM, node loss), the tasks it was running fail, and any *non-replicated* data it held (cached partitions, shuffle output) is lost. Spark's response is **recompute and retry**: it reschedules the failed tasks on other executors, recomputing lost partitions from **lineage** (the recorded transformations, lesson 18) and re-reading source data from durable storage. The application survives. If shuffle files an executor produced are needed by a later stage and that executor is gone, Spark may have to re-run the *map-side* stage that produced them. So executor failure is **recoverable but potentially costly** (recomputation).

- **Driver failure.** The driver is the single coordinator holding all scheduling state, the DAG, and the catalog. If the driver dies, there is no one to schedule or track work, so **the entire application dies**. Any in-progress jobs are lost; uncommitted results are gone. Recovery means restarting the application (and, for streaming, recovering from a **checkpoint**, lessons 16–18). This is why the driver is a critical single point of failure and why you keep large data off it (a driver OOM from `collect()` is, in effect, a self-inflicted driver failure). In managed environments, the platform may restart the driver/session, but the running work is not transparently rescued the way executor work is.

The asymmetry — executors are fungible and recoverable, the driver is singular and fatal — is a direct consequence of the one-coordinator/many-workers design and should shape how you write jobs: never concentrate data or critical state where its loss kills everything.

## Summary

A Spark application is one driver plus many executors, allocated by a pluggable cluster manager, computing over external durable storage. The driver builds plans, schedules tasks, and holds small results and coordination state; executors run tasks over partitions, hold data, and serve shuffles; storage is decoupled and durable. Code is *defined* on the driver and *executed* on executors. `collect()` endangers the driver; executor memory governs spills and failures. This architecture is invariant across platforms — Fabric only abstracts provisioning and storage mounting — and its failure asymmetry (recoverable executors, fatal driver) follows directly from the one-coordinator design.

## Terms to remember

- **Spark application** — one driver + its executors, for one session's lifetime.
- **SparkSession** — programmatic entry point; your handle on the driver and catalog.
- **Driver** — single coordinator: plans, schedules, holds small state.
- **Executor** — worker process: runs tasks, holds partitions, serves shuffle.
- **Cluster manager** — pluggable resource allocator below Spark core.
- **Slot / core** — unit of task concurrency on an executor.
- **Stateless compute** — Spark computes over durable storage it does not own.
