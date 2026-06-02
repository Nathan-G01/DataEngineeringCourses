# Fault Tolerance and Lineage

## Purpose of this lesson

At scale, failure is normal: executors die, nodes vanish, networks drop, tasks fail. A distributed engine that couldn't survive partial failure would be useless. Spark's fault tolerance is built on **lineage** — the recorded recipe to recompute any lost data — plus task retries, and (for state) checkpointing. But Spark's compute fault tolerance is only half the story: producing *reliable, correct, non-duplicated output* also depends on the source's replayability, the sink's transactionality, idempotent writes, and deterministic transformations. This lesson explains Spark's recovery model, the failure scenarios (task, executor, shuffle-file, driver), the difference between compute fault tolerance and storage/table reliability, exactly-once vs at-least-once *in practice*, and why determinism and side-effect-free tasks matter. It ties together lineage (05), architecture/failures (03), Delta (14), and streaming (16).

## Core mental model

```text
Spark survives failure by RECOMPUTING lost data from LINEAGE (the recorded transformation chain),
re-reading source data from DURABLE storage, and RETRYING failed tasks.

Compute fault tolerance (Spark):     recover lost partitions/tasks via lineage + retry.
Storage/table reliability (Delta):   atomic, non-corrupting, exactly-once-capable WRITES.

End-to-end reliability = deterministic transforms + replayable source + Spark recovery + transactional sink.
```

Spark guarantees it can *recompute*; reliability of the *result* additionally requires determinism, replayable input, and transactional output.

## Main explanation

### Spark's fault tolerance model

Spark's core fault-tolerance idea is **recomputation from lineage** rather than data replication. Every DataFrame/RDD records the deterministic sequence of operations that produced it (its lineage, lesson 05). If any partition is lost — because the executor holding it failed — Spark doesn't need a backup copy; it **recomputes** that partition by replaying its lineage from a recoverable starting point (the durable source data, or a checkpoint/shuffle file). Because source data lives in durable external storage (lesson 03) and transformations are deterministic, recomputation reproduces exactly the lost partition. This is elegant: fault tolerance is a *property of the plan*, not an expensive replication scheme. The cost is that recovery means *redoing work*, which can be significant for long lineages (mitigated by checkpointing, lesson 17).

### Lineage as the recovery mechanism

Lineage is both the optimization input (lesson 07) and the recovery recipe. When a partition is lost, the DAG scheduler (lesson 04) identifies which transformations must be re-run to regenerate it and reschedules those tasks. If the lost partition's inputs are still available (in shuffle files or cache), recomputation starts there; if not, it traces further back, potentially to the source. The further back it must go, the more expensive recovery is — which is exactly why **checkpointing truncates lineage** (lesson 17) to bound recovery cost for long/iterative plans. Lineage is the thread that makes Spark resilient without copying data everywhere.

### Task retry

The finest-grained recovery: when an individual **task fails** (an exception, a transient I/O error, an executor hiccup), the task scheduler simply **retries** it — re-running that task (which processes one partition) on an available executor, up to a configurable maximum attempts. Because a task is a deterministic computation over one partition derived from recoverable inputs, re-running it reproduces the correct result. Most transient failures are absorbed silently this way; you see them only as retried tasks in the Spark UI. Only when a task fails repeatedly (exceeding max attempts) does the stage — and the job — fail. Task retry is the everyday mechanism that makes large jobs survive the constant low-level flakiness of big clusters.

### Speculative execution (conceptually)

Distinct from failure recovery, **speculative execution** addresses *slow* (not failed) tasks. If a task runs much longer than its peers (a straggler — possibly due to a slow node, not skew), Spark can launch a **duplicate** copy of that task on another executor and take whichever finishes first, killing the other. This guards against individual slow nodes dragging out a stage. Note it does **not** fix data skew (lesson 11): a skewed task is slow because it has *more data*, so a duplicate would be equally slow — speculation helps with *infrastructure* stragglers, not *data* stragglers. Conceptually, speculation is a latency optimization layered on the retry/recovery machinery, and it requires tasks to be idempotent/deterministic so running two copies is safe.

## Key concepts

- **Lineage** — recorded transformation chain; the recipe to recompute lost data.
- **Task retry** — re-running a failed task on another executor.
- **Speculative execution** — duplicating slow (straggler) tasks; for infra slowness, not skew.
- **Shuffle file loss** — losing intermediate shuffle output, forcing map-side re-run.
- **Idempotent write** — a write that produces the same result if repeated (no duplicates).
- **Exactly-once vs at-least-once** — each record affects output once vs possibly more (on retry).
- **Determinism** — same input always yields same output; required for safe recomputation.

## Detailed technical sections

### Executor failure

When an **executor** dies (crash, OOM, node loss, preemption), Spark loses: the tasks it was running and any data it held — cached partitions and **shuffle files** it produced. Recovery (lesson 03): the failed in-flight tasks are **retried** on other executors; lost **cached** partitions are recomputed from lineage when next needed; and lost **shuffle output** triggers re-running the map-side tasks that produced it (below). The application **survives** — executors are fungible. The cost is the recomputation. This is why Spark scales reliably: any single executor can vanish and the work is reconstructed elsewhere from the durable source plus lineage.

### Shuffle file loss

A subtle and important case. A shuffle's map side writes intermediate **shuffle files** to executors' **local disk** (lesson 09), to be fetched by the reduce side. These files are *not* in durable storage — they live on the executor that produced them. If that executor dies *after* writing shuffle files but *before* (or while) the reduce side has consumed them, those shuffle files are **lost**, and the reduce tasks that needed them cannot proceed. Spark recovers by **re-running the map-side tasks** that produced the lost shuffle output (regenerating the files), then resuming the reduce side. This means executor loss during a shuffle can cascade into re-executing an earlier stage — a more expensive recovery than a simple task retry. (External shuffle services and shuffle-tracking mitigate this by decoupling shuffle files from executor lifetime, but the conceptual risk — shuffle output is ephemeral — remains.)

### Driver failure

The asymmetric, fatal case (lesson 03). The **driver** holds all scheduling state, the DAG, and coordination; there is exactly one. If it dies, there is no coordinator, so **the entire application dies** — in-progress jobs are lost, and uncommitted results are gone. There's no recomputing your way out of a driver loss the way you can for executors. Recovery means **restarting the application**. For **batch**, that's re-running the job (which, if idempotent, safely reproduces output). For **streaming**, restart recovers from the **checkpoint** (lesson 16): the durable record of offsets and state lets the restarted query resume exactly where it left off. This asymmetry — executors recoverable, driver fatal — is why you keep large data and critical state off the driver (a `collect()` OOM is a self-inflicted driver failure).

### Data source reliability and replayability

Spark's recomputation assumes it can **re-read the source data**. This requires a **reliable, replayable source**: durable storage (object store, Delta) that returns the same data when re-read, or a streaming source that supports re-reading from a known offset (files, Delta, Kafka). If the source is *not* replayable — e.g., a transient queue that discards messages once read, or a non-deterministic source — then recomputation can't faithfully reproduce lost data, and you can't guarantee correctness or exactly-once. Source replayability is a precondition for Spark's fault tolerance to actually deliver correct results, not just survive crashes. This is why the choice of source matters for reliability, not only performance.

### Sink reliability and idempotent writes

The output side. When a task that *writes* fails and retries, or when a whole job is re-run after failure, naive writes can produce **duplicates** — the retry writes the same output again. **Sink reliability** depends on the write being **idempotent** or **transactional**:

- **Idempotent write** — repeating it yields the same final state (no duplicates). Achieved by, e.g., **MERGE/upsert keyed on a business key** (lesson 14), or **overwriting a partition** rather than appending, so re-processing the same input updates-in-place.
- **Transactional sink** — the write commits atomically (all-or-nothing), so a failed write leaves no partial/duplicate output. **Delta** is transactional (lesson 14): a failed write commits nothing; a retried write doesn't duplicate because the commit is atomic and (for streaming) offset-aware.

Without sink idempotency/transactionality, Spark's compute fault tolerance can still produce *incorrect output* (duplicates) on retry — which is why output design is part of fault tolerance, not separate from it.

### Exactly-once vs at-least-once in practice

The crucial real-world distinction:

- **At-least-once** — every record is processed *and may be written more than once* (on retry/recovery). This is the *default* for naive append-style outputs: failures cause re-processing, and without idempotency you get duplicates. It's "no data loss, but possible duplicates."
- **Exactly-once** — every record affects the output *exactly once*, despite failures. As established in lesson 16, this is achievable only with the **combination** of: a **replayable source** (re-read from offset), **checkpointed progress** (know what's been done), and a **transactional/idempotent sink** (don't duplicate). **Delta sinks** provide the transactional piece, which is why Delta + Structured Streaming achieves true exactly-once into the lakehouse.

The practical lesson: **exactly-once is a property of the entire pipeline, not of Spark alone.** Spark gives you at-least-once compute recovery for free; turning that into exactly-once *output* requires replayable input + checkpointing + transactional sink. If any link is missing, you have at-least-once, and you must design for duplicate-tolerance (idempotent downstream) or accept duplicates.

### How Delta helps with transactional writes

Delta (lesson 14) is the linchpin of output reliability in the lakehouse. Its **atomic commits** mean a failed/retried write never leaves partial or duplicated data — either the commit (the log entry) succeeds and all its files become visible, or it doesn't and nothing changes. Its **MERGE** enables idempotent upserts. For **streaming**, Delta tracks offsets so a restarted query writes each micro-batch exactly once. So Delta converts Spark's at-least-once compute into exactly-once *persistence*: the recompute-and-retry that fault tolerance requires becomes safe because the writes are transactional. This is a primary reason the lakehouse uses Delta for its tables rather than plain Parquet (which offers none of these guarantees).

### Compute fault tolerance vs storage/table reliability

A clean conceptual split that prevents confusion:

- **Compute fault tolerance** (Spark): the ability to *survive failures during processing* and *recompute lost intermediate data* — task retry, lineage recomputation, executor recovery, shuffle re-execution. This keeps the *job* alive.
- **Storage/table reliability** (Delta + durable storage): the ability to *persist results correctly and atomically* — ACID writes, no corruption, exactly-once-capable output, durable, time-traveled. This keeps the *data* correct.

They're orthogonal and *both* required. Spark can faithfully recompute (compute FT) yet still corrupt a table if writes aren't transactional (no storage reliability); conversely, Delta's transactional writes don't help if the computation itself can't recover. Reliable pipelines compose both: Spark's recovery to finish the work, Delta's transactions to land it correctly.

### Why deterministic transformations matter

Spark's entire recovery model assumes **determinism**: re-running a transformation on the same input yields the *same* output. If a transformation is **non-deterministic** — depends on wall-clock time, random numbers without a fixed seed, mutable external state, or ordering that isn't guaranteed — then a recomputed partition may differ from the original, producing **inconsistent or incorrect results** when only *some* partitions are recomputed after a failure. For example, assigning IDs with a non-seeded random function means a retried task generates different IDs than the original attempt, corrupting downstream joins. Determinism is therefore not just good practice; it's a *correctness requirement* under Spark's recompute-on-failure model. Where you need values like timestamps or random IDs, fix them deterministically (capture once, seed RNGs) so recomputation reproduces them.

### Why side effects inside Spark tasks are dangerous

A related hazard. Tasks can be **retried, speculatively duplicated, or recomputed** — so a task may run **more than once**. If a task performs an **external side effect** (calling an API, sending a message, writing to an external non-idempotent system, incrementing a counter), that side effect can happen multiple times: a retried task sends the email twice, calls the payment API twice, double-writes to a downstream service. Spark's data outputs are made safe by transactional sinks, but *arbitrary side effects inside task code are not protected.* The rules: **keep tasks pure** (transform data, don't perform external side effects); push any necessary side effects to **idempotent, transactional, controlled boundaries** (a Delta write, an upsert) rather than ad-hoc calls inside `map`/`foreach`/UDFs; and if you must do external writes, make them idempotent. This is why `foreach`-ing external calls across a DataFrame is risky, and why output should flow through transactional sinks.

## Failure scenarios (worked)

```text
1. Transient task error          → task retried on another executor → succeeds (invisible-ish).
2. Executor OOM mid-stage        → its tasks retried; its cached partitions recomputed from lineage;
                                    its shuffle files (if any) regenerated by re-running map tasks.
3. Node lost after shuffle write → shuffle files gone → DAG scheduler re-runs the map-side stage
                                    to regenerate them, then resumes the reduce side.
4. Driver dies                   → whole application dies → restart job (batch: re-run idempotently;
                                    streaming: resume from checkpoint).
5. Job retried after failure with naive append sink → DUPLICATES (at-least-once).
   Same job with Delta MERGE/transactional sink     → no duplicates (exactly-once-capable).
6. Non-deterministic UDF (unseeded random) + partial recompute → inconsistent values → corruption.
```

## PySpark and SQL perspective

```python
# UNSAFE: non-deterministic + side effect inside a task
df.withColumn("id", F.expr("uuid()")) \      # non-deterministic: recompute → different ids
  .foreach(lambda row: call_external_api(row))  # side effect runs again on retry → duplicates

# SAFER: deterministic derivation + transactional, idempotent sink
clean = df.withColumn("load_date", F.lit(run_date))   # fixed, deterministic
# MERGE into Delta on a business key → idempotent, transactional (exactly-once-capable)
target.merge(clean.alias("s"), "t.event_id = s.event_id") \
      .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

The unsafe version breaks both rules: `uuid()` is non-deterministic (a recomputed partition gets different ids, corrupting any downstream join on `id`), and `foreach(call_external_api)` performs an external side effect that re-fires on task retry/speculation/recompute, causing duplicate API calls. The safer version uses a deterministic `load_date` (captured once, so recomputation reproduces it) and routes output through a Delta **MERGE** keyed on `event_id` — idempotent and transactional, so retries and re-runs don't duplicate, yielding exactly-once persistence. In SQL, the same principles hold: avoid non-deterministic expressions in columns that must survive recomputation consistently, and write via transactional `MERGE INTO ... ` rather than blind `INSERT` appends when idempotency matters.

## Architectural implications

- **Rely on lineage + retry for compute recovery; checkpoint to bound long-lineage recovery** (lesson 17).
- **Keep large data and critical state off the driver** — driver failure is fatal and unrecoverable mid-job.
- **Use replayable sources + transactional Delta sinks** to convert at-least-once compute into exactly-once output.
- **Make all writes idempotent** (MERGE/partition-overwrite) so retries and re-runs are safe (lesson 15).
- **Keep transformations deterministic** — fix timestamps/seeds; non-determinism corrupts under partial recompute.
- **Never perform external side effects inside tasks** — push them to idempotent, transactional boundaries.

## Common misconceptions

- *"Spark replicates data for fault tolerance."* No — it recomputes from lineage (replication is optional, e.g. `_2` cache levels).
- *"If Spark survives the failure, my output is correct."* Not necessarily — naive sinks duplicate on retry (at-least-once).
- *"Exactly-once is a Spark setting."* No — it's a whole-pipeline property: replayable source + checkpoint + transactional sink.
- *"Speculative execution fixes skew."* No — it fixes infra stragglers; a skewed task's duplicate is equally slow.
- *"A `foreach` external call is fine."* It can run multiple times (retry/speculation/recompute) — duplicate side effects.
- *"Non-deterministic functions are just a quality nuance."* They cause real corruption when partitions are partially recomputed.

## Summary

Spark achieves fault tolerance through **lineage-based recomputation** plus **task retry**, not data replication: lost partitions are regenerated by replaying their deterministic transformation chain from durable sources or shuffle files. Executor failures are recoverable (tasks retried, data recomputed, shuffle files regenerated by re-running map stages); driver failure is fatal and requires restart (batch re-run, or streaming resume from checkpoint). But Spark's compute fault tolerance only guarantees the *job* can recover and recompute — it yields **at-least-once** by default. Producing **exactly-once, correct output** additionally requires a **replayable source**, **checkpointed progress**, **deterministic transformations**, and a **transactional/idempotent sink** (Delta). Compute fault tolerance (Spark) and storage/table reliability (Delta) are orthogonal and both necessary. Determinism is a correctness requirement under partial recomputation, and external side effects inside tasks are dangerous because tasks can run multiple times — push side effects to idempotent, transactional boundaries.

## Terms to remember

- **Lineage** — recorded transformation chain; the recompute recipe.
- **Task retry** — re-running a failed task; the everyday recovery mechanism.
- **Shuffle file loss** — ephemeral shuffle output loss forcing map-stage re-execution.
- **Idempotent / transactional write** — repeatable/atomic writes that avoid duplicates (MERGE, Delta commit).
- **Exactly-once vs at-least-once** — once vs possibly-duplicated output; the former needs source+checkpoint+sink.
- **Determinism** — same input → same output; required for safe recomputation.
- **Side-effect-free task** — task that transforms data without external effects; safe under retry/speculation.
