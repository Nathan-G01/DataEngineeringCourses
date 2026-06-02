# Architectural Thinking for Spark Jobs

## Purpose of this lesson

The previous lessons each isolated one part of Spark: the execution model, Catalyst, shuffles, joins, Delta, observability. This lesson does the opposite. It is about *judgment* — the habit of mind that takes a vague requirement ("load yesterday's encounters and produce a daily department summary") and turns it into a job that is fast, correct, cheap, and maintainable, by reasoning from the execution model rather than guessing.

This is the lesson that separates someone who *uses* Spark from someone who *designs for* Spark. The difference is not knowing more APIs. It is asking the right questions *before* writing code, recognizing the anti-patterns that quietly destroy performance, and running a deliberate checklist over a design before it ships. None of this requires new Spark concepts; it requires applying the ones you already have, in the right order, at the right time.

There is no exercise here — this is study material. But the questions and checklists below are meant to become an internal monologue you run every time you open a notebook.

## Core mental model

**Every architectural decision about a Spark job is ultimately a decision about where data moves, how much of it moves, and how evenly the work divides — because those three things dominate cost. Good design minimizes and balances data movement; bad design ignores it.**

```
The whole course collapses into three questions:
  1. How much data crosses the network?      (shuffles, joins, broadcasts — lessons 09, 12)
  2. How evenly is the work divided?           (partitioning, skew — lessons 10, 11)
  3. How much work is repeated or wasted?      (laziness, caching, idempotency — lessons 05, 17, 15)
```

Hold those three questions in mind and the recurring pipeline — `User code → logical plan → optimized plan → physical plan → jobs → stages → tasks → partitions → executors → storage` — becomes a tool for prediction: you can trace a design through it and foresee where it will hurt.

## Main explanation

Architectural thinking in Spark is *predictive reading of the plan in your head*. Before you run anything, you mentally compile your intended logic into the stage/shuffle structure it will produce, and you ask whether that structure is the cheapest correct one. A senior engineer looking at `df.groupBy("patient_id").agg(...).join(dim, "patient_id")` does not see two convenient method calls; they see a shuffle for the aggregation, a second shuffle (or a broadcast) for the join, a question about whether `patient_id` is skewed, and a question about whether `dim` is small enough to broadcast. That mental compilation is the skill.

The reason this matters is that Spark's optimizer (lesson 07) and AQE (lesson 19) are powerful but bounded. They reorder and rewrite *within* the logic you give them; they cannot redesign a fundamentally wasteful approach. If you read a table twice because you didn't cache, join on a skewed key because you didn't salt, or write ten thousand tiny files because you over-partitioned, no optimizer rescues you. The architecture — the choices the optimizer *can't* make — is where the leverage is. The questions and anti-patterns in this lesson are organized around exactly those un-optimizable decisions.

## Key concepts

- **Predictive plan reading:** mentally compiling logic into stages and shuffles before running.
- **Minimize movement:** the single most reliable lever; every shuffle avoided is a network round-trip saved.
- **Balance the work:** an unbalanced job is gated by its slowest task regardless of cluster size.
- **Don't repeat work:** laziness means uncached reuse recomputes; design for single passes and idempotent reruns.
- **Design at boundaries:** the expensive decisions are at read, shuffle, join, and write — focus attention there.
- **The optimizer is not a substitute for architecture:** it optimizes within your design, not your design itself.

## Detailed technical sections

### The design questions to ask before writing a job

These are the questions to run, roughly in order, when given a new pipeline requirement. Each maps to a lesson where the reasoning lives.

**About the data and the goal**

1. **What is the grain of the output?** One row per what? This determines your final aggregation and dedup logic and reveals whether a join will fan out (lesson 12).
2. **How big are the inputs, roughly, and how do they grow?** Megabytes, gigabytes, terabytes? Static or growing daily? This sets expectations for partition counts and whether broadcast joins are viable (lessons 10, 12).
3. **Is this a full reprocess or incremental?** Will the job process all history every run, or only new/changed data? Incremental design changes everything downstream (lesson 15).
4. **What is the freshness requirement?** Batch nightly, hourly, or streaming? This decides batch vs structured streaming (lesson 16) — and most "real-time" requirements are satisfied by frequent batches.

**About data movement**

5. **Which operations will shuffle?** Enumerate the wide operations (joins, groupBy, distinct, window, repartition). Each is a stage boundary and a network round-trip (lesson 09).
6. **For each join, which strategy do I expect — broadcast or sort-merge?** Is one side small enough to broadcast? Are statistics good enough for the optimizer to know (lessons 07, 12)?
7. **Are any join/group keys likely skewed?** Nulls, default values, a few dominant entities? If so, plan mitigation before it bites (lesson 11).
8. **Can I filter and project early?** Push selectivity as close to the read as possible so less data enters the shuffles (lessons 07, 13).

**About partitioning and layout**

9. **How many partitions will each stage have, and is that sensible for the cluster?** Too few starves cores; too many adds overhead (lesson 10).
10. **How should the *output table* be partitioned (if at all)?** Only on low-cardinality columns that match query filters; never on high-cardinality keys (lessons 10, 14).
11. **Will this write produce small files?** If the job emits many small partitions, plan compaction (`OPTIMIZE`) or `coalesce`/`repartition` before write (lessons 13, 14).

**About correctness and reliability**

12. **Is the job idempotent — safe to rerun?** Does a partial-then-retried run produce duplicates or corruption? Use Delta `MERGE`/overwrite-by-partition for replayability (lessons 14, 15, 18).
13. **Are the tasks side-effect-free?** No external writes or non-deterministic logic that breaks under retry/speculation (lesson 18).
14. **What quality gates assert the output is right?** Row counts, null rates, schema conformance, uniqueness of the grain key (lessons 15, 20).
15. **Where does this fit in the medallion architecture?** Bronze (raw), Silver (cleaned/conformed), or Gold (aggregated/serving)? Don't collapse layers (lesson 15).

**About cost and observability**

16. **Is any DataFrame reused across multiple actions?** If so, cache it — otherwise its lineage recomputes each time (lessons 05, 17).
17. **How will I know this job is healthy in production?** What logs, metrics, and data-quality checks make "it ran" into "it ran correctly and efficiently" (lesson 20)?

Running these seventeen questions over a design takes minutes and prevents the large majority of slow, wrong, or expensive jobs. The point is not ritual; it is that each question targets a decision the optimizer *cannot* make for you.

### The anti-patterns to recognize and avoid

These are the recurring mistakes that destroy Spark job performance or correctness. Each has appeared in earlier lessons; here they are collected as a checklist of things to *not* do.

**Data-movement anti-patterns**

- **Joining on a skewed key without mitigation.** A few hot keys create straggler tasks that gate the whole stage; the cluster sits idle waiting (lesson 11). Salt, broadcast, or rely on AQE skew handling.
- **Sort-merge joining a small table that should be broadcast.** A needless shuffle of both sides when broadcasting the small one would avoid it. Check the broadcast threshold and statistics (lesson 12).
- **Shuffling more than necessary by not filtering early.** Carrying columns and rows through a shuffle that a pre-shuffle filter would have eliminated (lessons 07, 13).
- **Repartitioning when coalesce would do (or vice versa).** `repartition` forces a full shuffle to increase or rebalance partitions; `coalesce` reduces partitions without a shuffle but can't increase them or fix skew. Using the wrong one either over-shuffles or under-parallelizes (lesson 10).
- **Accidental Cartesian / many-to-many joins.** A missing or non-unique join key explodes row counts and memory (lesson 12).

**Partitioning and layout anti-patterns**

- **Partitioning a Delta table on a high-cardinality column** (e.g., `patient_id`, timestamp to the second). Produces an explosion of tiny directories and files, crippling reads and metadata (lessons 10, 14).
- **The small-file problem.** Streaming or frequent small writes, or over-partitioned outputs, create thousands of tiny files; reads pay per-file overhead (lessons 13, 14). Compact with `OPTIMIZE`.
- **Leaving shuffle partitions at the default 200 regardless of data size.** Fine for medium data; wasteful for tiny data and insufficient for huge data. AQE coalescing helps but doesn't excuse ignoring it (lessons 09, 19).

**Computation and correctness anti-patterns**

- **`collect()` / `toPandas()` on large data.** Pulls the whole dataset to the driver, which has no parallelism and limited memory — a classic driver crash (lesson 03).
- **Reusing an uncached DataFrame across multiple actions.** Each action recomputes the full lineage from source (lessons 05, 17). Cache the reused intermediate.
- **Caching everything "just in case."** Cache consumes executor memory and can cause spill/eviction that slows the job. Cache only genuinely reused, expensive intermediates (lesson 17).
- **Python UDFs for logic that built-in functions can express.** Each row crosses the JVM↔Python boundary, defeating Tungsten and Catalyst (lessons 02, 08, 20). Prefer built-ins or vectorized UDFs.
- **Non-idempotent writes (append without dedup, side effects in tasks).** Retries and reruns produce duplicates or partial corruption (lessons 15, 18). Design for replayability.
- **Collapsing medallion layers.** Doing raw ingestion, cleaning, and aggregation in one opaque job destroys replayability, auditability, and incremental processing (lesson 15).
- **Treating job success as correctness.** A green run can write zero rows, double rows, or drop a malformed file. Without quality gates you won't know (lessons 15, 20).

**Mindset anti-patterns**

- **Tuning by changing configs at random.** Without reading the Spark UI to form a hypothesis, config changes are superstition (lesson 20).
- **Assuming the managed platform handles architecture.** Fabric/Databricks manage infrastructure, not your design (lesson 21).
- **Treating Spark as "big pandas."** Thinking row-by-row and locally rather than in terms of distributed plans, shuffles, and partitions (lesson 01).

### The final architecture checklist

A compact pre-ship checklist that folds the questions and anti-patterns into a single pass:

1. **Grain & output contract** — output grain defined; schema and uniqueness key explicit.
2. **Inputs sized** — approximate input sizes known; growth understood; incremental vs full decided.
3. **Shuffles enumerated** — every wide operation identified and justified; early filtering/projection in place.
4. **Joins planned** — strategy (broadcast vs sort-merge) anticipated for each; skew keys identified and mitigated; no accidental Cartesian.
5. **Partitioning sane** — stage partition counts reasonable for the cluster; output partitioned only on low-cardinality filter columns; no small-file explosion (compaction planned).
6. **Reuse handled** — reused intermediates cached deliberately; nothing cached needlessly.
7. **Driver safe** — no `collect()`/`toPandas()` on large data.
8. **Idempotent & side-effect-free** — reruns safe; tasks deterministic; Delta MERGE/overwrite-by-partition where needed.
9. **Medallion placement** — layer (Bronze/Silver/Gold) clear; layers not collapsed.
10. **Observable** — logging, metrics, and data-quality gates assert correctness and efficiency, not just completion.

If a design passes all ten, it is very likely to be fast, correct, cheap, and maintainable. If it fails one, that failure is almost always where the eventual incident will come from.

## PySpark and SQL perspective

Architectural thinking is API-agnostic because PySpark and Spark SQL compile to the same Catalyst plan (lesson 02). Whether you express a join in `df.join(...)` or in a SQL `JOIN`, the design questions — strategy, skew, cardinality — are identical, because the *plan* is identical. This is liberating: choose the surface that makes the logic clearest (often SQL for set-based transformations, PySpark for procedural flow and parameterization), and reason about architecture at the plan level regardless.

The one architectural seam where the API genuinely matters is the **Python UDF boundary** (lessons 02, 08): a Python UDF is an architectural decision, not a stylistic one, because it forces a JVM↔Python crossing per row and blinds Catalyst to the logic inside. "Can I express this with built-in functions instead of a UDF?" belongs on the design checklist precisely because it is one of the few API choices with first-order performance consequences.

The recommended habit is to write the logic, call `df.explain("formatted")` (lesson 07), and read the plan *as a design review*: count the `Exchange` nodes (shuffles), check the join operators (broadcast vs sort-merge), and confirm filters pushed to the scan. The plan is the architecture made concrete; reviewing it before running is the cheapest possible verification.

## Architectural implications

The meta-implication of this lesson is that **Spark performance is designed, not tuned.** The large wins come from the architectural decisions made before the first run — how data moves, how work divides, what gets repeated — not from config tweaks afterward. Tuning (adjusting shuffle partitions, broadcast thresholds, memory) is real but second-order; it optimizes a sound design and cannot rescue an unsound one. An engineer who internalizes the design questions front-loads the thinking and rarely needs heroic tuning later.

A second implication is that **the execution model is the unifying abstraction.** Every anti-pattern in this lesson is a violation of the model: ignoring shuffles, ignoring skew, overloading the driver, breaking determinism. Once the model is internalized, the anti-patterns become self-evidently wrong rather than rules to memorize — you avoid a skewed-key join not because a checklist says so but because you can *see* the straggler task in your mind's eye. The checklist is training wheels for that intuition.

Finally, architectural thinking is what makes Spark knowledge *transferable and durable* (the theme that carries into lesson 23). APIs, platform features, and defaults change; the reasoning — minimize movement, balance work, avoid waste, design at boundaries, verify correctness — does not. An engineer who reasons from the model can sit down in front of any Spark deployment, on any cloud, in any year, and design well.

## Common misconceptions

- **"Performance is something you tune after the fact."** The large wins are architectural and made before running; tuning is second-order.
- **"The optimizer will fix a bad design."** Catalyst and AQE optimize *within* your logic; they cannot redesign a wasteful approach.
- **"More compute solves slow jobs."** Skew and serial bottlenecks ignore cluster size; uneven work is gated by the slowest task.
- **"If it ran, the design was fine."** Success hides inefficiency and silent incorrectness; only quality gates and UI reading reveal them.
- **"SQL vs PySpark is an architectural choice."** For equivalent logic they're the same plan; the real API-level architectural seam is the Python UDF boundary.
- **"Checklists are bureaucracy."** The questions target exactly the decisions the optimizer can't make — they are where the leverage is.

## Summary

Architectural thinking for Spark is the habit of predicting the plan in your head and designing for the three things that dominate cost: how much data moves, how evenly work divides, and how much work is wasted. Before writing a job, run the design questions — about grain, input size, incrementality, freshness, shuffles, join strategy, skew, early filtering, partition counts, output layout, small files, idempotency, determinism, quality gates, medallion placement, reuse/caching, and observability. While designing, avoid the recurring anti-patterns: skewed-key joins, needless sort-merge over broadcast, late filtering, wrong repartition/coalesce, Cartesian joins, high-cardinality partitioning, small files, default shuffle partitions used blindly, `collect()` on big data, uncached reuse, over-caching, Python UDFs for built-in logic, non-idempotent writes, collapsed medallion layers, treating success as correctness, random-config tuning, and "big pandas" thinking. Then run the ten-point checklist before shipping. The unifying truth: Spark performance is designed, not tuned; the optimizer works within your design but cannot replace it; and the execution model is the abstraction that makes every one of these judgments obvious rather than memorized.

## Terms to remember

- **Predictive plan reading:** mentally compiling logic into stages/shuffles before running.
- **Design questions:** the pre-job checklist targeting decisions the optimizer can't make.
- **Anti-pattern:** a recurring design mistake that violates the execution model (skewed join, small files, `collect()` on big data, etc.).
- **Idempotency:** the property that rerunning a job produces the same correct result without duplicates or corruption.
- **Replayability:** the ability to safely reprocess a layer from its source (lesson 15).
- **Quality gate:** an assertion (row counts, null rates, uniqueness) that turns "ran" into "ran correctly."
- **Designed, not tuned:** the principle that the large performance wins are architectural and front-loaded.
- **Boundary decisions:** the high-leverage choices at read, shuffle, join, and write.
- **Single-pass design:** structuring a job to avoid recomputing lineage (cache reused intermediates).
- **Plan as design review:** reading `explain()` before running as the cheapest verification of architecture.
