# Transformations, Actions, and Lazy Evaluation

## Purpose of this lesson

Laziness is the principle that makes Spark an *optimizing* engine rather than a step-by-step interpreter. Because transformations only build a plan and nothing runs until an action, Spark can see the *whole* computation before executing it and optimize across the entire pipeline. This lesson defines transformations and actions precisely, explains why deferral enables optimization, and works through the practical consequences — repeated recomputation, when caching helps, and why some notebook operations trigger execution unexpectedly. Misunderstanding laziness is the root of a large fraction of "why is my Spark slow / why did this run twice" confusion.

## Core mental model

```text
Transformations:  build the plan   (lazy, return a new DataFrame, run nothing)
Action:           execute the plan  (eager, returns a result or writes data)

[transform] → [transform] → [transform] → [ACTION] ⇒ Spark optimizes the whole chain, then runs it
```

A DataFrame is a **recipe**, not a meal. Transformations add steps to the recipe. An action cooks it.

## Main explanation

### Transformations

A **transformation** is an operation that takes a DataFrame and produces a *new* DataFrame describing a further computation. Transformations are **lazy**: calling one does not process any data; it returns a new DataFrame whose plan includes the additional operation. Because each transformation returns a new immutable DataFrame, chaining them simply grows the logical plan.

Common transformations: `select`, `filter` / `where`, `withColumn`, `drop`, `groupBy(...).agg(...)`, `join`, `orderBy` / `sort`, `distinct`, `dropDuplicates`, `union`, `repartition`, `coalesce`, `withColumnRenamed`. Some are **narrow** (no shuffle: `select`, `filter`, `withColumn`) and some are **wide** (shuffle: `groupBy`, `join`, `distinct`, `orderBy`), as in lesson 04 — but *all* are lazy regardless of width.

### Actions

An **action** is an operation that triggers execution and produces a result that is *not* a DataFrame — either a value returned to the driver, or a side effect like writing files. Actions force Spark to compile and run the accumulated plan as one or more jobs (lesson 04).

Common actions: `count`, `collect`, `take(n)`, `first`, `head`, `show(n)`, `write` / `save` / `saveAsTable`, `foreach`, `reduce`, `toPandas`, and aggregations consumed eagerly. The defining trait: an action needs *actual data*, so it must run the plan.

### Lazy evaluation

**Lazy evaluation** means Spark defers all computation until forced. When you write ten transformations, Spark does nothing but accumulate a logical plan. Only the eventual action causes Catalyst to optimize that whole plan and the scheduler to execute it. Contrast this with **eager** evaluation (pandas, ordinary Python), where each operation runs immediately. Laziness is not a convenience feature; it is the *enabling condition* for whole-query optimization.

### Lineage

As transformations accumulate, Spark records the chain that produced each DataFrame — its **lineage** (the logical plan is, in effect, the lineage). Lineage serves two purposes. First, **optimization**: the full chain is the input to Catalyst. Second, **fault tolerance** (lesson 18): if a partition is lost, Spark recomputes it by replaying the lineage from a recoverable point, rather than failing. Lineage is why Spark can be both lazy and resilient — the plan is both the thing to optimize and the recipe to recompute.

## Key concepts

- **Transformation** — lazy; builds plan; returns a new DataFrame.
- **Action** — eager; runs the plan; returns a value or writes data.
- **Lazy evaluation** — defer until an action, enabling whole-pipeline optimization.
- **Lineage** — recorded transformation chain; basis for optimization and recovery.
- **Recomputation** — each action re-runs the plan unless results are cached/persisted.

## Detailed technical sections

### Why Spark waits before executing

If Spark executed each transformation immediately, it would have to materialize the intermediate result of every step and could only optimize one operation at a time — exactly MapReduce's weakness. By waiting, Spark gathers the *entire* sequence of operations as a plan tree and hands it to Catalyst, which can then make global decisions: push a filter all the way down to the data scan, drop unused columns before reading them, reorder joins, combine adjacent operations, and fuse narrow operations into a single code-generated loop (lesson 08). None of these cross-operation optimizations are possible under eager evaluation. Deferral buys the optimizer a global view.

### How lazy evaluation enables optimization

A concrete illustration. Suppose you write:

```python
df = spark.read.parquet("Files/events")        # many columns, many rows
df2 = df.withColumn("y", df.x * 2)             # adds a column
df3 = df2.select("study_id", "country", "y")   # keeps 3 columns
df4 = df3.filter(df3.country == "CH")          # keeps Swiss rows
df4.count()                                      # ACTION
```

Eagerly, this would read all columns and all rows, compute `y` for everything, then project, then filter — wasteful. Lazily, when `count()` triggers, Catalyst sees the whole chain and rewrites it: it pushes the `country == 'CH'` **filter down** to the Parquet scan (so only Swiss row groups are read, using Parquet statistics — predicate pushdown), and **prunes columns** so only `study_id`, `country`, and the column behind `x` are read (column pruning). It may also realize that for a `count()`, the actual column values barely matter. The executed work is a fraction of the literal code. **You wrote what you wanted; the optimizer produced an efficient how — because it could see everything at once.**

### How repeated actions recompute work

Here is the most common practical trap of laziness. Because a DataFrame is a plan, *each action re-executes that plan from its recoverable sources*:

```python
df = spark.read.parquet("Files/big").filter(...).join(other, "k")  # plan only
df.count()    # ACTION 1: runs the whole scan+filter+join
df.show()     # ACTION 2: runs the whole scan+filter+join AGAIN
df.write...   # ACTION 3: runs it a THIRD time
```

Spark does **not** remember the result of `count()` for use by `show()`. Each action independently materializes the plan, re-reading the source data and re-doing the join every time. For an expensive pipeline, calling several actions on the same DataFrame multiplies the cost. This surprises engineers coming from eager systems, where a computed value is just sitting in a variable. In Spark, the variable holds a *plan*, and the plan is re-run on every action.

### How caching changes this

To avoid recomputation, you **cache** or **persist** the DataFrame, which tells Spark to *materialize and keep* its result (in memory, optionally spilling to disk) after the first computation, so subsequent actions reuse it instead of re-running the plan (lesson 17):

```python
df = spark.read.parquet("Files/big").filter(...).join(other, "k")
df.cache()       # marks for caching — still lazy
df.count()       # ACTION 1: runs the plan AND populates the cache
df.show()        # ACTION 2: reads from cache — no re-scan, no re-join
```

Two subtleties. First, `cache()` is itself **lazy** — it only *marks* the DataFrame; the data is materialized on the *first action*, which is why `count()` above does double duty (it both runs and fills the cache). Second, caching is not free: it consumes executor memory and can cause spills or evictions, so you cache only when a DataFrame is reused enough to pay back the memory cost (lesson 17 gives decision guidelines). The architectural alternative — writing an intermediate result to a Delta table and reading it back — is another form of materialization, often preferable for large, durable intermediates.

### Why `show()`, `count()`, `write()`, and `collect()` trigger jobs

Each needs *real data*, so each forces execution:

- **`count()`** must process rows to produce a number → full job (though Catalyst may optimize it to count without reading all columns).
- **`collect()`** must gather all rows to the driver → full job (and the driver-memory danger of lesson 03).
- **`show(n)` / `take(n)`** must produce actual rows, but only `n` of them → a job, often optimized to read just enough partitions (limit pushdown) rather than the whole dataset.
- **`write()` / `saveAsTable()`** must produce and persist all output rows → full job with output tasks.

The unifying point: an action's defining property is that it yields something that is not a lazy DataFrame, so it cannot be deferred further.

### Why `display()` in notebooks may trigger execution

In notebooks (including Fabric), `display(df)` renders a preview table or chart of the DataFrame's contents. To show contents, it must *fetch actual rows* — which is an action. So `display()` (and to some extent automatic rendering of a DataFrame as the last expression in a cell) **runs a job**, typically fetching a bounded number of rows. This catches people out: scattering `display()` calls through a notebook to "look at" intermediate DataFrames silently triggers repeated executions of those (possibly expensive) plans. Treat `display()` as an action for cost-reasoning purposes.

### Why `withColumn()` does not immediately execute

`withColumn()` is a transformation: it returns a new DataFrame whose plan includes the added/derived column, but it computes nothing. Even an expensive expression inside `withColumn` (a complex case expression, a built-in function over many columns) is only a *node in the plan* until an action arrives. This is why you can chain dozens of `withColumn` calls cheaply during development — none of them run. It also means a bug in a `withColumn` expression may not surface until you finally call an action and Spark actually evaluates it.

### Why chaining transformations is normal and good

Because transformations are lazy and fused by the optimizer, **long chains are the idiomatic, performant way to write Spark.** There is no penalty for breaking logic into many `select`/`filter`/`withColumn` steps; Catalyst collapses and optimizes them, and Tungsten fuses narrow ones into a single loop (lesson 08). Writing readable, step-by-step transformations does *not* mean step-by-step execution. The optimizer decouples *how you express* the pipeline from *how it runs*. So favor clarity in your chains; the engine handles efficiency.

### Logical plan construction vs physical execution

The clean separation to keep in mind:

```text
PLAN CONSTRUCTION (lazy, on the driver, cheap):
  each transformation appends to the logical plan; no executors involved.

PHYSICAL EXECUTION (eager, triggered by an action):
  Catalyst optimizes → physical plan → DAG of stages/tasks → executors run it.
```

All of development-time transformation chaining is plan construction. Only actions cross into physical execution. When reasoning about cost, ignore the transformations and ask: *where are the actions, and how many times does each force the plan to run?*

## PySpark and SQL perspective

```python
events = spark.read.parquet("Files/events")     # transformation (lazy): scan plan
swiss  = events.filter("country = 'CH'")        # transformation (lazy)
agg    = swiss.groupBy("study_id").count()      # transformation (lazy)
agg.write.format("delta").saveAsTable("g")      # ACTION: now everything runs, once
```

Up to the `write`, nothing has touched the data beyond reading file metadata. The `write` triggers a single job: Catalyst pushes the `country='CH'` filter into the scan, prunes to needed columns, plans a shuffle for the `groupBy`, and executes. In SQL, the equivalent `CREATE TABLE g USING delta AS SELECT study_id, COUNT(*) FROM events WHERE country='CH' GROUP BY study_id` is *entirely* declarative — the whole statement is one plan executed once. SQL has no separate "transformation vs action" syntax because the statement *is* the action; but underneath, Spark builds and optimizes the same lazy plan before running it.

A caution shown explicitly:

```python
agg.cache()
agg.count()    # runs plan + fills cache
agg.show()     # cache hit, cheap
agg.write...   # cache hit, cheap
# vs. WITHOUT cache(): count, show, and write would each re-run the full scan+filter+groupBy.
```

## Architectural implications

- **Count your actions, not your transformations.** Cost lives in actions and in how many times each forces the plan.
- **Reuse ⇒ materialize.** If a DataFrame feeds multiple actions/branches, cache it or write it to Delta; otherwise you pay for it repeatedly.
- **Laziness lets you write clear pipelines** without performance guilt — the optimizer fuses and prunes.
- **Beware hidden actions** in notebooks (`display`, auto-render, schema inference reads) that run plans you didn't mean to run.
- **Errors may be deferred.** A malformed expression surfaces at the action, not at the transformation that introduced it.

## Common misconceptions

- *"Each line that returns a DataFrame did some work."* No — it only extended the plan.
- *"Once I compute `count()`, the data is in memory for the next step."* No — without caching, the next action recomputes from sources.
- *"`cache()` immediately stores the data."* No — it marks for caching; the first action materializes it.
- *"More transformations = slower."* No — narrow chains are fused; clarity is free.
- *"`display()` is just printing."* No — it fetches rows, which runs a job.

## Summary

Transformations are lazy plan-builders; actions are eager plan-runners. Laziness gives Catalyst a global view, enabling cross-operation optimizations (predicate pushdown, column pruning, operator fusion) impossible under eager evaluation. The practical cost of laziness is that each action re-executes the plan from its sources, so reused DataFrames should be cached or materialized to Delta. Notebook conveniences like `display()` and schema inference are hidden actions. Reason about Spark cost by locating the actions and counting how many times each forces the plan to run, not by counting transformations.

## Terms to remember

- **Transformation** — lazy operation returning a new DataFrame; builds the plan.
- **Action** — eager operation that executes the plan and returns/writes a result.
- **Lazy evaluation** — deferral of computation until an action; enables global optimization.
- **Lineage** — recorded transformation chain; basis for optimization and recomputation.
- **Recomputation** — re-running a plan on each action absent caching.
- **Materialization** — forcing and keeping a result, via cache/persist or a Delta write.
