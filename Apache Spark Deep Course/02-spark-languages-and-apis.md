# Spark Languages and APIs

## Purpose of this lesson

Spark exposes itself through several programming languages (Scala, Python, SQL, Java, R) and several API layers (RDD, DataFrame, Dataset, SQL). New users assume these are competing dialects where the choice is mostly taste. They are not. The language is a *front end*; the API layer determines *how much the engine can optimize*. This lesson explains why the choices exist, what each costs and buys, and — most importantly — why PySpark and SQL usually compile to the *same* optimized plan, while a Python UDF can quietly destroy that optimization. Understanding this is the difference between writing Spark that the engine can accelerate and writing Spark that fights the engine.

## Core mental model

```text
Language (Scala / Python / SQL / Java / R)   ← how you type intent
        │
        ▼
API layer (RDD  |  DataFrame / Dataset / SQL) ← how much the engine understands
        │
        ▼
Catalyst logical plan  ← the common meeting point (for DataFrame/SQL/Dataset)
        │
        ▼
Optimized physical execution (Tungsten)
```

Two ideas to hold:
1. **The language barely matters for performance once you use DataFrame/SQL** — they all converge on the same Catalyst plan.
2. **The API layer matters enormously** — RDDs and Python UDFs are *opaque* to Catalyst, so the optimizer cannot help them.

## Main explanation

### Why Spark has multiple language APIs

Spark is written in **Scala** and runs on the **JVM**. Its core abstractions are JVM objects. But the people who need to process data — data engineers, analysts, scientists — work in many languages. Rather than forcing everyone into Scala, Spark provides bindings so each community can use its native language while sharing the same engine underneath. The strategic goal is reach: SQL for analysts, Python for data engineers and scientists, Scala/Java for JVM teams and library authors, R for statisticians. All of them ultimately drive the same Catalyst optimizer and Tungsten execution backend (when using DataFrame/SQL), which is why Spark could grow a huge user base without fragmenting into incompatible engines.

### Scala — the native JVM language

Scala is Spark's implementation language and its "first-class" API. Because Spark runs on the JVM and Scala compiles to JVM bytecode, Scala code interacts with Spark's internals **directly**, with no language bridge. New Spark features often appear in Scala first. The **Dataset** API (typed, compile-time-checked) is fully expressive only in Scala/Java because it relies on JVM type information. For most data engineering, you will not write Scala, but you should know that the engine *is* Scala/JVM, which is why anything that leaves the JVM (like Python execution) incurs a crossing cost.

### Python / PySpark and the Py4J bridge

PySpark lets you drive Spark from Python. But Spark's engine is on the JVM, and Python is a separate runtime. The two communicate through **Py4J**, a bridge that lets Python call JVM methods. The key insight is *what crosses the bridge*:

- When you call DataFrame operations (`df.filter(...)`, `df.groupBy(...).count()`), PySpark sends **plan-building instructions** across Py4J. The actual data is never touched by Python; the JVM builds and executes the plan. Performance is essentially identical to Scala because the work happens entirely on the JVM. Python is just issuing build commands.
- When you call a **Python UDF**, the situation reverses. Now Python must execute *per row*. Spark serializes each row, ships it from the JVM executor to a Python worker process, runs your Python, serializes the result, and ships it back. The data crosses the boundary, row by row, in both directions. This is the expensive path (detailed below).

So PySpark is fast *as long as you stay in the DataFrame/SQL API* and slow the moment you force per-row Python execution.

### SQL — the declarative interface

Spark SQL lets you express logic as standard SQL against tables and views. SQL is purely **declarative**: you state the result you want; the engine decides everything about execution. Spark parses the SQL, builds the *same kind* of Catalyst logical plan that the DataFrame API builds, and optimizes and executes it identically. SQL is not a second-class or slower path — it is a first-class front end to Catalyst. For set-based transformations, SQL is often the clearest expression and is accessible to analysts.

### Java and R

**Java** is supported because Spark runs on the JVM; Java can use the DataFrame and Dataset APIs, though more verbosely than Scala. It matters mainly to teams with existing JVM/Java codebases. **R** (SparkR and `sparklyr`) brings Spark to statisticians, exposing a DataFrame-style API from R. R, like Python, sits outside the JVM and pays a bridge cost for any R-level row processing. Neither is common in mainstream data engineering, but their existence reinforces the pattern: many front ends, one engine.

## Key concepts

- **DataFrame API** — untyped (in Python) relational API; operations build a Catalyst plan; fully optimized.
- **SQL API** — declarative text form of the same; same plan, same optimization.
- **Dataset API** — typed JVM API (Scala/Java); compile-time type safety with most Catalyst optimization.
- **RDD API** — low-level, functional, *opaque* to Catalyst; you control execution but lose optimization.
- **Python UDF** — user function executed per row in a Python process; opaque to Catalyst; expensive boundary crossing.

## Detailed technical sections

### DataFrame API vs SQL API

These are two surfaces over one engine. The DataFrame API expresses operations as method calls that build a plan; SQL expresses the same operations as text that is parsed into a plan. Consider:

```python
# DataFrame API
result = (df
          .filter(df.country == "CH")
          .groupBy("study_id")
          .count())
```

```sql
-- SQL API
SELECT study_id, COUNT(*) AS count
FROM df_view
WHERE country = 'CH'
GROUP BY study_id;
```

Both produce an `Aggregate` over a `Filter` over a `Relation` in the logical plan. After Catalyst optimization (predicate pushdown, column pruning), both yield essentially identical physical plans with the same shuffle for the grouping. You can verify this by calling `.explain()` on both — the physical plans match. The choice between them is **ergonomic**, not performance-driven: use whichever expresses the logic most clearly and is most maintainable for your team. Many pipelines mix both freely.

### Dataset API and why it matters more in Scala/Java

A **Dataset** is a typed collection of JVM objects (`Dataset[Person]`) with compile-time type checking. In Scala/Java you get both the optimizer's benefits *and* type safety: the compiler catches `person.naem` before you run anything. In **Python**, there is no `Dataset` type — Python is dynamically typed and lacks JVM type information, so PySpark exposes only `DataFrame` (which is essentially `Dataset[Row]`, untyped). This is why "Dataset vs DataFrame" is a real decision in Scala but a non-issue in Python: PySpark users always use the DataFrame API. The trade-off in Scala is that typed Dataset operations using lambdas (closures over JVM objects) can be *less* optimizable than relational expressions, because a lambda is partly opaque to Catalyst — a parallel to the Python UDF problem.

### The RDD API and why it is lower-level

RDDs (Resilient Distributed Datasets) were Spark's original abstraction: a distributed collection of objects you manipulate with functional operators (`map`, `filter`, `reduceByKey`) using arbitrary functions. RDDs are powerful and give you total control, but they are **opaque to Catalyst**. An RDD `map(lambda x: ...)` is just a black-box function to Spark; the engine cannot see inside it to push down filters, prune columns, or reorder operations. You manage partitioning and optimization yourself. RDDs also lack the schema and the Tungsten binary format benefits. For modern data engineering, you rarely touch RDDs directly; DataFrame/SQL is the default. RDDs remain useful for genuinely low-level needs (custom partitioning logic, non-tabular data, certain library internals).

### Why DataFrame and SQL are preferred for modern data engineering

Because they are **declarative and structured**, Catalyst can see the entire computation as a tree of known operators and optimize it: pushing filters to the data source, reading only needed columns, reordering joins, folding constants, and choosing physical operators. The schema lets Tungsten use a compact binary row format and generate efficient code. You write *what*, and the engine produces a near-optimal *how*. With RDDs you must hand-optimize, and with Python UDFs you blind the optimizer. For tabular pipelines — which is most data engineering — DataFrame/SQL is both more concise and dramatically faster.

### Why Python UDFs can be slower

A Python UDF breaks the all-JVM execution model. For each row (or batch, with optimizations), Spark must:

```text
JVM executor row (binary, Tungsten format)
   → serialize
   → ship to a separate Python worker process
   → deserialize into Python objects
   → run your Python function (interpreted, per row)
   → serialize result
   → ship back to JVM
   → deserialize into the row format
```

This incurs serialization cost, inter-process communication, and slow per-row Python execution, and — critically — the UDF is **opaque to Catalyst**, so the optimizer cannot push filters through it, prune around it, or generate code for it. A filter that could have been pushed to the Parquet scan may now have to run *after* materializing rows for the UDF. The mitigations are: prefer built-in functions (they become optimized JVM operators); use **Pandas UDFs / vectorized UDFs** (Apache Arrow transfers data in columnar batches, amortizing the crossing cost) when custom logic is unavoidable; or express the logic in SQL/DataFrame primitives. The rule of thumb: **reach for a Python UDF only when no built-in expresses the logic**, and prefer vectorized UDFs when you must.

### Declarative expression vs row-by-row execution

The deep distinction underlying all of the above: built-in DataFrame/SQL functions are **declarative descriptions** that Catalyst compiles into a single optimized, code-generated pipeline running entirely on the JVM over binary rows. A Python UDF is **imperative row-by-row execution** in a foreign runtime. The first lets the engine fuse operations and avoid per-row overhead (lesson 08, whole-stage code generation); the second forces materialization and a process boundary per row. The same logical result can differ by 10–100× in cost depending on which path you take.

### Why SQL and PySpark often compile to similar plans

Because both are parsed/translated into the **same Catalyst logical plan representation** before any optimization happens. Catalyst does not know or care whether a `Filter` node came from `df.filter(...)` or a SQL `WHERE`. From the optimizer's point of view, there is only the plan tree. Therefore the optimized logical plan and the physical plan are the same regardless of surface syntax. This is why you should choose front ends for clarity and team skills, and reason about performance at the *plan* level, not the *syntax* level.

## PySpark and SQL perspective

When to use each, in practice:

- **Use SQL** for set-based transformations that read naturally as queries: joins, aggregations, filtering, windowing across tables. It is concise, familiar to analysts, and reviewable. It shines for the bulk of medallion Silver/Gold transformations.
- **Use PySpark (DataFrame API)** when you need programmatic construction (building transformations in loops, parameterizing column lists, reusable functions), when integrating with Python libraries at the boundaries (not per row), or when the logic is easier to compose as method chains. It also integrates with the surrounding Python notebook control flow.
- **Mix them freely.** A common pattern: register a DataFrame as a temporary view and run SQL against it, or take a SQL result and continue with the DataFrame API. They interoperate seamlessly because they share the engine.
- **Use Scala** when you need typed Datasets, are writing performance-critical library code, need a feature not yet in PySpark, or want to avoid the Python boundary entirely for custom logic. For most Fabric data engineering, this is rare.

A data engineer should ultimately reason about the **engine**, not the surface syntax: where are the shuffles, what does the optimized plan look like, which operations are opaque to Catalyst. The syntax is interchangeable; the plan is what executes.

## Architectural implications

- Standardize on **DataFrame/SQL**; treat RDDs and Python UDFs as exceptions requiring justification.
- Because language doesn't change the plan, **choose front ends by team skills and readability**, not by a belief that one is faster.
- **Audit pipelines for Python UDFs**; each one is a potential optimization hole and a boundary cost.
- When a UDF is unavoidable, prefer **vectorized/Pandas UDFs** to amortize the crossing.

## Common misconceptions

- *"SQL is slower than PySpark"* (or vice versa) — false; they compile to the same plan.
- *"PySpark runs my Python on the cluster"* — only for UDFs; DataFrame ops run on the JVM.
- *"Datasets vs DataFrames is a choice I need to make in Python"* — no; Python only has DataFrames.
- *"A Python UDF is just a slower function"* — it is also opaque to the optimizer, which can hurt far more than the per-row cost.
- *"RDDs are faster because they're lower-level"* — usually slower for tabular work, because they lose Catalyst and Tungsten.

## Summary

Spark has many language front ends and several API layers over one Scala/JVM engine. The language is mostly cosmetic for performance once you use DataFrame/SQL, because all of them converge on the same Catalyst logical plan and Tungsten execution. The API *layer* is decisive: DataFrame/SQL is structured and optimizable; RDDs and Python UDFs are opaque to the optimizer and bypass its benefits. Prefer DataFrame/SQL, mix PySpark and SQL by clarity, avoid Python UDFs unless necessary (and then vectorize them), and always reason at the level of the plan rather than the syntax.

## Terms to remember

- **Py4J** — the bridge letting Python drive the JVM-side Spark engine.
- **DataFrame API** — untyped, optimizable relational API; the data engineering default.
- **Dataset API** — typed JVM API (Scala/Java); not available in Python.
- **RDD** — low-level distributed collection, opaque to Catalyst.
- **Python UDF** — per-row Python execution across a process boundary; opaque to the optimizer.
- **Vectorized / Pandas UDF** — Arrow-based columnar UDF that amortizes the JVM↔Python crossing.
- **Catalyst plan** — the common representation all DataFrame/SQL front ends compile to.
