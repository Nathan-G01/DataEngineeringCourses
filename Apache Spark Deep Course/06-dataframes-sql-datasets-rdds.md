# DataFrames, SQL, Datasets, and RDDs

## Purpose of this lesson

Spark offers four ways to express distributed computation: RDDs, DataFrames, Datasets, and Spark SQL. They differ in level of abstraction, type safety, and — decisively — how much the optimizer can help. This lesson compares them so you understand what you gain and lose with each, why modern data engineering standardizes on DataFrames and SQL, when RDDs still earn their place, and the conceptual point that a DataFrame is **not data** but a **plan**. This consolidates and deepens the API discussion from lesson 02 with a focus on the data abstractions themselves.

## Core mental model

```text
RDD        : distributed collection of opaque objects   — full control, no optimizer
DataFrame  : distributed table with a schema (untyped)  — optimizer sees everything
Dataset    : typed DataFrame (Scala/Java only)          — optimizer + compile-time types
Spark SQL  : declarative text over tables/views         — same engine as DataFrame
```

Higher up = more the engine understands = more it can optimize. Lower down = more control = more responsibility.

## Main explanation

### RDDs — the original abstraction

The **RDD (Resilient Distributed Dataset)** was Spark's founding abstraction: a fault-tolerant, distributed collection of arbitrary objects (`RDD[T]`) partitioned across the cluster, manipulated with functional operators (`map`, `filter`, `flatMap`, `reduceByKey`, `join`). "Resilient" refers to lineage-based fault tolerance (lesson 18); "distributed" to partitioning across executors. RDDs give you total control: you decide how to transform each element and can manage partitioning explicitly. But an RDD has **no schema** — Spark sees only opaque objects and black-box functions. It cannot inspect a `map(lambda x: ...)` to optimize it. RDDs also don't benefit from the Tungsten binary format; they hold deserialized JVM (or Python) objects with full object overhead.

### DataFrames — distributed relational tables

A **DataFrame** is a distributed collection of rows organized into named, typed columns — conceptually a distributed relational table. Crucially, it carries a **schema** (column names and types) that Spark knows at plan time. Operations are expressed as relational/columnar transformations (`select`, `filter`, `groupBy`, `join`) that build a Catalyst logical plan. Because the engine understands the schema and the operations, it can apply the full optimizer (lesson 07) and Tungsten execution (lesson 08). In Python, the DataFrame is *untyped* at compile time (it's `Dataset[Row]`), but it is fully *schema-aware* at runtime. The DataFrame is the default abstraction for data engineering.

### Datasets — the typed JVM API

A **Dataset** (`Dataset[T]` for a concrete type `T`, e.g. `Dataset[Event]`) is a typed, object-oriented API available in **Scala and Java**. It combines the optimizer benefits of DataFrames with **compile-time type safety**: the compiler checks field names and types before the job runs. It is, in effect, a DataFrame that knows it contains `Event` objects rather than generic `Row`s. The cost: operating on a Dataset with typed lambdas (`ds.map(e => e.x + 1)`) can be *less* optimizable, because the lambda is partly opaque to Catalyst — similar to the UDF problem. **Python and R have no Dataset API** (they are dynamically typed and lack JVM type info), so in PySpark there is only DataFrame. This is why "DataFrame vs Dataset" is a real decision in Scala but irrelevant in Python.

### Spark SQL

**Spark SQL** lets you express computation as ANSI-style SQL against tables and views. It is parsed into the same Catalyst logical plan as the DataFrame API and optimized and executed identically. SQL operates over the **catalog** (databases, tables) and over **temporary views** registered from DataFrames. It is a first-class, fully optimized interface — not a slower or limited path. Many engineers do most of their Silver/Gold transformation logic in SQL for its clarity and reviewability.

## Key concepts

- **Schema** — the named, typed column structure the engine uses to optimize and to format rows compactly.
- **Rows and columns** — DataFrames/Datasets are row collections with typed columns; columnar awareness enables pruning and vectorization.
- **Typed vs untyped** — Datasets are typed (compile-time checks); DataFrames in Python are untyped at compile time but schema-aware at runtime.
- **Declarative optimization** — relational/columnar operations are visible to Catalyst; opaque functions (RDD ops, lambdas, UDFs) are not.
- **Temporary views** — naming a DataFrame so SQL can query it within the session.
- **Managed vs external tables** — catalog tables whose data lifecycle Spark owns vs tables that merely point at existing storage.

## Detailed technical sections

### Schema, rows, and columns

A schema is the contract: it tells Spark each column's name and data type (and nullability). With it, Spark can (a) check operations at analysis time (does this column exist? are types compatible?), (b) prune to only needed columns when reading, (c) push predicates using column statistics, and (d) lay out rows in Tungsten's compact binary format addressed by column offset. RDDs lack all of this because they have no schema. The schema is the single most important thing DataFrames add over RDDs, because nearly every optimization depends on the engine knowing the structure of the data.

### Typed vs untyped APIs and what each gains and loses

| Aspect | RDD | DataFrame (Python) | Dataset (Scala/Java) | Spark SQL |
|--------|-----|--------------------|----------------------|-----------|
| Abstraction level | Low (objects) | High (table) | High (typed table) | High (declarative) |
| Schema known to engine | No | Yes | Yes | Yes |
| Catalyst optimization | No | Yes | Yes (less with lambdas) | Yes |
| Tungsten binary format | No | Yes | Yes | Yes |
| Compile-time type safety | Yes (object types) | No | Yes | No |
| Available in Python | Yes | Yes | **No** | Yes |
| Control over execution | Highest | Medium | Medium | Lowest |
| Typical data-engineering use | Rare/low-level | **Default** | JVM teams | **Default for set logic** |

What you **gain** going up the stack: optimization, compactness, conciseness, portability across languages. What you **lose**: fine-grained control over execution and (for DataFrames in Python) compile-time type checks. For tabular pipelines the trade strongly favors DataFrame/SQL.

### Why RDDs are less optimized

Because the engine cannot see inside RDD operations. An `rdd.map(f)` is, to Spark, "apply this black box to each element." Spark cannot know that `f` only uses two fields (to prune columns), or that a downstream filter could be pushed earlier, or how to generate fused code, because there is no schema and no relational structure — just functions over objects. RDDs also store deserialized objects with JVM (or Python) overhead rather than packed binary rows. So the *same* logical computation expressed as RDD operations versus DataFrame operations can run very differently: the DataFrame version is optimized and code-generated; the RDD version runs your functions as-is.

### Why DataFrames and SQL use Catalyst

Because they are *structured and declarative*. A DataFrame operation or SQL clause maps to a known logical operator (`Project`, `Filter`, `Aggregate`, `Join`) over a schema. Catalyst manipulates this tree: resolving names against the catalog, applying optimization rules, and planning physical operators (lesson 07). The structure is precisely what makes optimization possible. This is the central reason the course (and the industry) prefers DataFrame/SQL: you are speaking a language the optimizer understands.

### Why modern Spark data engineering mostly uses DataFrames and SQL

Three reasons converge: (1) **performance** — Catalyst + Tungsten make them far faster than RDDs for tabular work; (2) **productivity** — relational operations and SQL are concise and familiar; (3) **interoperability** — schema-and-catalog awareness integrates with Delta tables, the lakehouse, and BI tools. RDDs survive mainly inside library internals and special cases. For building Bronze→Silver→Gold pipelines, DataFrame/SQL is the standard.

### When RDDs are still useful

RDDs remain the right tool when: you need **fine-grained control over partitioning** or custom partitioners; you process **non-tabular or unstructured data** (raw text, binary, complex nested objects) that doesn't fit a relational schema; you implement **low-level algorithms** that don't map cleanly to relational operators; or you maintain legacy code. Even then, the common advice is to do the unstructured/low-level part in RDDs and convert to a DataFrame as soon as the data is tabular, so the rest of the pipeline gets optimized. RDDs are an escape hatch, not a default.

### How temporary views work

A **temporary view** registers a DataFrame under a name in the session catalog so SQL can query it:

```python
df = spark.read.parquet("Files/events")
df.createOrReplaceTempView("events")
agg = spark.sql("SELECT study_id, COUNT(*) FROM events GROUP BY study_id")
```

A temp view is **not** a copy of the data and **not** materialized — it is just a *name* bound to the DataFrame's *plan*. Querying the view splices its plan into the SQL query's plan; Catalyst then optimizes the combined plan as a whole. Temp views are session-scoped and vanish when the session ends. They are the seam that lets you move fluidly between DataFrame and SQL styles over the same logical data.

### How managed and external tables relate to DataFrames

A **table** in the catalog associates a name with data in storage plus metadata (schema, format, location). A **managed table** is one whose data lifecycle Spark/the platform owns: drop the table and the underlying data is deleted; the storage location is managed for you. An **external (unmanaged) table** points at a location you control: drop the table and only the metadata is removed, the data files remain. In a lakehouse (e.g. Fabric), tables are typically **Delta** tables in OneLake. When you read a table, you get a DataFrame (a plan to scan that table); when you `saveAsTable`, you write a DataFrame's result to the table's storage and register/update its metadata. So DataFrames are the *in-memory plan* view; tables are the *persistent catalog* view of the same data.

### DataFrames are not data — they are plans until executed

This is the conceptual keystone, restated from lesson 05 in the language of abstractions. A DataFrame variable does not hold rows. It holds a *logical plan* (lineage) describing how to produce rows from sources. Two DataFrames built from the same source share that source's scan in their plans, not a materialized result. Only an action turns a plan into actual rows, and (absent caching) those rows are not retained. This is why you can build enormous transformation graphs cheaply, why reused DataFrames recompute, and why "the DataFrame is large" is a category error — *the data the plan would produce* is large; the DataFrame itself is just a description. RDDs share this laziness, but the DataFrame's plan is *optimizable* in a way an RDD's chain of functions is not.

## PySpark and SQL perspective

```python
# DataFrame API
events = spark.read.parquet("Files/events")
result = events.groupBy("study_id").count()

# Equivalent via SQL over a temp view
events.createOrReplaceTempView("events")
result_sql = spark.sql("SELECT study_id, COUNT(*) AS count FROM events GROUP BY study_id")
```

Both `result` and `result_sql` are DataFrames holding equivalent logical plans; both compile through Catalyst to the same physical plan with a shuffle for the grouping. Neither has computed anything yet. By contrast, the RDD form of the same count:

```python
rdd = events.rdd.map(lambda r: (r["study_id"], 1)).reduceByKey(lambda a, b: a + b)
```

is opaque to Catalyst — Spark cannot prune to just `study_id`, cannot use the binary row format, and runs your lambdas as-is. It produces the same answer, generally slower, and you've taken on manual responsibility for efficiency. The lesson: express tabular logic relationally (DataFrame/SQL) and drop to RDDs only when you genuinely need their control.

## Architectural implications

- **Default to DataFrame/SQL**; treat RDDs as a justified exception for unstructured/low-level work.
- **Schema is leverage** — define and preserve schemas so the optimizer and Tungsten can do their jobs.
- **Temp views are free glue** between PySpark and SQL; use whichever reads best per transformation.
- **Tables vs DataFrames**: think persistent-catalog vs in-memory-plan views of the same data; manage table lifecycle (managed vs external) deliberately.
- **Never reason about "DataFrame size"** — reason about the size of the data its plan would materialize, and where that materialization happens.

## Common misconceptions

- *"A DataFrame stores my rows."* No — it stores an optimizable plan.
- *"Datasets are a Python option."* No — Python only has DataFrames; Datasets are Scala/Java.
- *"RDDs are faster because they're closer to the metal."* Usually slower for tabular work — no Catalyst, no Tungsten.
- *"A temp view materializes the data."* No — it's a named pointer to a plan, spliced in at query time.
- *"SQL and DataFrame are different engines."* Same engine; same Catalyst plan.

## Summary

Spark's four abstractions trade control for optimization. RDDs are low-level, schema-less, and opaque to the optimizer — powerful but slow for tabular work and reserved for special cases. DataFrames are schema-aware distributed tables that the optimizer and Tungsten fully accelerate, and they are the data-engineering default (untyped at compile time in Python). Datasets add compile-time types in Scala/Java only. Spark SQL is a declarative front end compiling to the same plans as DataFrames, glued to them by temporary views and the table catalog. Across all of them, a DataFrame/Dataset/RDD is a *plan*, not data, until an action runs it.

## Terms to remember

- **RDD** — schema-less distributed object collection; opaque to Catalyst.
- **DataFrame** — schema-aware distributed table; the optimized default.
- **Dataset** — typed DataFrame (Scala/Java only).
- **Schema** — named/typed column structure enabling optimization and compact rows.
- **Temporary view** — session-scoped name bound to a DataFrame's plan for SQL.
- **Managed vs external table** — Spark-owned vs pointer-to-existing-storage catalog tables.
