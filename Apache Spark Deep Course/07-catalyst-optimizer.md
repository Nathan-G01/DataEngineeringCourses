# The Catalyst Optimizer

## Purpose of this lesson

Catalyst is the reason DataFrame/SQL beats RDD, the reason your declarative code becomes efficient distributed work, and the reason "what you write" and "what runs" can differ dramatically. This lesson opens the optimizer: the phases a query passes through (parsing → analysis → logical optimization → physical planning), the specific rewrites Catalyst performs (predicate pushdown, column pruning, constant folding, join reordering, and more), and how to read explain plans so you can *see* the optimizer's decisions. Knowing Catalyst turns performance tuning from guesswork into reading the plan and reasoning about why the optimizer chose what it did.

## Core mental model

```text
User code / SQL
   │  parse
   ▼
Unresolved logical plan      (names not yet bound)
   │  analysis (catalog)
   ▼
Resolved logical plan        (columns/tables/types resolved)
   │  rule-based optimization
   ▼
Optimized logical plan       (pushdowns, pruning, folding, reordering)
   │  physical planning (+ cost-based choices)
   ▼
Physical plan(s)             (concrete operators: scans, exchanges, joins)
   │  selected by cost
   ▼
Executable plan → Tungsten codegen → tasks
```

Catalyst is a **tree-transformation engine**: every plan is a tree, and optimization is a sequence of rules that rewrite the tree into an equivalent but cheaper tree.

## Main explanation

### What Catalyst is

Catalyst is Spark's extensible query optimizer, written in Scala. It represents queries as trees of nodes (logical operators, then physical operators, with expressions as sub-trees) and applies **transformation rules** that rewrite these trees. It has two broad styles of optimization: **rule-based** (apply known-good rewrites, like pushing filters down) and **cost-based** (use statistics to choose among alternatives, like join order and strategy). Catalyst is what makes Spark a *compiler*: it takes a high-level description and produces an optimized executable plan.

### Logical plans

A **logical plan** is a tree describing *what* the query computes, in terms of relational operators — `Relation` (a data source), `Filter`, `Project` (select columns), `Aggregate`, `Join` — independent of *how* to execute them. It says "scan this, filter that, group by this," without specifying file reading details, shuffle mechanics, or join algorithms. Both the DataFrame API and SQL produce logical plans; this is their common representation (lesson 02).

### The analysis phase and unresolved attributes

When you first write `df.select("study_id")` or `SELECT study_id FROM events`, Spark produces an **unresolved logical plan**: it knows you referenced a column called `study_id` and a relation called `events`, but it has not yet confirmed they exist or determined their types. These references are **unresolved attributes**. The **analysis phase** resolves them against the **catalog** (the registry of tables, views, columns, and their schemas): it binds `events` to a real table/plan, confirms `study_id` exists, infers its type, and checks that the operations are valid (e.g., you can't sum a string). If a name doesn't resolve — a typo'd column — analysis fails with an `AnalysisException` *before* any execution. After analysis you have a **resolved (analyzed) logical plan**.

### Catalog resolution

The **catalog** is the metadata source the analyzer consults: it maps table and view names to their schemas and locations, including temp views (lesson 06) and persistent Delta tables (in Fabric, the Lakehouse catalog over OneLake). Resolution is where the abstract names in your code become concrete, typed references. This is also where schema-related errors surface early, which is a feature: you learn about a bad column reference at analysis time, not after an hour of computation.

### Optimized logical plans

The resolved plan is then rewritten by a battery of **rule-based optimizations** into an **optimized logical plan** — still logical (no physical operators yet), but transformed into an equivalent, cheaper form. This is where predicate pushdown, column pruning, constant folding, and similar rewrites happen (detailed below). The optimizer applies rules repeatedly until the plan stops changing (a fixed point).

### Physical planning

From the optimized logical plan, Catalyst generates one or more **physical plans** — trees of concrete physical operators that specify *how* to execute: which scan operator, where to insert **exchanges** (shuffles), which **join algorithm** (broadcast hash, sort-merge, shuffle hash — lesson 12), and so on. When multiple physical plans are possible (notably different join strategies), Catalyst uses **cost-based** reasoning over statistics to pick one. The chosen physical plan is then handed to Tungsten for code generation (lesson 08) and to the scheduler for execution (lesson 04).

## Key concepts

- **Rule-based optimization** — apply always-beneficial rewrites (pushdown, pruning, folding).
- **Cost-based optimization (CBO)** — use statistics (row counts, sizes) to choose among alternatives (join order/strategy).
- **Predicate pushdown** — move filters as close to the data source as possible.
- **Projection/column pruning** — read and carry only needed columns.
- **Constant folding** — evaluate constant expressions at plan time.
- **Null propagation** — simplify expressions involving nulls.
- **Join reordering** — order joins to minimize intermediate data.
- **Exchange** — the physical shuffle operator inserted during physical planning.

## Detailed technical sections

### Rule-based vs cost-based optimization

**Rule-based** optimizations are rewrites that are *always* (or almost always) beneficial and don't need data statistics — e.g., pushing a filter down can never hurt and usually helps, so Catalyst always does it. **Cost-based** optimizations choose between *valid alternatives* using estimated costs derived from **statistics** (number of rows, column sizes, distinct counts). The classic CBO decisions are **join order** (which tables to join first to keep intermediate results small) and **join strategy** (broadcast vs sort-merge, based on estimated table size). CBO is only as good as its statistics; stale or missing stats lead to poor choices — which is part of why **AQE** (lesson 19) exists, to correct plans using *actual* runtime statistics.

### Predicate pushdown and filter pushdown

**Predicate pushdown** moves filter conditions down the plan toward the data source, ideally *into the scan itself*. If your query filters `country = 'CH'`, Catalyst tries to apply that filter as early as possible — for columnar formats like Parquet, it can push the predicate into the file reader, which uses per-row-group **min/max statistics** to skip entire row groups that can't match, and into Delta's data-skipping (lesson 14) to skip whole files. The result: Spark reads far less data from storage. "Filter pushdown" is the same idea; the effect is reading less and processing less, as early as possible.

### Projection pruning / column pruning

**Column pruning** ensures Spark reads and carries only the columns the query actually needs. If a table has 80 columns but your query uses 3, Catalyst prunes the projection so the Parquet reader fetches only those 3 columns' data (columnar formats store columns separately, making this cheap and effective). This both reduces I/O and shrinks the rows flowing through the pipeline. Combined with predicate pushdown, pruning often means a query physically reads a tiny fraction of a large table.

### Constant folding

**Constant folding** evaluates expressions whose value is known at plan time, replacing them with the result. `WHERE year = 2020 + 5` becomes `WHERE year = 2025`; `SELECT 1 + 1 AS two` precomputes `2`. This removes per-row arithmetic that would otherwise run on every row across the cluster. It's a small but ubiquitous example of the optimizer doing work once at plan time instead of billions of times at execution time.

### Null propagation and expression simplification

**Null propagation** and related simplifications rewrite expressions using the semantics of nulls and booleans to cheaper equivalents. For example, an expression known to always be null can be folded to null; `x AND false` becomes `false`; redundant casts are removed; `NOT(NOT x)` becomes `x`. These simplifications shrink and speed up the per-row expression evaluation and sometimes enable further pushdown. Individually minor, collectively they keep the executed expression trees lean.

### Join reordering

When a query joins several tables, the *order* of joins hugely affects the size of intermediate results. Joining a huge table to another huge table first, then to a tiny one, can create an enormous intermediate; joining the selective/small ones first keeps intermediates small. **Join reordering** (a cost-based optimization) uses statistics to pick an order that minimizes intermediate data. This only works well with good statistics; without them, Catalyst may keep the written order, and AQE may step in at runtime.

### Why Catalyst is a main reason DataFrame/SQL beats RDD

Every optimization above requires the engine to *understand* the query: to see filters, columns, expressions, and joins as structured operators over a schema. RDDs present opaque functions over schema-less objects, so none of these rewrites are possible — Catalyst has nothing to optimize. DataFrame/SQL express logic in exactly the structured, declarative form Catalyst needs. This is the concrete, mechanical reason the course insists on DataFrame/SQL: you are handing the optimizer a tree it can rewrite, not a black box it must run verbatim.

### How PySpark code becomes a JVM-side logical plan

When you call `df.filter(...)` in PySpark, you are not building a Python data structure that gets shipped around. Through Py4J (lesson 02), the call constructs a node in the **JVM-side** Catalyst logical plan. Your Python is issuing instructions that build a plan tree living in the driver's JVM. By the time an action fires, there is a JVM logical plan that Catalyst optimizes exactly as if it had come from Scala or SQL — which is why PySpark DataFrame performance equals Scala's. The Python was a remote control for plan construction.

### Why built-in functions help Catalyst (and Python UDFs hurt)

Built-in functions (`F.col`, `F.when`, `F.to_date`, `F.sum`, etc.) are **expressions Catalyst understands**. It can analyze them, fold constants in them, push filters through them, and code-generate them into the fused execution loop (lesson 08). A **Python UDF**, by contrast, is **opaque**: Catalyst sees "call an unknown Python function" and cannot look inside. It cannot push a filter through the UDF, cannot prune based on what the UDF uses, and cannot code-generate it — and it must ship rows to a Python process. So a UDF is a *barrier* in the plan around which the optimizer cannot reason. Whenever a built-in expresses your logic, the optimizer can optimize it; whenever you use a UDF, you create an opaque region. This is the deepest reason to prefer built-ins.

## Detailed technical sections: interpreting `explain("formatted")`

`df.explain()` (or `EXPLAIN` in SQL) prints the plans Catalyst produced. `explain("formatted")` gives the most readable output, and `explain(True)` shows all four stages (parsed, analyzed, optimized logical, and physical). Reading the **physical plan** is the highest-value skill. A guide to what to look for:

- **Read bottom-up.** The leaves are data sources (`Scan parquet ...` / `FileScan`); execution flows upward.
- **`FileScan` / `Scan` node.** Look at `PushedFilters` — these are the predicates that were *pushed down* into the read. If your filter appears here, pushdown worked. Look at the column list — a short list means column pruning worked. `PartitionFilters` show partition pruning (lesson 10).
- **`Filter` node above the scan.** A residual filter that *couldn't* be pushed down (e.g., one wrapped behind a UDF or a non-pushable expression) appears here and runs per row.
- **`Exchange` nodes.** Each `Exchange` is a **shuffle** — a stage boundary. Count them to count your stages (lesson 04). The type (`hashpartitioning`, `rangepartitioning`, `SinglePartition`) tells you how data is redistributed. Fewer exchanges is generally better.
- **Join operator type.** `BroadcastHashJoin` (small side broadcast, no shuffle of the big side), `SortMergeJoin` (both sides shuffled and sorted), `ShuffleHashJoin` — this tells you the chosen strategy (lesson 12). `BroadcastExchange` feeding a join confirms a broadcast.
- **`HashAggregate` (often two of them).** A partial aggregate on the map side and a final aggregate on the reduce side, separated by an exchange — the map-side combine pattern (lesson 04).
- **`WholeStageCodegen` boxes.** Operators grouped inside a codegen boundary are fused into one generated loop (lesson 08). A `*` or codegen marker next to operators indicates they were code-generated. Operators *outside* codegen (like a Python UDF) signal a non-fused, slower region.
- **`AdaptiveSparkPlan` / `AQEShuffleRead`.** Indicates AQE is active and may replan at runtime (lesson 19); the *final* plan may differ from the initial one, and the UI's SQL tab shows the adapted plan.

A practical reading loop: find the scans (did pushdown and pruning happen?), count the exchanges (how many shuffles/stages?), identify the join strategies (broadcast or sort-merge, as expected?), and check for opaque UDFs or residual filters breaking the codegen pipeline. This single habit catches a large share of performance problems before they ever run.

## PySpark and SQL perspective

```python
df = spark.read.parquet("Files/events")       # 80 columns
(df.filter(df.country == "CH")
   .select("study_id", "country")
   .explain("formatted"))
```

In the physical plan you should see, on the `FileScan parquet` node, `PushedFilters: [EqualTo(country,CH)]` (predicate pushdown) and a column list limited to `study_id, country` (pruning) — even though the table has 80 columns and you filtered after reading in the code. The optimizer reordered and pushed your intent into the scan. The SQL form `SELECT study_id, country FROM events WHERE country='CH'` yields the same physical plan, confirming that Catalyst, not the syntax, determines execution. Now contrast wrapping the filter in a UDF: `df.filter(my_udf(df.country))` — the pushdown disappears, a residual `Filter` appears above the scan, the scan reads more, and the UDF region is outside whole-stage codegen.

## Architectural implications

- **Write declaratively with built-ins** so Catalyst can push down, prune, fold, and code-generate.
- **Keep statistics fresh** (and rely on AQE) so cost-based decisions — join order and strategy — are good.
- **Read the plan before tuning.** The physical plan tells you what *actually* runs; tune from it, not from the source code's apparent order.
- **Treat UDFs as optimization barriers** — expect lost pushdown and broken codegen around them.
- **Count exchanges** in the plan to anticipate shuffles and stages.

## Common misconceptions

- *"Spark runs my code in the order I wrote it."* No — Catalyst reorders and rewrites; the plan is what runs.
- *"Filters always run where I put them."* No — they're pushed down, ideally into the scan.
- *"The optimizer reads all my data to optimize."* No — rule-based rewrites are structural; only cost-based choices need (cheap) statistics.
- *"A UDF is just a slow function."* It's also an optimization barrier that blocks pushdown and codegen.
- *"explain output is the literal source."* It's the optimized/physical plan — often very different from the source.

## Summary

Catalyst is Spark's tree-transformation optimizer. It parses your query, resolves names against the catalog (analysis), applies rule-based rewrites (predicate pushdown, column pruning, constant folding, null propagation, join reordering) to produce an optimized logical plan, then generates physical plans and uses cost-based reasoning (over statistics) to choose join strategies and order. PySpark and SQL both build the same JVM-side logical plan, which is why they perform identically. Built-in functions are transparent to Catalyst and get optimized and code-generated; Python UDFs are opaque barriers that block pushdown and fusion. Reading `explain("formatted")` — scans/pushdown/pruning, exchanges/shuffles, join strategies, codegen boundaries — is the core skill for understanding and tuning what actually runs.

## Terms to remember

- **Catalyst** — Spark's tree-based query optimizer.
- **Unresolved attribute** — a name not yet bound to a real column/table.
- **Analysis** — resolving names/types against the catalog.
- **Predicate / filter pushdown** — moving filters toward (into) the scan.
- **Column / projection pruning** — reading only needed columns.
- **Constant folding** — evaluating constant expressions at plan time.
- **Cost-based optimization** — statistics-driven choices (join order/strategy).
- **Exchange** — physical shuffle operator; a stage boundary in the plan.
