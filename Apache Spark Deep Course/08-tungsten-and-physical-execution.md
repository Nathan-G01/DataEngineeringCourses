# Tungsten and Physical Execution

## Purpose of this lesson

Catalyst decides *what* to execute and *how* at the logical/physical-plan level. **Tungsten** is the backend that makes that physical plan run *fast on real CPUs and memory*. It addresses a different bottleneck than Catalyst: not "is the plan good?" but "is the execution efficient at the level of objects, memory, and instructions?" This lesson explains why JVM object overhead is a problem, how Tungsten's binary row format and memory management solve it, what whole-stage code generation and vectorization do, and why all of this is the mechanical reason DataFrame operations crush Python UDFs. Together with lesson 07, it completes the picture of Spark as a compiler.

## Core mental model

```text
Catalyst:  produces a good operator tree (WHAT/HOW at the plan level)
Tungsten:  executes that tree efficiently on hardware (FAST at the CPU/memory level)

Logical expression
   → optimized operator tree            (Catalyst)
   → generated code / physical operators (Tungsten codegen)
   → task execution over partitions      (one fused loop per stage, over binary rows)
```

Catalyst removes *unnecessary work*; Tungsten makes the *necessary work* cheap per row.

## Main explanation

### What Tungsten is

Tungsten is Spark's execution backend — a set of techniques introduced to push Spark performance toward the limits of the hardware rather than the JVM's defaults. Its three pillars are: a compact **binary in-memory row format** (UnsafeRow) that avoids JVM object overhead; **explicit/off-heap memory management** that bypasses much of the garbage collector; and **whole-stage code generation**, which compiles an entire stage of operators into a single tight loop. The motivating observation was that, after Catalyst, Spark's bottleneck was often *CPU and memory efficiency* — object allocation, pointer chasing, garbage collection, and virtual function call overhead — not I/O. Tungsten attacks those.

### Why JVM object overhead is a problem

Spark runs on the JVM, and naive JVM data processing represents each value as an object. A simple row of a few fields, stored as JVM objects (a `String`, boxed `Integer`s, etc.), carries substantial overhead: object headers, pointers between objects, padding, and the cost of the garbage collector tracking millions or billions of small objects. Processing data this way means constant allocation, pointer-chasing through scattered heap memory (poor CPU cache behavior), and frequent, pausing garbage collection. When you process billions of rows, this overhead dominates. RDDs of objects suffer exactly this. Tungsten exists to escape it.

### Binary row format and UnsafeRow

Tungsten stores rows in a **compact binary format** — `UnsafeRow` — rather than as graphs of JVM objects. A row is laid out as a contiguous block of bytes: a fixed-length region (with null-tracking bits and fixed-width fields or offsets) plus a variable-length region for things like strings. Fields are accessed by **offset arithmetic** directly on the byte block, not by following object pointers. Benefits: far less memory per row (no per-field object headers), excellent CPU cache locality (a row is one contiguous block), and the ability to operate on raw memory without deserializing into objects. Because the schema is known (lesson 06), Spark knows each field's position, so it can read and write fields by computed offsets. This is only possible because DataFrames carry schemas — another reason schema-aware APIs outperform RDDs.

### Memory management and off-heap memory

Tungsten manages memory **explicitly** rather than leaving everything to the JVM heap and garbage collector. It can allocate large regions and manage them itself (including **off-heap** memory — memory outside the JVM's managed heap, addressed directly), placing binary rows and operator state (hash tables for aggregations/joins, sort buffers) into these managed regions. This dramatically reduces GC pressure: instead of millions of small objects the GC must scan, Spark holds a few large byte regions it manages directly. It also lets Spark account for memory precisely and decide when to **spill** to disk. Off-heap memory in particular avoids the GC entirely for that data, at the cost of manual management — a trade Tungsten makes deliberately for throughput.

### Disk spill under memory pressure

Operators that accumulate state — aggregations (hash tables), joins (hash tables), sorts (buffers) — need memory proportional to the data they hold. When that exceeds the available execution memory, Tungsten **spills** the excess to local disk: it writes part of the state out, continues, and merges later. Spilling keeps the job correct under memory pressure but is much slower than staying in memory (it adds disk I/O and serialization). Spill metrics in the Spark UI (lesson 20) are a key signal that a stage is memory-starved — often due to too-few/too-large partitions or skew (lessons 10, 11).

## Key concepts

- **UnsafeRow** — Tungsten's compact binary row layout, accessed by offset.
- **Off-heap memory** — directly managed memory outside the JVM heap; avoids GC.
- **Whole-stage code generation (codegen)** — compiling a stage of operators into one fused loop.
- **Vectorized execution** — processing column batches at a time (esp. for columnar reads).
- **Spill** — writing operator state to disk when execution memory is exhausted.
- **GC pressure** — overhead from the JVM garbage collector tracking many objects, which Tungsten minimizes.

## Detailed technical sections

### Whole-stage code generation

A traditional query engine executes a physical plan with the **Volcano/iterator model**: each operator implements `next()`, and the engine pulls rows one operator at a time through virtual function calls. For each row, you pay the cost of multiple virtual calls and intermediate object handling across operators. **Whole-stage code generation** collapses an entire chain of operators within a stage (e.g. scan → filter → project → partial-aggregate) into a *single* generated function — a tight loop that, for each row, does all the operations inline with no per-operator virtual calls and no intermediate materialization. Spark literally generates Java source code for the stage at runtime, compiles it, and runs it. The effect is execution that resembles hand-written, specialized code rather than a generic interpreter, often a large speedup. In `explain` output and the SQL UI, operators enclosed in a `WholeStageCodegen` box (or marked with `*`) are fused this way. A `groupBy` job's map side — scan, filter, partial aggregate — typically fuses into one codegen loop; the shuffle breaks the codegen boundary, and the reduce side is another fused loop.

### Why operators *outside* codegen are a warning sign

Not everything can be fused. A **Python UDF**, for instance, cannot be code-generated into the JVM loop — it requires shipping rows to a Python process. So a UDF forces Spark to *break out* of the fused codegen pipeline: materialize rows, send them across the boundary, get results back, and resume. In the plan you'll see the UDF operator *outside* the `WholeStageCodegen` boxes, splitting what would have been one fast loop into pieces with a slow detour in the middle. This is the physical-execution counterpart to lesson 07's "UDFs are optimization barriers": Catalyst can't optimize around them *and* Tungsten can't fuse through them.

### Vectorized execution

For some workloads, Spark processes data in **column batches** (vectors of values for a column) rather than row by row — **vectorized execution**. The most prominent case is the **vectorized Parquet reader**, which decodes column chunks into batches and feeds them through operators in a way that maximizes CPU cache use and enables tight, branch-predictable loops (and, on supporting hardware, SIMD-friendly patterns). Columnar batch processing reduces per-row overhead further and pairs naturally with columnar formats (lesson 13). Vectorized UDFs (Pandas/Arrow UDFs, lesson 02) extend this idea to the Python boundary: data crosses in columnar Arrow batches rather than row by row, amortizing the crossing cost.

### Why DataFrame operations beat Python UDFs (the mechanical reason)

Combine everything: a DataFrame expression of built-in functions becomes part of a **whole-stage-codegen loop** operating on **UnsafeRows** in **Tungsten-managed memory** with **minimal GC** — essentially specialized machine-friendly code over packed bytes. A Python UDF instead forces: breaking the codegen loop, serializing rows out of the binary format, an inter-process hop to a Python interpreter, slow per-row Python execution, and serialization back. The difference is not a constant factor of "Python is a bit slower"; it is the difference between fused native-speed JVM code over binary rows and an interpreted per-row round trip across a process boundary. This is *why* the course repeatedly says: prefer built-ins; vectorize UDFs when unavoidable.

### The relationship between Catalyst and Tungsten

They are complementary stages of the same compiler pipeline:

```text
Catalyst (front/middle end): logical optimization + physical planning
        → chooses a good operator tree, decides shuffles and join strategies
Tungsten (back end): code generation + binary format + memory management
        → executes that operator tree efficiently on hardware
```

Catalyst answers *"what is the least work, and in what shape?"* Tungsten answers *"given that shape, run each row as cheaply as possible."* A query benefits from both: Catalyst might prune to 3 columns and push a filter into the scan, and Tungsten then runs the remaining scan-filter-aggregate as one fused loop over binary rows. Neither alone is sufficient — a good plan executed inefficiently, or efficient execution of a bad plan, both leave performance on the table.

### How physical operators execute over partitions

The physical plan's operators run **per partition**, inside tasks (lesson 04). For each stage, Tungsten's generated code is the body of the task: a task reads its input partition (from storage or shuffle files), runs the fused operator loop over that partition's rows, and produces output (to the next shuffle or to storage). Stateful operators (aggregate hash tables, join hash tables, sort buffers) build their state *within the task*, using Tungsten-managed memory, spilling to disk if needed. So "physical execution" concretely means: *N tasks, each running the stage's generated code over one partition's binary rows, using managed memory, possibly spilling.* This is where the abstract plan finally meets CPUs, memory, and disk.

### Why CPU, memory, disk, and network all matter

Performance is bounded by whichever resource is the bottleneck for a given stage:

- **CPU** — per-row computation; codegen and binary format minimize CPU cost per row, but heavy expressions or UDFs raise it.
- **Memory** — operator state and cached data; insufficient memory causes spills (slower) or failures.
- **Disk** — spill I/O and shuffle file read/write; excessive spilling or large shuffles make disk the bottleneck.
- **Network** — shuffle data transfer between executors (lesson 09); large shuffles make the network the bottleneck.

Tungsten optimizes the CPU/memory axis (cheaper rows, less GC, fused loops). Catalyst and good design optimize the disk/network axis (fewer shuffles, less data moved). A well-tuned job balances all four; reading the Spark UI (lesson 20) tells you which one is currently limiting you.

## PySpark and SQL perspective

```python
# Built-in expression — fully code-generated over binary rows
df.withColumn("amount_chf", F.col("amount") * F.lit(0.95)) \
  .filter(F.col("amount_chf") > 100) \
  .groupBy("study_id").agg(F.sum("amount_chf"))
```

This entire map side — scan, multiply, filter, partial sum — fuses into a single whole-stage-codegen loop operating on UnsafeRows; the shuffle for `groupBy` breaks the boundary, and the final sum is another fused loop. Now the UDF version:

```python
@F.udf("double")
def to_chf(amount):           # opaque Python
    return amount * 0.95
df.withColumn("amount_chf", to_chf("amount")).filter(...).groupBy(...).agg(...)
```

Here `to_chf` cannot be code-generated or pushed; Spark breaks codegen, serializes `amount` out to a Python worker per row, runs Python, and serializes back — before it can even apply the filter (which can no longer be pushed into the scan, per lesson 07). Same result, very different physical execution: the built-in path is fused native-speed code over bytes; the UDF path is an interpreted per-row round trip. In SQL, `amount * 0.95` is a built-in expression and behaves like the first (fast) version; there is no way to *accidentally* introduce a Python boundary in pure SQL, which is one reason SQL transformations are reliably fast.

## Architectural implications

- **Prefer built-in expressions** to keep stages inside whole-stage codegen over binary rows.
- **Vectorize unavoidable UDFs** (Arrow/Pandas) so the boundary is crossed in batches, not per row.
- **Watch spill metrics** — they signal memory-starved stages; fix via partitioning/skew handling, not just more memory.
- **Right-size partitions** so each task's state fits in execution memory (avoid huge partitions that force spills; lessons 10, 11).
- **Identify the bottleneck resource** (CPU/memory/disk/network) before tuning; Tungsten already minimizes per-row CPU, so your remaining levers are usually shuffle (network/disk) and skew (memory).

## Common misconceptions

- *"Tungsten and Catalyst are the same thing."* No — Catalyst plans/optimizes; Tungsten executes efficiently.
- *"Spark stores rows as objects."* No — DataFrame data is packed binary (UnsafeRow), accessed by offset.
- *"A Python UDF is only a little slower."* No — it breaks codegen and crosses a process boundary per row.
- *"Spilling is fine, Spark handles it."* It's correct but slow; persistent spilling indicates a real problem.
- *"Off-heap memory is just more heap."* No — it's outside JVM GC, managed explicitly to avoid GC pauses.

## Summary

Tungsten is Spark's execution backend, complementary to Catalyst. Where Catalyst removes unnecessary work and chooses a good operator tree, Tungsten makes the necessary work cheap on real hardware: a compact binary row format (UnsafeRow) accessed by offset, explicit/off-heap memory management that slashes GC pressure, disk spill when memory is exhausted, whole-stage code generation that fuses a stage's operators into one tight loop, and vectorized columnar processing. This is the mechanical reason built-in DataFrame/SQL operations dramatically outperform Python UDFs — the former become fused native-speed code over binary rows, the latter break the loop and cross a process boundary per row. Physical execution is ultimately N tasks running generated code over partitions, bounded by CPU, memory, disk, or network.

## Terms to remember

- **Tungsten** — Spark's hardware-efficient execution backend.
- **UnsafeRow** — compact binary row format accessed by offset.
- **Off-heap memory** — directly managed memory outside the JVM heap; avoids GC.
- **Whole-stage code generation** — fusing a stage's operators into one compiled loop.
- **Vectorized execution** — processing column batches for cache/SIMD efficiency.
- **Spill** — writing operator state to disk under memory pressure.
