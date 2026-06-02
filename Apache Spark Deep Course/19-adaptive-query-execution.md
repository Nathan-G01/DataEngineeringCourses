# Adaptive Query Execution (AQE)

## Purpose of this lesson

Catalyst plans a query *before* it runs, using estimates from statistics (lesson 07). But estimates are often wrong — stats are stale, missing, or made unpredictable by filters and joins — and a plan optimal for the *estimated* data can be terrible for the *actual* data. **Adaptive Query Execution** fixes this by *re-planning at runtime* using real statistics measured as the query executes. This lesson explains why static planning is imperfect, what AQE does (coalescing shuffle partitions, switching join strategies, handling skew, local shuffle reads), how it relates to Catalyst, what it can and cannot fix, and why it does *not* replace architectural reasoning. It directly resolves problems raised in shuffles (09), partitioning (10), skew (11), and joins (12).

## Core mental model

```text
Static planning (Catalyst, pre-execution):  plan from ESTIMATES → may be wrong.
AQE (during execution):                      measure ACTUAL data at shuffle boundaries → REPLAN.

At each completed shuffle stage, AQE looks at real sizes/distribution and adjusts the rest of the plan:
   • coalesce too-many shuffle partitions  → fewer, right-sized tasks
   • a side turned out small               → switch sort-merge join → broadcast join
   • a partition turned out skewed         → split it into sub-partitions
```

AQE turns a one-shot plan into a **plan that adapts as ground truth arrives**.

## Main explanation

### What AQE is

AQE is a Spark SQL feature (enabled by default in modern Spark) that allows the physical plan to be **revised during execution** based on **runtime statistics**. Normally a physical plan is fixed before any task runs. AQE inserts decision points at **shuffle boundaries** (stage boundaries — lesson 04): when a stage completes its shuffle write, Spark has *exact* statistics about the data produced (sizes per partition, total size, distribution). AQE uses these real numbers to re-optimize the *remaining* plan before launching the next stage. So a query under AQE may execute a different physical plan than the one `explain` initially showed — the final plan in the SQL UI reflects the adaptations.

### Why static query planning is imperfect

Pre-execution planning relies on **estimates** that are frequently inaccurate:

- **Stale or missing statistics.** Tables change; stats lag. Without stats, Catalyst guesses (often conservatively), e.g., assuming a table is large when it isn't.
- **Estimates degrade through operators.** Even with good base-table stats, estimating the size *after* a chain of filters and joins is hard — selectivity estimation compounds errors. A filter might remove 99% of rows, but the optimizer may not know that, so it plans for the full size.
- **Data-dependent effects.** Skew, cardinality explosion, and actual value distributions are nearly impossible to predict statically.

The consequence is concrete planning mistakes: choosing a sort-merge join when a broadcast would have worked (because the post-filter size was underestimated as large), setting a fixed 200 shuffle partitions that's wildly wrong for the actual data, or failing to anticipate a skewed partition. AQE exists because these mistakes are common and costly, and because the *true* information needed to avoid them only becomes available at runtime.

### Runtime statistics

The fuel for AQE. When a shuffle map stage finishes writing its output, Spark knows, exactly: the total size of the shuffled data, the size of each shuffle partition, and therefore the distribution (including whether some partitions are far larger — skew). These are not estimates; they're measured facts about the data that just moved. AQE consumes these at the shuffle boundary to make its three main adaptations (below). The reliance on shuffle boundaries is why AQE's adaptations all relate to operations that involve shuffles — aggregations and joins — and why a shuffle-free query has nothing for AQE to adapt.

## Key concepts

- **Coalescing shuffle partitions** — merging too-many small post-shuffle partitions into fewer right-sized ones.
- **Dynamic join strategy switching** — converting a planned sort-merge join to a broadcast join when a side is actually small.
- **Skew join handling** — splitting an oversized skewed shuffle partition into sub-partitions processed in parallel.
- **Local shuffle reader** — reading shuffle data locally when a shuffle becomes unnecessary after a broadcast switch.
- **Runtime statistics** — exact data sizes/distribution measured at shuffle boundaries.

## Detailed technical sections

### Coalescing shuffle partitions

The most broadly useful AQE feature. Recall the default `spark.sql.shuffle.partitions = 200` (lesson 09) is a *fixed* number applied regardless of actual data size, causing too-many tiny partitions for small post-shuffle data (overhead, tiny output files) or too-few huge ones for large data. AQE **coalesces** dynamically: after the shuffle, it sees the real total size and *merges adjacent small partitions* into fewer partitions of a target size (e.g., combining 200 tiny partitions into 8 reasonably-sized ones). This automatically right-sizes post-shuffle parallelism without manual tuning — eliminating much of the historical need to set `shuffle.partitions` per job, and directly reducing the small-files and tiny-tasks problems (lessons 10, 13). You can start with a high partition count and let AQE coalesce down to fit the data.

### Dynamically switching join strategies

The highest-impact AQE feature for joins. A join may be planned as a **sort-merge join** because pre-execution estimates judged both sides large (lesson 12). But after the input stages run — applying filters that the optimizer couldn't fully account for — one side may turn out **small enough to broadcast**. AQE measures the actual post-shuffle/post-filter size and, if a side is below the broadcast threshold, **switches the sort-merge join to a broadcast hash join at runtime**, eliminating the expensive shuffle of the large side. This fixes the single most common and costly planning mistake — a *missed broadcast* due to underestimated selectivity — using ground truth. It's why the *final* plan in the SQL UI may show a `BroadcastHashJoin` where the initial `explain` showed a `SortMergeJoin`. This adaptation alone often delivers large speedups on real pipelines where filters dramatically shrink a side before a join.

### Skew join handling

AQE detects **skewed partitions** in a join's shuffle output (lesson 11): using runtime statistics, it identifies partitions far larger than the median and **splits each oversized partition into multiple sub-partitions**, processed by multiple tasks in parallel (replicating the matching partition from the other side as needed). This parallelizes the long-tail task that would otherwise stall the stage, mitigating join skew **automatically** without you salting keys manually. It's the cheapest skew mitigation (just enable AQE) and handles moderate skew well — but it has thresholds and limits (below), so extreme skew or skew outside its detection (non-join skew, cardinality explosion) may still need manual handling.

### Local shuffle reader

A supporting optimization. When AQE converts a sort-merge join to a broadcast join at runtime, the shuffle that the sort-merge join would have required is no longer needed in the usual all-to-all way. The **local shuffle reader** lets the resulting stage read shuffle data **locally** (each reducer reads the map output on its own node) rather than performing a full network shuffle, avoiding unnecessary data movement after the strategy switch. It's an internal efficiency that makes the join-strategy adaptation cheaper; you don't configure it directly, but it's part of why AQE's join switching is so effective.

### Relationship between AQE and Catalyst

AQE is **an extension of Catalyst's physical planning into the runtime**, not a replacement. Catalyst still does all the logical optimization and produces the initial physical plan (lesson 07); AQE then **re-invokes optimization at shuffle boundaries** with real statistics to refine the *remaining* physical plan. Think of it as Catalyst getting a second (and third, etc.) chance to optimize, each time armed with facts instead of estimates. The rule-based and cost-based machinery is the same; what changes is that some decisions are *deferred* until the data is measurable. So AQE doesn't make Catalyst obsolete — it makes Catalyst's cost-based decisions *accurate* by feeding them runtime truth where estimates would otherwise be used.

### What AQE can fix

- **Wrong shuffle partition count** → coalesces to right-size post-shuffle parallelism and output files.
- **Missed broadcast joins** → switches sort-merge to broadcast when a side is actually small (post-filter).
- **Moderate join skew** → splits skewed partitions to parallelize the long tail.
- **Unnecessary shuffles after a join switch** → local shuffle reader avoids the movement.

These cover a large fraction of the most common real-world performance problems, which is why enabling AQE is a near-universal first step and why modern Spark enables it by default.

### What AQE cannot fix

AQE is powerful but bounded, and over-trusting it is an anti-pattern (lesson 22):

- **Bad data layout.** AQE can't fix a table stored as giant CSVs, or millions of tiny files, or high-cardinality partitioning (lessons 10, 13) — those are storage decisions made before the query runs.
- **Fundamentally excessive shuffles.** AQE optimizes around shuffle boundaries but can't eliminate a shuffle your logic genuinely requires, nor undo a design that re-shuffles data needlessly.
- **Extreme skew / cardinality explosion.** Its skew handling has thresholds; pathological skew or a many:many cardinality explosion (lesson 12) may exceed what it splits effectively, still needing salting, hot-key isolation, or query redesign.
- **Non-shuffle problems.** AQE acts at shuffle boundaries; it can't help shuffle-free issues like a scan reading too much data because filters weren't pushed (often itself caused by a UDF barrier, lesson 07).
- **Logic/correctness or unnecessary work.** AQE optimizes *how* the plan runs, not *whether* you asked for the right thing — it won't remove a redundant join or a needlessly wide computation.

The boundary: AQE corrects *planning estimate errors at runtime*; it does not correct *architectural and data-layout decisions* made by you.

### Why AQE does not replace architectural reasoning

This is the key takeaway. AQE is a safety net for *planning* mistakes, not a substitute for *designing well*. The biggest performance levers — choosing columnar/Delta storage, right-sizing files, partitioning on sensible columns, structuring joins around cardinality, avoiding UDF barriers, minimizing shuffles, anticipating skew from domain knowledge (lesson 11) — are all decisions AQE cannot make for you because they're made *before* the query or are about *what* you compute. AQE makes a *reasonably-designed* query robust to estimation error; it cannot rescue a *badly-designed* one. The right mental stance: design the pipeline well using all the prior lessons, **and** enable AQE so that runtime reality corrects the residual planning estimates. Treating AQE as "I don't need to think about performance" is exactly the anti-pattern (lesson 22) it most invites.

### How AQE affects explain plans and the Spark UI

Because AQE changes the plan at runtime, the `explain` output you see *before* running shows an `AdaptiveSparkPlan` node marked `isFinalPlan=false` — the *initial* plan, which may be adapted. After execution, the **SQL/DataFrame tab** in the Spark UI (lesson 20) shows the **final, adapted plan** (`isFinalPlan=true`), which can differ from the initial one — e.g., a `BroadcastHashJoin` where the initial plan had `SortMergeJoin`, `AQEShuffleRead` nodes indicating coalesced/local reads, and split partitions for skew handling. This is why you should read the *final* plan in the UI, not just the pre-run `explain`, to understand what actually executed. The presence of `AdaptiveSparkPlan` and `AQEShuffleRead` operators is the signature of AQE at work.

## PySpark and SQL perspective

```python
# Ensure AQE is on (default in modern Spark; shown explicitly for clarity)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# A join planned as sort-merge that AQE may switch to broadcast after filtering
big = spark.table("orders").filter("order_date >= '2026-01-01'")   # filter shrinks 'big' a lot
result = big.join(spark.table("customers"), "customer_id")          # initial plan: SortMergeJoin
result.write.format("delta").saveAsTable("gold_recent_orders")
```

The optimizer might plan a `SortMergeJoin` because, pre-execution, it estimated `orders` (and thus the filtered side) as large. But the `order_date` filter may shrink it dramatically — a fact only known *after* the scan/filter stage runs. AQE measures the actual size at the shuffle boundary and, if the filtered side is now small enough, **switches to a `BroadcastHashJoin`**, broadcasting it and avoiding the big shuffle entirely — and if `customers` is the small side, that's broadcast instead. AQE also coalesces the post-join shuffle partitions to match the actual (smaller) result size, producing fewer, right-sized output files. The initial `explain` shows `AdaptiveSparkPlan (isFinalPlan=false)` with `SortMergeJoin`; the SQL UI after running shows the final plan with `BroadcastHashJoin` and `AQEShuffleRead`. In SQL, the same query (`SELECT ... FROM orders WHERE order_date >= ... JOIN customers USING(customer_id)`) gets identical AQE treatment — the adaptations are engine behavior, not syntax. Note what AQE *didn't* do: it didn't change the storage layout, didn't remove the shuffle the join inherently needs (it converted it via broadcast), and wouldn't have helped if `orders` were stored as one giant non-splittable CSV — those remain your design responsibility.

## Architectural implications

- **Enable AQE** (it's default-on in modern Spark) — it's the cheapest, broadest performance win and fixes common planning errors automatically.
- **Read the final plan in the Spark UI**, not just pre-run `explain`, since AQE adapts at runtime.
- **Let AQE coalesce shuffle partitions** rather than hand-tuning `spark.sql.shuffle.partitions` for every job.
- **Rely on AQE for moderate skew**, but keep manual mitigations (broadcast, salting, hot-key isolation) for extreme cases (lesson 11).
- **Still design well** — storage layout, partitioning, join structure, and shuffle minimization are yours to get right; AQE only corrects estimate errors, not design errors.

## Common misconceptions

- *"AQE means I don't need to worry about performance."* No — it fixes planning estimate errors, not data-layout or design mistakes.
- *"The `explain` output is what runs."* Under AQE, the *final* plan (in the UI) may differ from the initial `explain`.
- *"AQE eliminates shuffles."* It optimizes around them (coalesce, broadcast-switch, skew-split) but can't remove a genuinely needed shuffle.
- *"AQE fully solves skew."* It handles moderate join skew automatically; extreme skew and cardinality explosion still need manual work.
- *"AQE replaces Catalyst."* No — it's Catalyst's optimization extended into runtime with real statistics.

## Summary

Adaptive Query Execution re-optimizes the physical plan *during* execution using exact runtime statistics gathered at shuffle boundaries, correcting the inevitable inaccuracies of pre-execution estimates. Its main adaptations are coalescing too-many shuffle partitions into right-sized ones, dynamically switching sort-merge joins to broadcast joins when a side turns out small (the highest-impact fix, addressing missed broadcasts from underestimated selectivity), and splitting skewed join partitions to parallelize long-tail tasks — supported by the local shuffle reader. AQE is an extension of Catalyst into the runtime, not a replacement, and it's default-on in modern Spark. It fixes *planning estimate errors* but cannot fix *data-layout and design decisions* — bad storage, high-cardinality partitioning, fundamentally excessive shuffles, extreme skew, or non-shuffle problems remain your responsibility. Enable AQE, read the final adapted plan in the UI, and still design pipelines well: AQE makes good designs robust, it does not rescue bad ones.

## Terms to remember

- **AQE** — runtime re-optimization of the physical plan using actual statistics.
- **Runtime statistics** — exact data sizes/distribution measured at shuffle boundaries.
- **Coalesce shuffle partitions** — merge too-many small post-shuffle partitions into right-sized ones.
- **Dynamic join switching** — sort-merge → broadcast at runtime when a side is actually small.
- **Skew join handling** — splitting oversized skewed partitions for parallel processing.
- **AdaptiveSparkPlan / AQEShuffleRead** — plan/UI markers of AQE adaptations.
