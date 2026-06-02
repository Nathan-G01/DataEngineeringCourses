# Data Skew

## Purpose of this lesson

Data skew is the most common reason a Spark job that "should be fast" is mysteriously slow — and the reason adding more executors often does nothing. Skew is when work is distributed *unevenly* across partitions, so a few tasks do most of the work while the rest finish quickly and sit idle. This lesson explains why distributed systems are inherently vulnerable to skew, where it comes from (hot keys, nulls, lopsided business domains), how it manifests in joins and aggregations, how to recognize it in the Spark UI, why throwing hardware at it fails, and the full menu of mitigations with their trade-offs. It builds directly on shuffles (lesson 09) and partitioning (lesson 10).

## Core mental model

```text
Balanced (good):   each task ≈ equal work, stage ends when they all finish together
   ▏▏▏▏▏▏▏▏  ← 8 tasks, similar length

Skewed (bad):      one task has most of the data; stage waits for it (the "long tail")
   ▏▏▏▏▏▏▏███████████████████  ← 7 quick tasks, 1 enormous task
                              ↑ the whole stage is as slow as this one task
```

A stage finishes only when its **slowest** task finishes. Skew makes one task the bottleneck for the entire stage.

## Main explanation

### What data skew is

**Data skew** is an uneven distribution of data across partitions. After a shuffle, rows are assigned to partitions by `hash(key) % n` (lesson 09). If the *keys* are evenly distributed, partitions are balanced. But real data is rarely uniform: some key values appear vastly more often than others. All rows for a popular key hash to the *same* partition, so that one partition becomes huge while others stay small. The task processing the huge partition runs far longer than the rest. Since a stage isn't done until every task is done, the entire stage — and the job — waits on that one long task.

### Why distributed systems suffer from skew

The premise of distributed processing is *divide the work evenly across machines*. Skew violates that premise: when one piece of the divided work is far larger than the others, the parallelism breaks down. You can have 100 executors, but if one partition holds 80% of the relevant data, then 99 executors finish their small tasks and wait while one executor grinds through the giant partition alone. Distributed systems are only as fast as their most overloaded worker for a given stage. Skew is fundamentally a *load-balancing* failure, and it is endemic because the natural distribution of real-world keys (customers, products, studies, events) is almost never uniform — it usually follows a power law where a few keys dominate.

### Where skew comes from

- **Hot keys.** A few key values are wildly more frequent: one giant customer placing millions of orders, one popular product, one dominant study in a clinical dataset. All their rows land in one partition.
- **Null-heavy keys.** Nulls in a join/group key often all hash to the same partition. If a large fraction of rows have a null key (missing foreign keys, unmapped records), that single null partition becomes enormous.
- **Uneven business domains.** The domain itself is lopsided: 90% of traffic from one region, most events from one device type, the bulk of records from one tenant. The data faithfully reflects a skewed reality, and that skew flows into partitioning.
- **Default/sentinel values.** Placeholder values like `-1`, `"UNKNOWN"`, `0`, or an epoch date applied to many rows act exactly like a hot key.

## Key concepts

- **Hot key** — a key value far more frequent than others; concentrates data into one partition.
- **Long-tail task** — the one (or few) task in a stage that runs much longer than the rest.
- **Skew in joins** — a hot join key inflates one reduce partition (often worsened by cardinality explosion).
- **Skew in aggregations** — a hot group key concentrates one group's rows (partly mitigated by map-side combine).
- **Salting** — adding randomness to a hot key to spread it across partitions.
- **AQE skew handling** — runtime splitting of detected skewed partitions (lesson 19).

## Detailed technical sections

### Skew in joins

Joins are the most painful place for skew. A non-broadcast join shuffles **both** sides by the join key so matching rows meet (lesson 12). If one key is hot on the large side, the reduce task for that key must process a huge number of rows. Worse, if the key is also duplicated on the *other* side, the join produces a **cardinality explosion**: a hot key with `m` rows on the left and `k` rows on the right yields `m × k` output rows, all in one partition. So join skew can be doubly bad: a huge input concentration *and* a multiplicative output blowup, all funneled through a single task. This is why a join on a key with one dominant value can take hours while the rest of the data joins in seconds.

### Skew in aggregations

Aggregations (`groupBy`) are somewhat protected by **map-side combine** (lesson 09): each input partition pre-aggregates its rows, so only partial results per key are shuffled. For a hot group key, this dramatically reduces the shuffled volume — instead of moving all the hot key's rows, Spark moves one partial per partition. So *simple* aggregations (count, sum) over a hot key are often fine. The exception is aggregations that *can't* be partially combined — e.g., collecting all values into a list, computing exact distinct counts, or median-like operations — where the hot key's full data must reach one reduce task, reproducing join-style skew. So "does this aggregation support map-side combine?" determines whether skew bites.

### Symptoms in the Spark UI

Skew has a recognizable signature (lesson 20):

- In the **Stages tab**, the stage's **task duration distribution** is lopsided: the *max* task time is far larger than the *median* (e.g., median 5s, max 400s). The summary metrics show a huge gap between the 75th percentile and the max.
- The **shuffle read size** per task is similarly lopsided — one task reads gigabytes while others read megabytes.
- The stage's timeline shows most tasks completing early and one (or a few) running long — the **long tail**.
- **Spill** metrics may spike on the skewed task, because its oversized partition exceeds execution memory (lesson 08).

The diagnostic phrase to internalize: *"the stage is 99% done but won't finish."* That is almost always one straggler task chewing through a skewed partition.

### Why a job can be 99% complete but still slow

Because the progress bar reflects *completed tasks*, not *completed work*. If 199 of 200 tasks finished in the first minute and the 200th task holds 80% of the data, the job sits at "199/200, 99%" for a long time while that final task runs. The near-complete progress is misleading: the *amount of work* remaining is large even though the *number of tasks* remaining is one. This is the visual hallmark of skew and a frequent source of "the job is stuck" reports that are really "the job is skewed."

### Why adding more executors does not solve skew

This is the counterintuitive, crucial point. More executors add more *slots*, letting more tasks run in parallel. But skew's problem is that **one task** is enormous — and a single task runs on a single slot on a single executor; it cannot be split across executors just by having more of them. Adding executors lets the *small* tasks finish even faster (they were never the problem) while the giant task runs exactly as slowly as before, now with even more idle executors waiting on it. You can double or triple the cluster and the skewed stage barely improves, because its critical path is one indivisible oversized task. Skew must be fixed by **rebalancing the data**, not by adding hardware. Recognizing this saves enormous wasted spend on bigger clusters that don't help.

## Mitigation strategies and their trade-offs

There is no single fix; you choose based on the skew's source and shape.

- **Filter early.** Remove irrelevant rows (including, where valid, the hot/sentinel rows) *before* the shuffle so less skewed data reaches the wide operation. *Trade-off:* only applies when the hot data is genuinely unneeded; doesn't help if you need the hot key's results.

- **Handle nulls separately.** If a null join/group key causes skew, filter out null-key rows, process them separately (they can't match in an inner join anyway), and union results if needed. *Trade-off:* adds pipeline complexity; only addresses null-driven skew, and you must preserve correct semantics (e.g., left joins where null-key rows must be retained).

- **Salting.** Append a random "salt" (e.g., 0–N) to the hot key on the large side, replicate the small side across all salt values, join on `(key, salt)`, then aggregate away the salt. This *spreads* one hot key across N partitions, so N tasks share the work instead of one. *Trade-off:* it's invasive (you rewrite the join/aggregation), it replicates the small side N-fold, and you must remove the salt afterward; choosing N is a tuning exercise. Powerful but the heaviest manual technique.

- **Pre-aggregation.** Aggregate at a finer grain first (which combines map-side), reducing the hot key's volume before the expensive operation. *Trade-off:* only works when the operation is associative/combinable; doesn't help non-combinable aggregations or joins that need raw rows.

- **Broadcast joins.** If the *other* side of a skewed join is small, broadcast it (lesson 12). This eliminates the shuffle entirely — no redistribution by key means no skewed reduce partition. *Trade-off:* only possible when one side fits in memory for broadcast; the most elegant fix when it applies.

- **AQE skew handling.** Adaptive Query Execution (lesson 19) detects skewed shuffle partitions at runtime and *splits* them into multiple smaller sub-partitions processed by multiple tasks, automatically. *Trade-off:* it's automatic and often sufficient for moderate join skew, but it has thresholds (very extreme skew or non-join skew may exceed what it handles), and it can't fix cardinality explosion or skew that needs domain logic. Enable it first; it's the cheapest mitigation.

- **Splitting hot keys.** Identify the few hot keys explicitly, process them with a tailored strategy (their own broadcast, their own salted path), and process the long tail of normal keys with the default path, then union. *Trade-off:* most targeted and effective for a known small set of hot keys, but requires knowing which keys are hot and adds branching logic to maintain.

A reasonable escalation order: **enable AQE** → **filter/handle nulls** → **broadcast the small side if possible** → **pre-aggregate** → **salt or split hot keys** for stubborn, extreme cases.

### How to detect skew conceptually before writing code

You can often *predict* skew without running anything by reasoning about the data domain (lesson 22). Ask: *What is the cardinality and distribution of each join/group key? Is there a dominant value — a giant customer, a default sentinel, a flood of nulls in this foreign key? Does the business reality concentrate on a few entities?* If you know one study/customer/tenant dominates, you should *expect* skew on any key tied to it and design around it from the start (filter, broadcast, or split) rather than discovering it as a stuck job. Profiling the key distribution (`groupBy(key).count().orderBy(desc("count"))`) on a sample confirms suspicions cheaply. The best engineers anticipate skew from domain knowledge before the first slow run.

## PySpark and SQL perspective

Consider a clinical dataset where one mega-study dominates:

```python
# events: billions of rows; study "S001" accounts for 70% of them.
events.groupBy("study_id").count()          # OK-ish: map-side combine shrinks S001's volume
events.groupBy("study_id").agg(F.collect_list("payload"))  # BAD: S001's full payloads funnel to one task
```

The simple `count` survives because partial counts are combined before the shuffle, so S001 contributes one partial per input partition, not 70% of all rows, to its reduce task. The `collect_list` cannot combine — every S001 payload must reach the single S001 reduce partition, producing a massive long-tail task and likely spills/OOM. For a skewed *join*:

```python
# orders heavily skewed on customer_id (one giant customer); customers is small
orders.join(customers, "customer_id")       # SortMergeJoin → skewed reduce partition for the big customer
orders.join(F.broadcast(customers), "customer_id")  # BroadcastHashJoin → no shuffle, no skewed partition
```

Broadcasting `customers` removes the shuffle on the join key entirely, so the hot `customer_id` never concentrates into one reduce task — the most elegant fix, available because `customers` is small. If both sides were large, you'd salt the hot key or rely on AQE skew-join handling. In SQL, the same applies: `/*+ BROADCAST(customers) */` hints the broadcast, and AQE may convert a sort-merge join to broadcast or split skewed partitions at runtime regardless of syntax.

## Architectural implications

- **Profile key distributions** as part of design; treat known dominant entities as expected skew sources.
- **Prefer combinable aggregations** (count/sum) over non-combinable ones (collect_list/exact-distinct) on potentially hot keys; if you need the latter, plan a mitigation.
- **Broadcast the small side** whenever a skewed join has one small side — it sidesteps skew entirely.
- **Enable AQE** as the baseline automatic defense, but don't assume it fixes extreme or non-join skew.
- **Don't scale the cluster to fix skew** — rebalance the data instead; bigger clusters waste money on idle executors.
- **Isolate hot keys** with dedicated paths when a few known keys dominate.

## Common misconceptions

- *"More executors will speed up a skewed stage."* No — one giant task can't be split across executors; the rest just idle.
- *"All aggregations suffer equally from skew."* No — combinable aggregations are largely protected by map-side combine; non-combinable ones aren't.
- *"99% complete means almost done."* With skew, the last 1% of tasks can hold most of the work.
- *"AQE makes skew a non-issue."* It handles moderate join skew automatically but not extreme skew, cardinality explosion, or domain-specific cases.
- *"Skew is rare."* Real-world key distributions are almost always skewed; uniform data is the exception.

## Summary

Data skew is uneven data distribution across partitions, usually from hot keys, null-heavy keys, or lopsided business domains. Because a stage finishes only when its slowest task does, a single oversized partition creates a long-tail task that stalls the whole stage — visible as a "99% done but stuck" job and a huge gap between median and max task time in the UI. Adding executors does not help, because one task cannot be split across them; skew must be fixed by rebalancing data. Aggregations with map-side combine resist skew; joins and non-combinable aggregations are most vulnerable, especially with cardinality explosion. Mitigations — filtering early, handling nulls separately, broadcasting the small side, pre-aggregating, salting, splitting hot keys, and AQE skew handling — each have trade-offs; choose by the skew's source, and anticipate skew from domain knowledge before running.

## Terms to remember

- **Data skew** — uneven data distribution across partitions.
- **Hot key** — a disproportionately frequent key value concentrating data in one partition.
- **Long-tail task** — the straggler task that holds up a stage.
- **Cardinality explosion** — multiplicative row blowup from duplicated join keys (worsens skew).
- **Salting** — adding randomness to a hot key to spread it across partitions.
- **AQE skew handling** — runtime splitting of skewed shuffle partitions.
