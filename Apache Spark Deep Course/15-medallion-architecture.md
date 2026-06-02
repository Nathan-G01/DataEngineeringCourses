# Medallion Architecture

## Purpose of this lesson

The medallion architecture (Bronze → Silver → Gold) is the dominant organizing pattern for lakehouse pipelines, and Spark is the engine that moves data between its layers. But medallion is an *architectural pattern*, not a Spark feature — it's a way of structuring responsibilities and data quality across stages, and its value comes from discipline, not from any API. This lesson explains the purpose of each layer, what data looks like at each, the engineering properties that make the pattern robust (replayability, idempotency, incremental vs full processing, quality gates, schema contracts, lineage), how Spark's execution concepts shape good medallion design, and the common ways teams get it wrong. It builds on Delta (lesson 14) — the layers are Delta tables — and feeds the architectural thinking of lesson 22.

## Core mental model

```text
SOURCE → ┌─────────┐ → ┌─────────┐ → ┌─────────┐ → consumers (BI, ML, apps)
         │ BRONZE  │   │ SILVER  │   │  GOLD   │
         │ raw     │   │ clean   │   │ business│
         │ as-is   │   │ conformed│  │ products│
         └─────────┘   └─────────┘   └─────────┘
   source-aligned   standardized    business-ready
   (replayable)     (deduped/typed) (aggregated/modeled)

Each arrow is a Spark job. Each box is a Delta table. Data gets more refined and more opinionated left→right.
```

The pattern's discipline: **never lose the ability to reprocess** (Bronze is your replay anchor), and **push opinion rightward** (raw stays raw; business logic lives in Gold).

## Main explanation

### Raw / Bronze

The **Bronze** layer holds data **as ingested from the source, with minimal or no transformation** — source-aligned, append-only, schema-faithful to the origin. Its purpose is to be a **complete, durable, replayable record of what arrived**. You add ingestion metadata (load timestamp, source file name, batch id) but you do *not* clean, dedupe, reshape, or apply business rules. Bronze is your insurance policy: if a downstream transformation has a bug, or requirements change, you can **reprocess from Bronze** without re-fetching from the (possibly unavailable or changed) source. Raw JSON/CSV from the source is typically landed and then stored as Delta in Bronze so it's queryable and transactional, but its *content* stays raw.

### Silver

The **Silver** layer holds **cleaned, conformed, standardized** data. Here you apply the transformations that make data *correct and consistent* but still relatively general-purpose: parsing/typing (JSON → typed columns), deduplication, null/quality handling, standardizing formats and units, resolving keys, joining reference/lookup data, and enforcing schema. Silver is **source-aligned-but-cleaned**: it still reflects the source entities (one Silver table per logical entity), not business aggregates. Multiple Gold products can be built from the same Silver tables, so Silver is the **reusable, trustworthy foundation**. Crucially, Silver should contain *cleaning and conforming* logic, not *business* logic — putting business rules here couples Silver to particular consumers and undermines its reusability.

### Gold

The **Gold** layer holds **business-ready data products**: aggregated, modeled, and shaped for specific consumption — star schemas/dimensional models, pre-computed aggregates, KPIs, feature tables, report-specific datasets. Gold is **opinionated and consumer-driven**: each Gold table exists to serve a particular query pattern, dashboard, or model. This is where business logic and aggregation live. Because Gold is shaped for consumers, its design must start from **how it will be queried** (lesson 22) — partitioning, clustering, and aggregation granularity chosen to make the consuming queries fast.

### Why more refinement and opinion moves rightward

The flow embodies a principle: **defer opinion**. Bronze makes no claims beyond "this is what arrived." Silver claims "this is clean and consistent." Gold claims "this answers a business question." Each step adds interpretation and loses raw fidelity, which is why you keep the earlier layers — they're the recoverable ground truth. A change in business definition affects only Gold (rebuild from Silver); a change in cleaning logic affects Silver and Gold (rebuild from Bronze); only a change in what the *source* sends requires re-ingestion. This layering localizes the blast radius of change and bugs.

## Key concepts

- **Source-aligned data** — structured like the source (Bronze/Silver), not the business.
- **Standardized data** — typed, deduped, conformed (Silver).
- **Business-ready products** — aggregated, modeled, consumer-shaped (Gold).
- **Replayability** — ability to reprocess a layer from the one before it.
- **Idempotency** — re-running a job produces the same result, not duplicates.
- **Incremental vs full processing** — processing only new/changed data vs reprocessing everything.
- **Quality gates / schema contracts** — validations and schema guarantees between layers.
- **Lineage** — knowing what produced each dataset and from what.

## Detailed technical sections

### Replayability

**Replayability** is the ability to rebuild a downstream layer by reprocessing from an upstream one. It depends on Bronze being a faithful, retained record of source data. If Bronze is complete and immutable-ish (append-only), then any bug or requirement change downstream is recoverable: fix the Silver/Gold logic and reprocess. Lose Bronze (or transform it lossily) and you lose this safety net — a change might require re-fetching from a source that no longer has the data. Replayability is the single strongest reason Bronze must stay raw and complete. It's why "over-transforming raw" (below) is a cardinal sin: it destroys your ability to replay.

### Idempotency

**Idempotency** means running a job multiple times yields the same end state — no duplicate rows, no double-counting. This is essential because jobs fail and retry (lesson 18), and orchestrators may re-run a task. Non-idempotent appends ("just insert the new batch") create duplicates on retry. The standard mechanism is **MERGE/upsert into Delta** (lesson 14): instead of blindly appending, you upsert keyed on a business or natural key, so re-processing the same input updates-in-place rather than duplicating. Designing each layer's write as idempotent (overwrite a partition, or MERGE on a key) makes the whole pipeline safe to retry and re-run — a prerequisite for reliable production operation.

### Incremental processing vs full reloads

- **Full reload**: reprocess the entire dataset each run. Simple and robust (naturally idempotent if you overwrite), but expensive and slow at scale, and increasingly impractical as data grows.
- **Incremental processing**: process only new or changed data each run (new files, new partitions, CDC changes), MERGE-ing results into the target. Efficient and scalable, but more complex — you must track what's new (high-water marks, Delta change feeds, partition boundaries), handle late-arriving data, and ensure idempotency so reprocessing an overlapping increment doesn't duplicate.

The choice is a trade-off (lesson 22): start with full reloads for small/simple data and correctness; move to incremental as volume demands, accepting the operational complexity. Delta's MERGE and change data feed are the tools that make incremental processing safe and idempotent.

### Data quality gates

**Quality gates** are validation checkpoints between layers that reject or quarantine bad data before it propagates. Between Bronze and Silver you might check: required fields present, types valid, values in expected ranges, referential integrity, duplicate rates within tolerance, row-count sanity. Bad records can be **quarantined** (written to an error table for inspection) rather than silently dropped or allowed through. Quality gates prevent the classic failure where garbage flows unnoticed into Gold and shows up as wrong numbers on a dashboard. They embody the principle that *successful job completion is not the same as correct data* (lesson 20) — a job can succeed while producing nonsense, so explicit data-quality checks are part of the pipeline, not optional.

### Schema contracts

A **schema contract** is an explicit, enforced agreement about a table's schema — its columns, types, and constraints — so that producers and consumers can rely on stability. Delta's **schema enforcement** (lesson 14) implements contracts mechanically: writes that violate the schema are rejected, protecting consumers from surprise breakage, while controlled **schema evolution** allows intentional, versioned change. Contracts are most important at layer boundaries (Silver and Gold tables that many consumers depend on): they turn "hope the upstream didn't change" into "the platform guarantees the shape." Without contracts, a silent upstream schema change corrupts everything downstream; with them, the violation is caught at the boundary.

### Metadata and lineage

**Lineage** is the record of what data produced what — which source files became which Bronze rows, which Bronze/Silver tables fed which Gold product, and via which job/version. Lineage is essential for debugging (trace a wrong number back to its source), auditing/compliance (prove provenance), and impact analysis (what breaks if this source changes?). Some lineage is captured automatically by the platform/catalog; some you add deliberately (load metadata columns, run ids, Delta versions). Combined with Delta **time travel** (lesson 14), lineage lets you reconstruct *what the data was and where it came from* at any point — a powerful debugging and governance capability. Clear **ownership** of each table (especially Gold) is part of this metadata discipline.

### Why medallion is an architectural pattern, not a Spark feature

There is no "medallion API" in Spark. Medallion is a *convention* for organizing tables and responsibilities; Spark merely executes the jobs that move data between layers, and Delta provides the reliable tables. You could implement medallion with other engines and formats; you could (badly) ignore medallion while using Spark. The value is in the *discipline* — keeping raw raw, conforming in Silver, modeling in Gold, enforcing contracts and quality gates, ensuring replayability and idempotency. Recognizing this prevents the mistake of thinking the pattern is automatic; it must be *designed and enforced* by you, using Spark and Delta as tools.

### How Spark execution concepts shape medallion design

The earlier lessons directly inform good medallion design:

- **Shuffles and joins (09, 12)**: Silver-layer conforming often joins reference data — broadcast small dimensions, watch cardinality. Gold aggregations shuffle — design group keys and use map-side combine.
- **Partitioning and small files (10, 13)**: each layer's write must control file size (compaction, lesson 14) and avoid high-cardinality partitioning. Bronze from streaming/incremental loads is a small-file magnet — compact it.
- **Skew (11)**: dominant entities (a mega-customer, a giant study) cause skew in Silver joins and Gold aggregations — anticipate and mitigate.
- **Storage layout (13, 14)**: convert raw JSON/CSV (Bronze) to columnar Delta (Silver) so downstream layers query cheaply; cluster/partition Gold for its consumers' filters.
- **Caching/materialization (17)**: intermediate Silver tables *are* materialization — writing to Delta between layers truncates lineage and enables reuse/replay, often better than caching across a long pipeline.
- **Fault tolerance/idempotency (18)**: MERGE-based idempotent writes make retries safe.

In short, the medallion layers are where every performance and reliability concept in this course gets applied to a real pipeline shape.

## Common mistakes

- **Over-transforming raw (Bronze).** Cleaning, deduping, or reshaping in Bronze destroys replayability — you can no longer reprocess from a faithful source record. Keep Bronze raw; do cleaning in Silver.
- **Treating Silver as a dump.** Appending everything to Silver without conforming, deduping, or enforcing schema makes it just a messier Bronze, and Gold inherits the mess. Silver must be genuinely clean and consistent, with contracts and quality gates.
- **Putting business logic too early.** Embedding consumer-specific aggregations or business rules in Silver couples it to particular Gold products and prevents reuse. Business logic belongs in Gold; Silver stays general-purpose.
- **Unclear ownership of Gold.** Gold tables proliferate without clear owners, definitions, or alignment to consumer needs, producing duplicate/conflicting "truths." Each Gold product needs an owner, a defined purpose, and a query pattern it serves.
- **No replay strategy.** Pipelines designed as one-way append with no way to reprocess become unrecoverable when bugs or changes appear. Design for replay (retained Bronze) and idempotency (MERGE) from the start.

To these, add: ignoring small files (each layer accumulates them), no quality gates (garbage reaches dashboards), and designing Gold without knowing the query pattern (lesson 22's anti-patterns).

## PySpark and SQL perspective

```python
# BRONZE: land raw source as-is (no cleaning), add ingestion metadata, store as Delta
(spark.read.json("Files/landing/events/*.json")
   .withColumn("_ingested_at", F.current_timestamp())
   .withColumn("_source_file", F.input_file_name())
   .write.format("delta").mode("append").saveAsTable("bronze_events"))

# SILVER: clean, type, dedupe, conform; enforce schema; idempotent MERGE on a key
silver = (spark.table("bronze_events")
   .withColumn("event_ts", F.to_timestamp("ts"))      # parse/type
   .dropDuplicates(["event_id"])                       # dedupe
   .filter(F.col("event_id").isNotNull()))             # quality gate
# ...MERGE into silver_events on event_id (idempotent upsert)

# GOLD: business aggregate shaped for a specific dashboard's query pattern
(spark.table("silver_events")
   .groupBy("study_id", F.date_trunc("day", "event_ts").alias("day"))
   .agg(F.count("*").alias("event_count"))
   .write.format("delta").mode("overwrite").saveAsTable("gold_daily_event_counts"))
```

Each arrow is a Spark job writing a Delta table. Bronze keeps the raw JSON content (only metadata added) so it's replayable. Silver does the *conforming* work — parsing, deduping, a null-key quality gate — and an idempotent MERGE so retries don't duplicate. Gold computes a *business* aggregate (`daily event counts per study`) shaped for a dashboard's filters (by `study_id` and `day`), which informs its partitioning/clustering. Note the execution concepts at play: the Silver dedupe (`dropDuplicates`) is a shuffle (lesson 09); the Gold `groupBy` shuffles by `study_id, day` with map-side combine (lessons 04, 09); a mega-study would skew the Gold aggregation (lesson 11). In SQL the same structure holds — `CREATE TABLE silver_... AS SELECT ... `, `MERGE INTO`, `CREATE TABLE gold_... AS SELECT ... GROUP BY ...` — because the layers are about *organization and discipline*, not syntax.

(Healthcare/clinical framing is used here only as illustration — `study_id`, events — not as a domain requirement; the pattern is domain-agnostic.)

## Architectural implications

- **Keep Bronze raw and retained** — it's your replay anchor; never clean or reshape it.
- **Make Silver genuinely clean, conformed, schema-enforced, and reusable** — no business logic, no dumping.
- **Drive Gold from consumer query patterns** — model and lay out for how it's read (lesson 22).
- **Build idempotent, MERGE-based writes** so retries and re-runs are safe (lesson 18).
- **Put quality gates and schema contracts at layer boundaries** — completion ≠ correctness.
- **Apply the execution lessons per layer** — manage shuffles, skew, small files, and partitioning at each hop.
- **Capture lineage and assign ownership**, especially for Gold products.

## Common misconceptions

- *"Medallion is a Spark/Delta feature you turn on."* No — it's a discipline you design and enforce; Spark/Delta are just the tools.
- *"Clean the data as early as possible, even in Bronze."* No — cleaning Bronze destroys replayability; clean in Silver.
- *"Silver is where I put the business logic."* No — Silver conforms; business logic belongs in Gold (reusability).
- *"Appending is fine; the orchestrator won't re-run."* It will, and failures retry — non-idempotent appends create duplicates.
- *"If the job succeeds, the data is correct."* No — success ≠ correctness; quality gates exist precisely because jobs can succeed with garbage.

## Summary

The medallion architecture organizes lakehouse pipelines into Bronze (raw, source-aligned, replayable), Silver (cleaned, conformed, schema-enforced, reusable), and Gold (business-ready, modeled, consumer-shaped) layers, each a Delta table and each hop a Spark job. Its robustness comes from discipline: keep raw raw to preserve replayability, defer opinion rightward, make writes idempotent (MERGE) so retries are safe, choose incremental vs full processing by scale, enforce schema contracts and quality gates at boundaries, and capture lineage and ownership. It is an architectural pattern, not a Spark feature — Spark and Delta are merely the tools. Every execution concept in this course (shuffles, joins, skew, partitioning, small files, materialization, fault tolerance) gets applied per layer, and the most common failures (over-transforming Bronze, dumping into Silver, business logic too early, ownerless Gold, no replay strategy) are failures of discipline, not of technology.

## Terms to remember

- **Bronze / Silver / Gold** — raw / cleaned-conformed / business-ready layers.
- **Replayability** — ability to reprocess a layer from the prior one (anchored by retained raw Bronze).
- **Idempotency** — re-running yields the same state; achieved via MERGE/upsert.
- **Incremental vs full processing** — process only new/changed data vs reprocess everything.
- **Quality gate** — validation/quarantine checkpoint between layers.
- **Schema contract** — enforced agreement on a table's schema (Delta schema enforcement).
- **Lineage** — provenance of each dataset; basis for debugging, audit, impact analysis.
