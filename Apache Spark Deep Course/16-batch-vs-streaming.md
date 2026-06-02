# Batch vs Streaming

## Purpose of this lesson

Most data engineering is batch — process a bounded dataset, produce output, finish. But some problems require *streaming* — continuously processing data as it arrives. Spark supports both with a deliberately *unified* API (Structured Streaming), which is conceptually elegant but operationally consequential: streaming adds state, watermarks, checkpoints, and exactly-once concerns that batch doesn't have. This lesson defines bounded vs unbounded data, explains micro-batch and continuous processing, event time vs processing time, watermarks and state, checkpointing and exactly-once semantics, sources and sinks, and — importantly — *when streaming is justified versus when batch is enough*. It distinguishes stream *processing* (Spark) from message *transport* (brokers), reinforcing lesson 01, and connects to fault tolerance (lesson 18) and Delta (lesson 14).

## Core mental model

```text
BATCH:     bounded input ──► process all of it ──► output ──► done
                            (a finite dataset, a finite job)

STREAMING: unbounded input ──► [process arriving data continuously, forever] ──► output
                              (an infinite dataset, a never-ending job that maintains STATE)

Spark Structured Streaming = the SAME DataFrame/SQL API, executed as repeated MICRO-BATCHES over the unbounded input.
```

The key reframe: a stream is an *unbounded table that grows over time*; a streaming query is a *standing query* continuously updated as rows arrive. The API looks like batch; the execution is incremental and stateful.

## Main explanation

### Bounded vs unbounded data

The fundamental distinction. **Bounded** data is finite and complete — a file, a table, yesterday's logs: you can read all of it, process it, and finish. **Unbounded** data is infinite and ongoing — a continuous feed of events with no end: clickstreams, sensor readings, transactions as they happen. Batch processing operates on bounded data (process the whole thing, terminate). Stream processing operates on unbounded data (process indefinitely, never terminating normally). This single property — *does the input end?* — drives every other difference: state management, time semantics, checkpointing, and the operational complexity of a job that must run forever and survive failures without losing its place.

### Batch processing

**Batch processing** reads a bounded dataset, transforms it, writes output, and completes. It's the default for most pipelines (the medallion jobs of lesson 15 are typically batch). Batch is **simple and robust**: the job has a clear start and end, is naturally re-runnable (re-read the input, reproduce the output), and all the optimization concepts in this course apply directly. Latency is the trade-off: results are only as fresh as the last batch run (hourly, daily). For most analytics, that's perfectly adequate, which is why batch dominates and why "do I actually need streaming?" should be asked before reaching for it.

### Micro-batch processing (Structured Streaming's default)

Spark's primary streaming model, **Structured Streaming**, processes an unbounded stream as a sequence of small **micro-batches**: it periodically takes the data that arrived since the last batch, runs it through the *same* DataFrame/SQL logic as a tiny batch job, updates any state, writes output, records progress, and repeats. So streaming in Spark is, mechanically, *batch executed repeatedly on small increments*. This reuses the entire batch engine (Catalyst, Tungsten, shuffles, the execution model) — a streaming aggregation shuffles just like a batch one. The benefits: one API and one engine for both, with most of your batch knowledge transferring directly. The latency is bounded by the micro-batch interval (sub-second to seconds), which is "near-real-time" — sufficient for the vast majority of streaming use cases.

### Continuous processing (conceptually)

Spark also has an experimental **continuous processing** mode that aims for true record-at-a-time, very-low-latency (millisecond) processing rather than micro-batches. It targets cases needing the lowest possible latency, at the cost of supporting fewer operations and weaker guarantees. Conceptually it's the alternative to micro-batching: instead of "process small chunks repeatedly," it's "process each record as it arrives." In practice, micro-batch is the mainstream, well-supported mode; continuous processing is a niche option to be aware of. The conceptual point is that "streaming" spans a latency spectrum from large batches → micro-batches → continuous, and you pick based on how fresh results must be.

## Key concepts

- **Bounded / unbounded data** — finite vs infinite input; the root distinction.
- **Micro-batch** — streaming as repeated small batch jobs (Spark's default).
- **Event time vs processing time** — when an event *happened* vs when Spark *processed* it.
- **Watermark** — a threshold on event-time lateness, bounding how long state is kept.
- **State** — information a streaming query maintains across batches (for aggregations, joins, dedup).
- **Checkpoint** — durable record of progress and state enabling fault-tolerant restart.
- **Source / sink** — where the stream reads from / writes to.
- **Exactly-once** — each input record affects the result exactly once, despite failures/retries.

## Detailed technical sections

### Event time vs processing time

A pivotal distinction in streaming. **Event time** is when an event actually occurred (a timestamp in the data — e.g., the sensor reading's time). **Processing time** is when Spark happens to process it. These differ because of network delays, buffering, retries, and out-of-order delivery: an event that *happened* at 10:00 might *arrive* at 10:05, or after an event that happened at 10:02. Correct time-based analytics (windowed aggregations like "events per minute") almost always need **event time**, because you care about when things *happened*, not when they were processed. But event-time processing raises a hard question: how long do you wait for late events before finalizing a window? You can't wait forever (state would grow unbounded). That's what watermarks resolve.

### Watermarks

A **watermark** is a moving threshold that tells Spark how late an event can be and still be counted, e.g., "accept events up to 10 minutes late." Spark tracks the maximum event time seen and sets the watermark at `max_event_time − lateness_threshold`. Windows older than the watermark are considered **final**: their state is emitted and **dropped**, and events arriving later than the watermark for those windows are discarded. The watermark thus serves two purposes: it **bounds state** (old window state can be cleaned up, so memory doesn't grow forever) and it **defines correctness for late data** (how late is too late). Choosing the watermark is a trade-off: a longer allowance handles more late data but keeps more state longer; a shorter one is leaner but drops more late events. Without watermarks, stateful streaming aggregations would accumulate state indefinitely.

### State and stateful aggregations

Many streaming operations require **state** — information carried across micro-batches. A streaming `groupBy().count()` must remember the running count per key across batches (so each new batch *updates* counts rather than recomputing from scratch). Streaming joins, deduplication, and windowed aggregations all maintain state keyed by group/window/key. This state is held in executor memory (backed by the checkpoint, below) and grows with the number of active keys/windows — which is exactly why watermarks matter, to bound and evict it. **Stateless** streaming operations (a plain `filter`/`select` map) carry no state and are simple. **Stateful** ones (aggregations, joins, dedup) are where streaming's complexity concentrates: you must reason about how much state accumulates and how it's bounded. State management is the single biggest source of streaming operational difficulty.

### Checkpointing

Because a streaming query runs forever and maintains state, it must survive failures and restarts without losing its place or its state. **Checkpointing** writes, durably (to object storage), the query's **progress** (which input offsets have been processed — the "where am I in the stream") and its **state** (the accumulated aggregation/join state). On failure and restart, Spark reads the checkpoint, resumes from the last committed offsets, and restores state — so it neither reprocesses already-handled data nor loses in-flight aggregates. The checkpoint is what makes streaming fault-tolerant (lesson 18) and is the basis of exactly-once semantics. Every production streaming query needs a stable checkpoint location; losing or changing it incorrectly breaks recovery and can cause reprocessing or state loss.

### Exactly-once semantics and their caveats

Spark Structured Streaming offers **exactly-once** end-to-end semantics under specific conditions: each input record affects the result exactly once, even across failures and retries. It achieves this by combining **replayable sources** (sources that let Spark re-read from a known offset — e.g., Kafka, files, Delta), **checkpointed offsets** (so Spark knows exactly what's been processed), and **idempotent or transactional sinks** (so re-writing the same output doesn't duplicate it). The crucial caveats: exactly-once holds only when *all three* conditions are met. If the **source isn't replayable**, you can't guarantee re-reading lost data (you fall back to at-most-once or weaker). If the **sink isn't idempotent/transactional** (e.g., a naive append to a non-transactional system), a retry can duplicate output — giving **at-least-once** in practice. **Delta sinks** are transactional (lesson 14), which is a major reason Delta + Structured Streaming achieves true exactly-once into the lakehouse. So "exactly-once" is a *property of the whole pipeline*, not a free guarantee — it requires replayable source + checkpoint + transactional sink.

### Sources and sinks

A streaming query reads from a **source** and writes to a **sink**:

- **File sources** — monitor a directory for new files, processing them as they appear. Simple; latency bounded by how often files land. Replayable (Spark tracks which files it processed).
- **Delta sources** — read new commits from a Delta table incrementally (lesson 14). The same Delta table can be a streaming source *and* a batch table — a powerful unification.
- **Message brokers** (Kafka, Event Hubs) — the classic high-throughput streaming source; replayable via offsets, enabling exactly-once.
- **Sinks** — Delta tables (transactional, exactly-once), files, brokers, or `foreachBatch` for custom writes. Delta sinks are preferred in the lakehouse for their transactional guarantees.

The replayability of the source and the transactionality of the sink determine the achievable guarantees (above).

### Message transport vs stream processing

A critical conceptual separation (from lesson 01). A **message broker** (Kafka, Event Hubs, Service Bus) *transports* events — durable, ordered, partitioned delivery between producers and consumers. It does **not** transform, aggregate, join, or window data. **Stream processing** (Spark Structured Streaming) *computes over* the stream — running the aggregations, joins, windowed logic, and writing results. Spark *consumes from* the broker and *processes*; the broker is the pipe, Spark is the processor on the pipe. "We have Kafka" means you have transport, not processing; you still need a processing engine. Conversely, Spark streaming needs a source (often a broker) to read from. Keeping transport and processing as distinct layers prevents architectural confusion about what each system is responsible for.

### When batch is enough vs when streaming is justified

This judgment matters because **streaming adds substantial operational complexity** (state, watermarks, checkpoints, exactly-once reasoning, a perpetually-running job to monitor and recover). Use **batch** when: results don't need to be fresher than your batch cadence (hourly/daily is fine), the data is naturally bounded or arrives in periodic loads, and simplicity/reliability matter more than latency — which covers *most* analytics. Use **streaming** when: you genuinely need low-latency, continuously-updated results (real-time dashboards, fraud detection, alerting, operational monitoring), the data is truly unbounded and continuous, and the business value of freshness justifies the operational cost. The common anti-pattern (lesson 22) is reaching for streaming when periodic batch (even frequent micro-batch-like scheduled batch) would meet the freshness requirement at a fraction of the complexity. **Default to batch; justify streaming.**

### How streaming fits into lakehouse architecture

Streaming integrates naturally with the medallion/lakehouse model (lesson 15) precisely because of the **batch/streaming unification** on Delta (lesson 14). A streaming job can write into a Bronze/Silver Delta table (each micro-batch a transaction), and downstream jobs — batch or streaming — read it. The *same* table serves both modes, so you don't maintain separate streaming and batch storage. A typical pattern: stream raw events into Bronze (low latency ingestion), then either stream or batch-process Bronze→Silver→Gold depending on freshness needs per layer. Delta's transactional sinks give exactly-once into the lakehouse, and checkpoints provide fault tolerance. This is why "streaming in the lakehouse" is mostly about *where freshness is needed*, layered onto the same tables and the same Spark engine you already use for batch.

## PySpark and SQL perspective

```python
# BATCH read — bounded, runs once and finishes
batch_df = spark.read.format("delta").table("silver_events")
batch_df.groupBy("study_id").count().write.format("delta").saveAsTable("gold_counts")

# STREAMING read — unbounded, runs continuously; SAME transformation API
stream_df = spark.readStream.format("delta").table("silver_events")
(stream_df
   .withWatermark("event_ts", "10 minutes")                      # bound lateness/state
   .groupBy("study_id", F.window("event_ts", "1 minute"))        # stateful windowed aggregation
   .count()
   .writeStream
   .format("delta")
   .option("checkpointLocation", "Files/_chk/gold_counts")       # durable progress + state
   .outputMode("append")
   .toTable("gold_counts_stream"))
```

The transformation logic (`groupBy().count()`) is *identical* to batch — that's the unification. The differences are all streaming concerns: `readStream`/`writeStream` (unbounded), `withWatermark` (bound state and define late-data correctness), `window` (event-time windows), and `checkpointLocation` (fault-tolerant restart). Under the hood, Spark runs this as repeated micro-batches: each interval, it reads new Delta commits since the last offset, updates the per-(study, minute-window) counts in state, writes results transactionally to Delta (exactly-once: replayable Delta source + checkpoint + transactional Delta sink), and records progress. The streaming aggregation shuffles by `study_id, window` exactly as a batch `groupBy` would (lessons 04, 09) — and a dominant study would skew it (lesson 11). Spark SQL can express the transformation identically over the streaming DataFrame; the streaming-specific options remain in the read/write API.

## Architectural implications

- **Default to batch; justify streaming** by a real low-latency requirement, not by novelty.
- **Separate transport (broker) from processing (Spark)** in your architecture — they're different layers.
- **For streaming, design state and watermarks deliberately** — unbounded state is the main failure mode.
- **Use replayable sources + checkpoints + transactional (Delta) sinks** to get true exactly-once.
- **Exploit batch/streaming unification on Delta** — same tables serve both; choose mode per layer by freshness.
- **Budget for streaming's operational cost** — perpetual jobs need monitoring, checkpoint management, and recovery runbooks.

## Common misconceptions

- *"Streaming is just faster batch."* No — it's unbounded, stateful, and runs forever, with fundamentally different operational concerns.
- *"Having Kafka means I have stream processing."* No — Kafka transports; Spark processes. Different layers.
- *"Exactly-once is automatic in Spark."* Only with replayable source + checkpoint + transactional/idempotent sink; otherwise at-least-once.
- *"Event time and processing time are basically the same."* No — they diverge due to delays/out-of-order; correct analytics needs event time + watermarks.
- *"Streaming state takes care of itself."* No — without watermarks, stateful state grows unbounded until it fails.
- *"We should stream everything for freshness."* Usually wrong — batch meets most freshness needs at far lower complexity.

## Summary

Batch processes bounded data and finishes; streaming processes unbounded data continuously and maintains state. Spark's Structured Streaming unifies them by executing streaming as repeated micro-batches over the same DataFrame/SQL engine, so most batch knowledge transfers — but streaming adds event-time semantics, watermarks (to bound state and define late-data correctness), state management, checkpointing (for fault-tolerant restart), and exactly-once reasoning (which requires replayable sources + checkpoints + transactional sinks like Delta). Message brokers transport events; Spark processes them — distinct layers. Streaming integrates with the lakehouse via Delta's batch/streaming unification, letting the same tables serve both modes. Because streaming is operationally complex, default to batch and justify streaming only when a genuine low-latency requirement exists.

## Terms to remember

- **Bounded / unbounded data** — finite (batch) vs infinite (streaming) input.
- **Micro-batch** — Spark streaming as repeated small batch jobs.
- **Event time vs processing time** — when an event happened vs when it was processed.
- **Watermark** — lateness threshold that bounds state and defines late-data handling.
- **State** — cross-batch information for stateful operations (aggregations, joins, dedup).
- **Checkpoint** — durable progress + state for fault-tolerant restart.
- **Exactly-once** — each record affects results once; needs replayable source + checkpoint + transactional sink.
- **Source / sink** — stream input / output; Delta serves as both, transactionally.
