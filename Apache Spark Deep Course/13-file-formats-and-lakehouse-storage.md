# File Formats and Lakehouse Storage

## Purpose of this lesson

Spark is compute over storage (lesson 01), so the *format* and *layout* of that storage directly determine how fast and cheaply Spark can read it. The difference between CSV and Parquet for the same data can be 10× or more in scan cost, entirely because of how the bytes are organized. This lesson explains row-oriented vs columnar formats, the major formats (CSV, JSON, Avro, Parquet, ORC), and the properties that make a format fast or slow for analytics — splittability, compression, schema handling, predicate pushdown, column pruning, and statistics. It also revisits how file size and count interact with Spark's partitioning (lesson 10). This is the foundation for Delta Lake (lesson 14) and lakehouse design (lesson 15).

## Core mental model

```text
Row-oriented (CSV, JSON, Avro):   [row1: a,b,c][row2: a,b,c]...   good for writing/whole rows
Columnar (Parquet, ORC):          [all a's][all b's][all c's]     good for reading SOME columns of MANY rows

Analytics reads few columns of many rows → COLUMNAR wins:
   read only the columns you need (pruning) + skip blocks via stats (pushdown) + compress better.
```

For analytical/lakehouse workloads, **columnar formats (Parquet) are almost always the right choice**, and the reasons are mechanical, not fashion.

## Main explanation

### Storage vs compute (restated)

Storage holds bytes durably; compute (Spark) reads and transforms them (lesson 01). Because they're decoupled, *you choose the storage format*, and that choice is a performance lever Spark cannot overcome at runtime — Spark can only read what's there as efficiently as the format allows. A query over poorly-formatted data (giant CSVs, millions of tiny JSON files) is slow no matter how good the cluster is. Storage layout is therefore a first-class design decision, not an afterthought.

### Row-oriented vs columnar formats

The fundamental axis. A **row-oriented** format stores each record's fields together, one record after another. To read any part of a record you read the whole record; to read one column across all records you must read every record and discard the rest. A **columnar** format stores all values of a column together, separately from other columns. To read one column you read only that column's contiguous bytes, touching none of the others.

```text
Row-oriented file:                  Columnar file:
  record1: id, name, amount, ts       column id:     [id1, id2, id3, ...]
  record2: id, name, amount, ts       column name:   [name1, name2, ...]
  record3: id, name, amount, ts       column amount: [amount1, amount2, ...]
  ...                                 column ts:     [ts1, ts2, ...]
```

Analytics typically reads **a few columns across many rows** (e.g., `SELECT study_id, SUM(amount)`). Columnar storage serves that pattern perfectly: read only `study_id` and `amount`, skip everything else (column pruning, lesson 07). Row storage forces reading whole records. This single difference is why columnar formats dominate analytical/lakehouse workloads, while row formats suit write-heavy, whole-record patterns (streaming ingestion, OLTP-style logs).

### The formats

- **CSV** — plain text, row-oriented, no schema, no types (everything is text), no compression by default, no statistics. Human-readable and universal, but fragile and slow for analytics.
- **JSON** — text, row-oriented, self-describing (field names per record), supports nesting. Flexible and ubiquitous for APIs/events, but verbose, slow to parse, and expensive at scale.
- **Avro** — binary, row-oriented, schema-based (schema stored with data), compact, supports schema evolution well. Excellent for *write-heavy* and streaming/serialization scenarios where you process whole records.
- **Parquet** — binary, **columnar**, schema-embedded, compressed, with rich per-column/row-group statistics and pushdown support. The de facto standard for analytical lakehouse storage (and Delta's underlying format).
- **ORC** — binary, **columnar**, similar goals and properties to Parquet (compression, statistics, pushdown), originating in the Hive ecosystem. Comparable to Parquet; Parquet is more common in Spark/lakehouse contexts.

## Key concepts

- **Splittability** — whether a file can be divided into independently readable chunks (enables parallel reads).
- **Compression** — reducing bytes on disk; columnar data compresses better; some codecs are splittable, some not.
- **Schema inference / evolution** — deriving types from data / handling schema changes over time.
- **Predicate pushdown** — using format statistics to skip data that can't match a filter.
- **Column pruning** — reading only needed columns (cheap in columnar formats).
- **File metadata / statistics** — per-file/row-group min/max/counts enabling skipping.

## Detailed technical sections

### Compression

Storing data compressed saves I/O (fewer bytes to read from storage) at the cost of CPU to decompress. **Columnar formats compress dramatically better** than row formats because a column holds homogeneous, often repetitive values (the same data type, frequently repeated values), which compresses far more effectively than mixed-type rows — and columnar formats add encodings like dictionary, run-length, and bit-packing on top. So Parquet/ORC are both smaller *and* faster to scan. Compression codec choice matters too: **Snappy** is fast and **splittable** (the common default for analytics — balances speed and size); **gzip** compresses more but is **not splittable** as a whole-file codec (problematic — see below); **zstd** offers strong ratio with good speed. Within Parquet, compression is applied per column chunk, preserving splittability regardless of codec.

### Splittability

For parallel reads, Spark needs to divide a file into chunks that can be read independently as separate input partitions (lesson 10). A **splittable** file can be cut at known boundaries so multiple tasks read different parts simultaneously. A **non-splittable** file (e.g., a single gzip-compressed text file as one opaque stream) must be read start-to-finish by *one* task — destroying parallelism for that file no matter its size. This is why a 10 GB gzipped CSV is a performance trap: it can't be split, so one task reads all 10 GB serially. Parquet and ORC are internally splittable (organized into row groups/stripes), and their per-chunk compression keeps them splittable. Splittability is a key reason to avoid whole-file-gzipped text for large data.

### Schema inference and schema evolution

**Schema inference** is deriving column names and types from the data when no schema is declared. CSV/JSON have no embedded schema, so Spark must *sample/scan* the data to guess types — which costs an extra read pass (an extra job, lesson 04) and can guess wrong (a numeric-looking string inferred as a number, inconsistent JSON fields). Self-describing binary formats (Avro, Parquet, ORC) **embed the schema**, so reads are fast and types are exact — no inference needed. **Schema evolution** is handling changes to the schema over time (added/removed/renamed columns). Avro was designed for robust evolution (writer/reader schema resolution); Parquet supports evolution within limits (e.g., adding columns). For lakehouse tables, **Delta** layers explicit schema enforcement and controlled evolution on top of Parquet (lesson 14), which is far safer than ad-hoc inference.

### Predicate pushdown and column pruning in storage

Two optimizations from lesson 07, realized at the storage layer:

- **Column pruning** is cheap and effective only in *columnar* formats: Spark reads just the requested columns' bytes. In row formats, the reader must read whole records and discard unwanted fields — pruning helps far less.
- **Predicate pushdown** relies on **statistics** stored in the format. Parquet/ORC store per-**row-group** (a horizontal chunk of rows) **min/max** values per column. When a query filters `amount > 1000`, the reader checks each row group's `amount` max; if the max is ≤ 1000, it **skips the entire row group** without reading it. CSV/JSON have no statistics, so every row must be read and then filtered. This is why the *same* filter is far cheaper on Parquet than on CSV: Parquet can skip blocks; text formats cannot.

### File metadata and statistics

Beyond row-group min/max, columnar formats store metadata: row counts, encodings, null counts, and a footer indexing the file's structure. Spark reads the lightweight footer to learn the schema and statistics *before* reading data, enabling pruning and pushdown decisions cheaply. This metadata is the basis of "data skipping." Delta extends this with table-level file statistics in its transaction log, enabling skipping at the *file* granularity across the whole table (lesson 14).

### Why Parquet is common in lakehouse architectures

Parquet wins on every analytical axis: columnar (cheap column pruning), compressed (less I/O, better ratios), splittable (parallel reads), schema-embedded (no inference cost, exact types), and statistics-rich (predicate pushdown / row-group skipping). For the dominant lakehouse access pattern — scanning many rows of a few columns with selective filters — these properties compound into large speedups over text formats. It is also the substrate of Delta Lake and the lingua franca read by Spark, SQL warehouses, and BI tools alike, which fits the decoupled-storage lakehouse model. That ecosystem alignment plus its technical fit make Parquet the default.

### Why JSON is useful but expensive

JSON's strengths are flexibility (self-describing, nested, schema-optional) and ubiquity (the native format of web/event data). Its costs are real: it's **verbose** (field names repeated in every record → large size), **slow to parse** (text parsing of every record), **row-oriented** (no column pruning benefit, no statistics for pushdown), and **inference-dependent** (extra scan, fragile type guesses, ragged schemas). JSON is excellent at the *ingestion boundary* (raw events arrive as JSON) but expensive as a query format. The standard lakehouse pattern is to **land raw JSON in Bronze, then convert to Parquet/Delta in Silver** (lesson 15), paying the parsing cost once rather than on every query.

### Why CSV is fragile

CSV is the lowest-common-denominator format and the most error-prone for data engineering: **no schema or types** (everything is text; numbers, dates, and booleans must be inferred or cast, and inference is unreliable), **no nesting**, **delimiter/quoting ambiguities** (commas and newlines inside fields, inconsistent quoting, encoding issues), **no statistics** (no pushdown — every row is read), **no compression by default**, and **often non-splittable when compressed**. CSVs silently corrupt easily (a stray comma shifts every column) and there's no embedded contract to catch it. Use CSV only when forced by an external source; convert to Parquet/Delta as early as possible.

### How Spark reads files

When Spark reads a path/table, it: (1) **lists** the files (and partition directories), (2) reads **metadata/footers** to get schemas and statistics, (3) decides which files/row groups it can **skip** via partition pruning and predicate pushdown, (4) **splits** the surviving readable files into input partitions based on size, and (5) launches one **task per input partition** to read and decode the bytes (vectorized for Parquet, lesson 08). Each of these steps is influenced by the format: text formats force full reads (no skipping) and extra inference passes; columnar formats enable steps 2–3 to eliminate most I/O. File *count* affects step 1 (listing overhead) and step 4 (how partitions form).

### How file size influences partitions; the small- and large-file problems

(Connecting to lesson 10.) File size and count drive input partitioning. **Too many small files**: heavy listing overhead, many tiny input partitions → many tiny tasks with high scheduling overhead, and poor compression/statistics efficiency (statistics overhead per file dominates). **Too few huge files**: too few input partitions → idle cores, and (if non-splittable) serial reads. The sweet spot is a moderate number of reasonably sized files (commonly ~128 MB–1 GB) that split into more partitions than you have cores. The **small-file problem** is endemic in streaming/incremental writes (each micro-batch writes small files) and is solved by **compaction** (lesson 14, Delta `OPTIMIZE`). How storage is laid out — format, file size, file count, and table partitioning — *is* how you control read performance.

### How storage layout influences query performance

Putting it together: a well-laid-out table (Parquet/Delta, right-sized files, partitioned on a low-cardinality filtered column, with useful statistics and clustering) lets Spark read a small fraction of the data for a typical query — prune partitions, skip files/row groups, read only needed columns. A poorly-laid-out table (giant CSV, or millions of tiny JSON files, or partitioned on a high-cardinality column) forces full scans, serial reads, or listing storms. The *same logical query* can be cheap or ruinous depending entirely on layout — which is why lakehouse design (lessons 14, 15, 22) treats layout as a primary concern.

## File format comparison

| Property | CSV | JSON | Avro | Parquet | ORC |
|----------|-----|------|------|---------|-----|
| Orientation | Row | Row | Row | **Columnar** | **Columnar** |
| Binary/text | Text | Text | Binary | Binary | Binary |
| Schema embedded | No | No (self-desc.) | Yes | Yes | Yes |
| Typed | No | Weak | Yes | Yes | Yes |
| Compression | None default | Poor | Good | **Excellent** | **Excellent** |
| Splittable | Only if uncompressed | Often per-line | Yes | **Yes** | **Yes** |
| Column pruning | No | No | No | **Yes** | **Yes** |
| Predicate pushdown / stats | No | No | Limited | **Yes (row-group min/max)** | **Yes** |
| Nesting | No | Yes | Yes | Yes | Yes |
| Best for | Interchange (forced) | Ingestion/events | Write-heavy/streaming | **Analytics/lakehouse** | Analytics (Hive) |

## PySpark and SQL perspective

```python
# Reading CSV: no schema → inference scan (extra job), no pushdown, full read
raw = spark.read.option("header", True).option("inferSchema", True).csv("Files/landing/*.csv")

# Reading Parquet: schema from footer, pushdown + pruning available
events = spark.read.parquet("Files/events")
events.filter("amount > 1000").select("study_id", "amount")   # prunes columns + skips row groups
```

For the Parquet read, the explain plan shows `FileScan parquet` with `PushedFilters: [GreaterThan(amount,1000)]` and a two-column read — Spark skips row groups whose `amount` max ≤ 1000 and reads only `study_id` and `amount`. The equivalent CSV read can do *neither* (no statistics, row-oriented), so it reads every byte of every row and filters afterward. This is the format choice showing up directly in the physical plan. The canonical pipeline move: `spark.read.json("Files/raw").write.format("delta").saveAsTable("silver")` — parse the expensive JSON once and persist as columnar Delta so all downstream queries are cheap. In SQL, `SELECT ... FROM parquet.\`Files/events\` WHERE amount > 1000` behaves identically, since the format's capabilities, not the query language, enable the skipping.

## Architectural implications

- **Default to Parquet/Delta for any data queried analytically**; convert text/JSON to it early (Bronze→Silver).
- **Right-size files** (~128 MB–1 GB) and avoid both tiny-file storms and giant non-splittable files; compact regularly (lesson 14).
- **Use splittable compression** (Snappy/zstd) for large data; avoid whole-file gzip on text.
- **Avoid CSV except at forced boundaries**; never rely on `inferSchema` for production — declare schemas.
- **Lay out storage for the read pattern**: format, file size, partitioning, and clustering determine query cost more than cluster size.

## Common misconceptions

- *"The file format doesn't affect Spark performance much."* It can change scan cost by 10×+ via pruning, pushdown, and compression.
- *"Columnar formats are slower to write so they're worse."* They cost slightly more to write but vastly less to read repeatedly — the right trade for analytics.
- *"A big gzipped CSV is fine because it's compressed."* It's non-splittable — one task reads the whole thing serially.
- *"inferSchema is convenient and harmless."* It costs an extra scan and guesses types unreliably; declare schemas in production.
- *"More files means more parallelism."* Too many small files create overhead and tiny tasks; size matters more than count.

## Summary

The storage format determines how efficiently Spark can read, and the dominant axis is row-oriented vs columnar. Analytical workloads read a few columns of many rows with selective filters, which columnar formats (Parquet, ORC) serve far better than row formats (CSV, JSON, Avro) through column pruning, predicate pushdown via row-group statistics, superior compression, and splittability. CSV is fragile (untyped, ambiguous, no stats); JSON is flexible but verbose and slow; Avro suits write-heavy/streaming; Parquet is the analytical/lakehouse default and the substrate of Delta. File size and count drive input partitioning and must be right-sized to avoid small-file overhead or under-parallelized giant files. Storage layout — format, compression, file sizing, partitioning, statistics — is a primary performance lever that Spark cannot compensate for at runtime.

## Terms to remember

- **Columnar format** — stores columns contiguously; enables cheap pruning and skipping (Parquet/ORC).
- **Row-oriented format** — stores whole records together (CSV/JSON/Avro); good for writes/whole rows.
- **Splittability** — whether a file can be read in parallel chunks.
- **Row group** — a horizontal chunk in Parquet with per-column min/max statistics.
- **Predicate pushdown / data skipping** — skipping blocks/files that can't match a filter, via statistics.
- **Schema inference / evolution** — deriving types from data / managing schema change over time.
- **Small-file problem** — too many tiny files degrading reads and metadata; fixed by compaction.
