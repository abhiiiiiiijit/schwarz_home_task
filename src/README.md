# Schwarz IT – Data Engineer Home Task

## Project Structure

```
assignment1/
  generate_data.py          # provided – generates random basket data
  split.py                  # Subtask 1 – splits CSV into shards
  cooccurrence.py           # Subtask 2 – computes product co-occurrences
  run_assignment1.py        # Convenience wrapper (split + compute + preview)

assignment2/
  incremental_ingest.py     # Spark incremental ingestion + dedup
  introduce_duplicates.py   # Helper – adds duplicate rows for testing
```

---

## Assignment 1 – Product Co-occurrence (memory-constrained)

### How to run

```bash
# 1. Generate test data
python generate_data.py --scale 1          # creates data_1.csv.gz

# 2. Run the full pipeline (split → compute → preview)
python run_assignment1.py data_1.csv.gz

# Optional: control shard count and output path
python run_assignment1.py data_1.csv.gz --shards 32 --output result.csv.gz

# Or run steps individually:
python split.py data_1.csv.gz --shards 16 --output-dir shards/
python cooccurrence.py --shard-dir shards/ --output cooccurrences.csv.gz
```

**Low-memory testing** (simulate the constraint):

```bash
python -Xmx64m run_assignment1.py data_1.csv.gz
```

### Why it computes the correct result

**Splitting strategy**: Every row of a basket is routed to the same shard
using `hash(basket_id) % N`.  Because a basket never spans two shards,
every co-occurring pair (p1, p2) within that basket is always counted
together in exactly one shard.  Summing the per-shard counts therefore
gives the exact global count.

**Pair generation**: For each basket we take the *sorted* pair
`(min(p1,p2), max(p1,p2))` so that (1,2) and (2,1) are treated as the
same combination, satisfying the requirement.

### Why it works within memory constraints

| Stage | What is in memory |
|-------|-------------------|
| Splitting | One CSV row at a time (one string) |
| Co-occurrence | One basket at a time (≤ 5 product IDs) + the pair-count dict |

The pair-count dict is bounded by the number of *distinct* pairs, which
is at most `P*(P-1)/2` where P = number of products.  For scale=1,
P = 256, giving at most 32 640 entries – negligible.  Even for scale=10
(P = 2 560) the upper bound is ~3.3 M entries, and in practice far fewer
pairs ever co-occur, so the dict stays small relative to the simulated
memory cap.

### How to productionise

1. **Parallelise the shard processing loop**: Submit each shard as a
   separate task to a job queue (Celery, Airflow, AWS Batch, etc.) and
   collect the partial dicts with a reduce step.
2. **Distributed counting**: Use Spark/Flink – `explode` the per-basket
   product lists, self-join on basket_id where p1 < p2, then
   `groupBy(p1, p2).count()`.
3. **Persist intermediate results**: Write shard counts as Parquet so that
   a re-run after a failure only reprocesses failed shards.
4. **Streaming**: For continuous ingest, maintain a running count table
   (e.g., in Redis or a Parquet Delta table) and update it as new baskets
   arrive.

### Bonus answers

**Runtime scaling**: The algorithm is *O(R log R + P²·B)* where R = rows,
P = products per basket (≤5), B = baskets.  Doubling the scale doubles R
and B, so runtime grows roughly linearly with scale.  In practice the
sort within `pairs_from_basket` is O(P log P) with P ≤ 5, which is
constant, so the dominant term is O(R).

**Skewed co-occurrence counts**: The algorithm is correct regardless of
how skewed the counts are.  A very frequent pair (e.g. products 1 and 2
in every basket) just produces a large integer for that dict key – it does
not affect memory or correctness.

**Triples (k=3)**: Change `pairs_from_basket` to yield all 3-combinations
using `itertools.combinations(sorted_products, 3)`.  The rest of the
pipeline is identical.  Memory remains bounded because the number of
distinct k-tuples is finite and small for small k.

---

## Assignment 2 – Incremental Spark Ingestion

### How to run

```bash
# 1. Generate two data files
python generate_data.py --scale 1       # data_1.csv.gz  (run twice, rename second)
cp data_1.csv.gz data_1b.csv.gz

# 2. Introduce duplicates and stage them in incoming/
python introduce_duplicates.py data_1.csv.gz data_1b.csv.gz \
    --dup-ratio 0.15 --output-dir incoming/

# 3. Run incremental ingestion (first batch)
spark-submit incremental_ingest.py \
    --input-dir incoming/ \
    --output-dir warehouse/sales_dedup/

# 4. Generate a third file and run again (incremental step)
python generate_data.py --scale 1
mv data_1.csv.gz data_2.csv.gz
python introduce_duplicates.py data_2.csv.gz --output-dir incoming/

spark-submit incremental_ingest.py \
    --input-dir incoming/ \
    --output-dir warehouse/sales_dedup/
```

On the second run, already-processed files are skipped (tracked in
`processed_files.txt`) and only the new file is unioned with the
existing Parquet table before deduplication.

### Why Parquet?

- **Columnar storage**: ideal for the two-column schema; reads only
  needed columns and applies statistics-based pruning.
- **Splittable**: Spark can parallelize reads across row groups.
- **Native Spark support**: no extra dependencies; read/write with
  `spark.read.parquet()` / `.write.parquet()`.
- **Interoperability**: works with Hive, Trino, BigQuery, Redshift,
  Delta Lake, Iceberg, and virtually every modern data platform.
- **Compression**: Snappy by default (good balance of speed and size).

### Why deduplication is correct

`dropDuplicates(["basket_id", "product_id"])` keeps exactly one row for
every distinct `(basket_id, product_id)` pair across the union of existing
data and new incoming data.  The assignment requires:

- Records that appear in multiple files are not duplicated → ✓ (union + dropDuplicates)
- Previously ingested records are not deleted → ✓ (existing Parquet is
  always included in the union before overwrite)

### How to productionise

1. **Delta Lake / Apache Iceberg**: Replace the overwrite pattern with a
   `MERGE INTO` (upsert) so only changed partitions are rewritten.
   This scales to tables with billions of rows.

2. **Spark Structured Streaming**: Use `spark.readStream` with
   `cloudFiles` (Databricks Auto Loader) or a standard file-stream source
   to trigger ingestion automatically when new files land in S3/ADLS/GCS.

3. **Idempotency**: The `processed_files.txt` log should be stored in a
   reliable external store (S3, HDFS, or a metadata DB) rather than the
   local filesystem so that it survives node restarts.

4. **Schema evolution**: Add `mergeSchema` option on Parquet reads and
   enforce schema contracts via a schema registry (e.g. Confluent,
   AWS Glue) to handle upstream changes safely.

5. **Monitoring**: Emit row counts, duplicate counts, and file counts as
   metrics to a monitoring system (Prometheus, Datadog) and alert on
   anomalies.

6. **Partitioning**: Partition the output Parquet table by a time-based
   column (e.g., ingestion date) to enable efficient time-range queries
   and partition-level compaction.
