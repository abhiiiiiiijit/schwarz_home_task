# Schwarz IT – Data Engineer Home Task

## Project Structure

```
assignment1/
  generate_data.py          # provided – generates random basket data
  split.py                  # Subtask 1 – splits CSV into shards
  cooccurrence.py           # Subtask 2 – computes product co-occurrences
  k_cooccurrence.py         # Bonus  – generalised k-combination co-occurrences
  benchmark.py              # Bonus  – runtime scaling measurement + skew analysis
  run_assignment1.py        # Convenience wrapper (split → compute → preview)

assignment2/
  incremental_ingest.py     # Spark incremental ingestion + deduplication
  introduce_duplicates.py   # Test helper – adds duplicate rows for testing

tests/
  conftest.py               # Path setup (works with pytest and unittest)
  test_assignment1.py       # Full test suite – 50 tests, zero external deps
```

---

## Assignment 1 – Product Co-occurrence (memory-constrained)

### How to run

```bash
# 1. Generate test data
python3 generate_data.py --scale 1          # creates data_1.csv.gz

# 2. Run the full pipeline (split → compute → preview)
python3 run_assignment1.py data_1.csv.gz

# Optional: tune shard count and output path
python3 run_assignment1.py data_1.csv.gz --shards 32 --output result.csv.gz

# Or run each step individually:
python3 split.py data_1.csv.gz --shards 16 --output-dir shards/
python3 cooccurrence.py --shard-dir shards/ --output cooccurrences.csv.gz
```

**Low-memory testing** (simulate the memory constraint):

```bash
python3 -Xmx64m run_assignment1.py data_1.csv.gz
```

### Why it computes the correct result

**Splitting strategy**: Every row belonging to a basket is routed to the
same shard via `md5(basket_id) % N`.  Because a basket never spans two
shards, every co-occurring pair `(p1, p2)` within that basket is always
counted together in exactly one shard.  Summing the per-shard dictionaries
therefore gives the exact global count.

**Pair generation**: For each basket we emit the *sorted* pair
`(min(p1,p2), max(p1,p2))`, so `(1,2)` and `(2,1)` always map to the
same key, satisfying the combination requirement.

### Why it works within memory constraints

| Stage | What is held in memory at once |
|-------|-------------------------------|
| Splitting | One CSV row (one string) |
| Co-occurrence | One basket (≤ 5 product IDs) + the pair-count dict |

The pair-count dict is bounded by the number of *distinct* pairs
`P*(P-1)/2` where P = product count.  For scale=1, P = 256, giving at
most 32 640 entries – negligible.  Even for scale=10 (P = 2 560) the
upper bound is ~3.3 M entries; in practice far fewer pairs co-occur, so
the dict stays small relative to any realistic memory cap.

### How to productionise

1. **Parallelise the shard-processing loop**: Submit each shard as an
   independent task to a job queue (Celery, Airflow, AWS Batch) and
   reduce the partial dicts in a final step.
2. **Distributed counting with Spark**: `explode` per-basket product lists,
   self-join on `basket_id` where `p1 < p2`, then `groupBy(p1,p2).count()`.
3. **Persist intermediate results**: Write shard counts as Parquet so a
   re-run after failure only reprocesses failed shards.
4. **Streaming**: Maintain a running count table (Redis, Delta Lake) and
   update it as new baskets arrive.

---

## Bonus – Runtime Scaling, Skew, and Triples

### Runtime scaling

**How to run the benchmark:**

```bash
# Scales 1–3 with pairs (k=2) – takes ~1-2 min
python3 benchmark.py --scales 1 2 3 --shards 16

# Add scale 4 and 5 for more data points (takes longer)
python3 benchmark.py --scales 1 2 3 4 5 --shards 16

# Measure triples (k=3) instead
python3 benchmark.py --scales 1 2 3 --k 3

# Skip the skew analysis and only measure runtime
python3 benchmark.py --scales 1 2 3 --skip-skew
```



### Skewed co-occurrence counts

**Would the algorithm still work if some combinations are much more common?**

**Yes, completely.**  The count for a pair is simply an integer in a python3
dict.  A pair that appears in every basket just accumulates a large integer
for that key — the data structure, the shard routing, and the per-basket
grouping are all unaffected by the magnitude of individual counts.

The benchmark script's `analyse_skew()` function (and the dedicated
`TestSkewedData` test class) verify this empirically:

- 200 baskets are constructed so that products `0` and `1` always appear
  together, while all other pairs are rare.
- The pipeline's output is compared against a brute-force reference.
- Zero mismatches are found, including for the dominant pair and for the
  rarest pairs.

The only scenario where skew *could* be a problem is if a single product
appears in so many baskets that the pair-count dict grows too large for
the available memory.  This is a memory constraint issue, not a correctness
issue, and is addressed by increasing the number of shards so each shard
holds fewer baskets.

### Combinations of three products (triples, k=3)

**How does the problem change?**

The algorithmic structure is **identical** — only the combination
generator changes from 2-combinations to 3-combinations.  This is
implemented in `k_cooccurrence.py`, which accepts a `--k` argument:

```bash
# Compute triples after splitting
python3 split.py data_1.csv.gz --shards 16 --output-dir shards/
python3 k_cooccurrence.py --shard-dir shards/ --k 3 --output triples.csv.gz

# Or via the benchmark
python3 benchmark.py --scales 1 2 3 --k 3
```


---

## Tests

### Running the tests

```bash
# With pytest (recommended):
pip install pytest
pytest tests/test_assignment1.py -v

# Without pytest – stdlib unittest only, zero extra dependencies:
python3 -m unittest tests.test_assignment1 -v

# From inside the tests/ directory:
python3 -m unittest test_assignment1 -v
```

### Test coverage (50 tests)

| Class | What is tested |
|---|---|
| `TestShardFor` (5) | Determinism, range guarantee, uniform distribution, non-trivial hash |
| `TestSplitFunction` (8) | File count, gzip output, row completeness, basket-integrity guarantee, gzip input, single shard, short-row skipping, empty input |
| `TestPairsFromBasket` (7) | 2-, 3-, 5-product baskets, single, empty, dedup, sorted order |
| `TestProcessShard` (5) | Basic counts, gzip shard, accumulator merging, empty shard, single-product baskets |
| `TestComputeCooccurrences` (5) | Task-spec example, brute-force match (200 baskets), all-singles, header, positive integer counts |
| `TestKTuplesFromBasket` (6) | C(4,3), k=2 parity, k=1 singletons, k > basket size, dedup, C(5,3) |
| `TestKCooccurrences` (5) | Brute-force match (triples), header, no triples when baskets too small, single triple, accumulation |
| `TestSkewedData` (4) | Dominant pair correct, full correctness under skew, extreme skew (one pair only), rare pairs alongside dominant |
| `TestEndToEnd` (5) | More shards than baskets, single shard, k=2 vs k=3 consistency, 1 000-basket stress test, gzip input |

All tests use only the python3 standard library (`unittest`, `csv`, `gzip`,
`tempfile`, `itertools`, `random`, `collections`).  No network access, no
`pip install` required beyond pytest itself (which is optional).

---

## Assignment 2 – Incremental Spark Ingestion

### How to run

```bash
# 1. Generate two data files
python3 generate_data.py --scale 1
cp data_1.csv.gz data_1b.csv.gz

# 2. Introduce duplicates and stage them in incoming/
python3 introduce_duplicates.py data_1.csv.gz data_1b.csv.gz \
    --dup-ratio 0.15 --output-dir incoming/

# 3. First incremental run
spark-submit incremental_ingest.py \
    --input-dir incoming/ \
    --output-dir warehouse/sales_dedup/

# 4. Generate a third file and run again (incremental step)
python3 generate_data.py --scale 1
mv data_1.csv.gz data_2.csv.gz
python3 introduce_duplicates.py data_2.csv.gz --output-dir incoming/

spark-submit incremental_ingest.py \
    --input-dir incoming/ \
    --output-dir warehouse/sales_dedup/
```

On the second run, already-processed files are skipped (tracked in
`processed_files.txt`) and only the new file is unioned with the existing
Parquet table before deduplication.

### Why Parquet?

- **Columnar storage**: ideal for the two-column schema; reads only needed
  columns and applies statistics-based pruning.
- **Splittable**: Spark parallelises reads across row groups.
- **Native Spark support**: no extra dependencies.
- **Interoperability**: works with Hive, Trino, BigQuery, Redshift Spectrum,
  Delta Lake, Iceberg, and virtually every modern data platform.
- **Compression**: Snappy by default (good speed/size balance).

### Why Deduplication is Correct

Deduplication is implemented using a window-function strategy that keeps the **most recent record** for each `(basket_id, product_id)` pair based on the ingestion timestamp.

For every `(basket_id, product_id)` group, records are ordered by **`ingestion_date` in descending order**, and only the **latest record** is retained. This ensures that when the same record appears multiple times across different files or ingestion runs, the newest version replaces the older ones.


### How to productionise

1. **Delta Lake / Apache Iceberg**: Replace the full-rewrite overwrite
   with `MERGE INTO` (upsert) so only changed partitions are rewritten —
   scales to billions of rows.
2. **Spark Structured Streaming**: Use `spark.readStream` with
   `cloudFiles` (Databricks Auto Loader) or a standard file-stream source
   to trigger ingestion automatically when new files land in object storage.
3. **Reliable processed-file log**: Move `processed_files.txt` to S3, HDFS,
   or a metadata database so it survives node restarts and concurrent runs.
4. **Schema evolution**: Add `mergeSchema` on Parquet reads and enforce
   contracts via a schema registry (Confluent, AWS Glue).
5. **Monitoring**: Emit row counts, duplicate rates, and file counts to
   a monitoring system (Prometheus, Datadog) and alert on anomalies.
6. **Partitioning**: Partition the output table by ingestion date to enable
   efficient time-range queries and partition-level compaction.