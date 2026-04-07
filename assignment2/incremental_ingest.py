"""
incremental_ingest.py  –  Assignment 2
=======================================
Incrementally reads new sales CSV files, deduplicates records based on
the (basket_id, product_id) tuple, and writes the result as Parquet.

Why Parquet?
------------
- Columnar format: highly efficient for the two-column schema here.
- Native Spark support with pushdown predicates and partition pruning.
- Splittable and compressed out of the box (Snappy by default).
- Interoperable with virtually every data platform (Hive, Trino, BigQuery,
  Redshift Spectrum, Delta Lake, etc.).
- Schema enforcement prevents silent type coercions.

Deduplication strategy
-----------------------
On each run we:
  1. Read ALL new/incoming CSV files from the *input_dir*.
  2. Read the existing Parquet table from *output_dir* (if it exists).
  3. Union the two DataFrames and call dropDuplicates(["basket_id", "product_id"]).
  4. Overwrite the output table.

This is a "full-rewrite" incremental pattern – correct for datasets that
fit in Spark's distributed memory.  See the "Productionisation" section
in the README for Delta Lake / merge-based alternatives.

Usage
-----
    spark-submit incremental_ingest.py \\
        --input-dir  incoming/ \\
        --output-dir warehouse/sales_dedup/ \\
        [--processed-log processed_files.txt]

The optional --processed-log keeps a list of already-ingested files so
that re-runs only pick up genuinely new files (idempotent runs).
"""

import argparse
import os
import sys

# ---------------------------------------------------------------------------
# PySpark import with a friendly error message
# ---------------------------------------------------------------------------
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
except ImportError:
    sys.exit(
        "PySpark is not installed.  Install it with:\n"
        "    pip install pyspark\n"
        "or submit this script via spark-submit."
    )


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SCHEMA = StructType([
    StructField("basket_id",  StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_processed_log(log_path: str) -> set:
    """Return the set of file paths already processed."""
    if not os.path.exists(log_path):
        return set()
    with open(log_path) as f:
        return {line.strip() for line in f if line.strip()}


def append_processed_log(log_path: str, paths) -> None:
    """Append newly processed file paths to the log."""
    with open(log_path, "a") as f:
        for p in paths:
            f.write(p + "\n")


def find_new_files(input_dir: str, already_processed: set) -> list:
    """Return CSV / CSV.GZ files in *input_dir* not yet processed."""
    candidates = []
    for fname in os.listdir(input_dir):
        if fname.endswith(".csv") or fname.endswith(".csv.gz"):
            full = os.path.abspath(os.path.join(input_dir, fname))
            if full not in already_processed:
                candidates.append(full)
    return sorted(candidates)


# ---------------------------------------------------------------------------
# Main ingestion logic
# ---------------------------------------------------------------------------

def ingest(input_dir: str, output_dir: str, processed_log: str) -> None:
    already_processed = load_processed_log(processed_log)
    new_files = find_new_files(input_dir, already_processed)

    if not new_files:
        print("No new files to process. Exiting.")
        return

    print(f"Found {len(new_files)} new file(s) to ingest:")
    for f in new_files:
        print(f"  {f}")

    spark = (
        SparkSession.builder
        .appName("SalesIncrementalIngestion")
        # Tune for local testing; on a cluster remove these overrides.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read incoming files
    incoming_df = (
        spark.read
        .option("header", "false")
        .schema(SCHEMA)
        .csv(new_files)
    )

    # 2. Load existing deduplicated data (if any)
    if os.path.exists(output_dir) and os.listdir(output_dir):
        existing_df = spark.read.schema(SCHEMA).parquet(output_dir)
    else:
        # Empty DataFrame with the same schema
        existing_df = spark.createDataFrame([], SCHEMA)

    # 3. Union and deduplicate
    combined_df = existing_df.union(incoming_df)
    deduped_df  = combined_df.dropDuplicates(["basket_id", "product_id"])

    # 4. Overwrite the output table
    (
        deduped_df
        .write
        .mode("overwrite")
        .parquet(output_dir)
    )

    total = deduped_df.count()
    print(f"Deduplicated table written to '{output_dir}' ({total} records).")

    # 5. Record the processed files so we skip them next time
    append_processed_log(processed_log, new_files)
    print("Processed log updated.")

    spark.stop()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Incrementally ingest and deduplicate sales CSV files using Spark."
    )
    parser.add_argument(
        "--input-dir", required=True,
        help="Directory containing incoming CSV / CSV.GZ files."
    )
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory for the deduplicated Parquet output table."
    )
    parser.add_argument(
        "--processed-log", default="processed_files.txt",
        help="File tracking already-ingested paths (default: processed_files.txt)."
    )
    args = parser.parse_args()
    ingest(args.input_dir, args.output_dir, args.processed_log)
