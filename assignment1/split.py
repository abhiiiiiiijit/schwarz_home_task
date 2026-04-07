"""
split.py  –  Subtask 1 of Assignment 1
=======================================
Reads the (potentially huge) sales CSV and splits it into N intermediate
files so that every basket is written to exactly ONE shard.

Splitting strategy
------------------
We hash the basket_id with a fast, deterministic hash and route the whole
basket to shard  hash(basket_id) % NUM_SHARDS.

Because ALL rows of a basket land in the SAME shard, we can later process
each shard independently (counting co-occurrences inside that shard) and
then simply merge the per-shard counts.  No basket ever "crosses" a shard
boundary, so no co-occurrence is missed or double-counted.

Memory usage
------------
We keep only the current line in memory while reading.  Each shard file is
opened in append mode so we never buffer more than one line of the input
at a time.
"""

import argparse
import csv
import gzip
import hashlib
import io
import os
import sys


def shard_for(basket_id: str, num_shards: int) -> int:
    """Return a deterministic shard index for *basket_id*."""
    digest = hashlib.md5(basket_id.encode()).hexdigest()
    return int(digest, 16) % num_shards


def split(input_path: str, output_dir: str, num_shards: int) -> None:
    os.makedirs(output_dir, exist_ok=True)

    # Open all shard writers up front so we don't repeatedly open/close files.
    shard_files = []
    shard_writers = []
    for i in range(num_shards):
        path = os.path.join(output_dir, f"shard_{i:04d}.csv.gz")
        fh = gzip.open(path, "wt", newline="")
        shard_files.append(fh)
        shard_writers.append(csv.writer(fh))

    try:
        open_fn = gzip.open if input_path.endswith(".gz") else open
        with open_fn(input_path, "rt", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2:
                    continue
                basket_id, product_id = row[0], row[1]
                idx = shard_for(basket_id, num_shards)
                shard_writers[idx].writerow([basket_id, product_id])
    finally:
        for fh in shard_files:
            fh.close()

    print(f"Split '{input_path}' into {num_shards} shards in '{output_dir}/'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split sales CSV into shards.")
    parser.add_argument("input", help="Path to input .csv or .csv.gz file")
    parser.add_argument(
        "--shards", type=int, default=16,
        help="Number of output shards (default: 16)"
    )
    parser.add_argument(
        "--output-dir", default="shards",
        help="Directory to write shard files (default: shards/)"
    )
    args = parser.parse_args()
    split(args.input, args.output_dir, args.shards)
