"""
run_assignment1.py  –  End-to-end runner for Assignment 1
==========================================================
Usage:
    python run_assignment1.py data_1.csv.gz
    python run_assignment1.py data_1.csv.gz --shards 32 --output result.csv.gz

Steps performed:
    1. Split the input file into N shards (default 16).
    2. Compute co-occurrences from the shards.
    3. Print a small preview of the result.
"""

import argparse
import csv
import gzip
import sys
import os

# Allow running from the same directory without installing the package.
sys.path.insert(0, os.path.dirname(__file__))

from split import split
from cooccurrence import compute_cooccurrences


def preview(output_path: str, n: int = 10) -> None:
    print(f"\nTop {n} product pairs by co-occurrence count:")
    print(f"{'product_1':>12}  {'product_2':>12}  {'baskets':>10}")
    print("-" * 40)
    with gzip.open(output_path, "rt") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= n:
                break
            print(f"{row['product_1']:>12}  {row['product_2']:>12}  {row['baskets']:>10}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run full Assignment 1 pipeline.")
    parser.add_argument("input", help="Input .csv or .csv.gz file")
    parser.add_argument("--shards", type=int, default=16)
    parser.add_argument("--shard-dir", default="shards")
    parser.add_argument("--output", default="cooccurrences.csv.gz")
    args = parser.parse_args()

    print("=== Step 1: Splitting ===")
    split(args.input, args.shard_dir, args.shards)

    print("\n=== Step 2: Computing co-occurrences ===")
    compute_cooccurrences(args.shard_dir, args.output)

    preview(args.output)
