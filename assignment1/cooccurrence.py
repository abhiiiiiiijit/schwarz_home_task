"""
cooccurrence.py  –  Subtask 2 of Assignment 1
===============================================
Reads each shard produced by split.py and accumulates product-pair
co-occurrence counts, then writes the final result.

Algorithm (per shard)
---------------------
1. Read the shard line-by-line, grouping consecutive rows that share the
   same basket_id into a small in-memory list (each basket is tiny –
   at most 5 products per the data generator).
2. For each basket, emit every *sorted* pair (min_id, max_id) from the
   product list.
3. Accumulate pair → count in a plain dict for the current shard.

Merge
-----
After processing each shard we add its dict into a global accumulator.
Because each basket lives entirely in one shard, counts are correct.

Memory analysis
---------------
- One basket at a time: ≤ 5 product ids  → negligible.
- The accumulator dict grows with the number of DISTINCT pairs, not with
  the number of rows.  With P products there are at most P*(P-1)/2 pairs.
  For scale=1 that is 256*255/2 = 32 640 entries – tiny.
  For scale=10 it is 2560*2559/2 ≈ 3.3 M entries (a few hundred MB in the
  worst case, but typically much less because not all pairs co-occur).
- We never load a whole shard into memory.

Output
------
A gzip-compressed CSV with columns: product_1, product_2, baskets
(where product_1 < product_2 as integers).
"""

import argparse
import collections
import csv
import gzip
import os
import sys


def pairs_from_basket(products):
    """Yield every sorted 2-combination of *products* (list of str)."""
    products = sorted(set(products), key=lambda x: int(x))  # dedup + sort
    for i in range(len(products)):
        for j in range(i + 1, len(products)):
            yield products[i], products[j]


def process_shard(path: str, accumulator: dict) -> None:
    """Read one shard and add its pair counts into *accumulator*."""
    open_fn = gzip.open if path.endswith(".gz") else open
    with open_fn(path, "rt", newline="") as f:
        reader = csv.reader(f)
        current_basket = None
        current_products = []
        for row in reader:
            if len(row) < 2:
                continue
            basket_id, product_id = row[0].strip(), row[1].strip()
            if basket_id != current_basket:
                # Flush previous basket
                if current_basket is not None and len(current_products) >= 2:
                    for p1, p2 in pairs_from_basket(current_products):
                        accumulator[(p1, p2)] = accumulator.get((p1, p2), 0) + 1
                current_basket = basket_id
                current_products = [product_id]
            else:
                current_products.append(product_id)
        # Flush last basket in shard
        if current_basket is not None and len(current_products) >= 2:
            for p1, p2 in pairs_from_basket(current_products):
                accumulator[(p1, p2)] = accumulator.get((p1, p2), 0) + 1


def compute_cooccurrences(shard_dir: str, output_path: str) -> None:
    shards = sorted(
        os.path.join(shard_dir, f)
        for f in os.listdir(shard_dir)
        if f.startswith("shard_") and (f.endswith(".csv") or f.endswith(".csv.gz"))
    )
    if not shards:
        sys.exit(f"No shard files found in '{shard_dir}'.")

    accumulator = {}
    for i, shard_path in enumerate(shards, 1):
        print(f"  Processing shard {i}/{len(shards)}: {os.path.basename(shard_path)}")
        process_shard(shard_path, accumulator)

    print(f"Writing {len(accumulator)} co-occurrence pairs to '{output_path}' …")
    with gzip.open(output_path, "wt", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_1", "product_2", "baskets"])
        for (p1, p2), count in sorted(accumulator.items(), key=lambda kv: (-kv[1], kv[0])):
            writer.writerow([p1, p2, count])
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute product co-occurrences from shard files."
    )
    parser.add_argument(
        "--shard-dir", default="shards",
        help="Directory containing shard files (default: shards/)"
    )
    parser.add_argument(
        "--output", default="cooccurrences.csv.gz",
        help="Output file path (default: cooccurrences.csv.gz)"
    )
    args = parser.parse_args()
    compute_cooccurrences(args.shard_dir, args.output)
