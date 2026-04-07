"""
k_cooccurrence.py  –  Bonus: Generalised k-combination co-occurrence
=====================================================================
Extends cooccurrence.py to support arbitrary k (default k=2 for pairs,
k=3 for triples, etc.).

The algorithm is identical to the pair case:
  1. Read each shard line-by-line, grouping rows by basket_id.
  2. For each basket, emit every sorted k-combination via itertools.combinations.
  3. Accumulate in a dict; merge across shards.

Memory analysis (k=3, scale=1)
-------------------------------
Max distinct triples = C(256, 3) = 2 730 240.  Each entry is a tuple of
3 strings + one int ≈ ~200 bytes → ~546 MB upper bound.  In practice far
fewer triples occur (baskets have at most 5 products, so at most C(5,3)=10
triples per basket).  For k=2 and k=3 the memory stays manageable;
for k=4+ consider writing partial results to disk between shards.

Usage
-----
    python k_cooccurrence.py --shard-dir shards/ --k 3 --output triples.csv.gz
    python k_cooccurrence.py --shard-dir shards/ --k 2 --output pairs.csv.gz
"""

import argparse
import csv
import gzip
import itertools
import os
import sys


def k_tuples_from_basket(products: list, k: int):
    """Yield every sorted k-combination of *products*."""
    unique_sorted = sorted(set(products), key=lambda x: int(x))
    yield from itertools.combinations(unique_sorted, k)


def process_shard_k(path: str, accumulator: dict, k: int) -> None:
    """Read one shard and add its k-tuple counts into *accumulator*."""
    open_fn = gzip.open if path.endswith(".gz") else open
    with open_fn(path, "rt", newline="") as f:
        reader = csv.reader(f)
        current_basket = None
        current_products: list = []
        for row in reader:
            if len(row) < 2:
                continue
            basket_id, product_id = row[0].strip(), row[1].strip()
            if basket_id != current_basket:
                if current_basket is not None and len(current_products) >= k:
                    for combo in k_tuples_from_basket(current_products, k):
                        accumulator[combo] = accumulator.get(combo, 0) + 1
                current_basket = basket_id
                current_products = [product_id]
            else:
                current_products.append(product_id)
        # flush last basket
        if current_basket is not None and len(current_products) >= k:
            for combo in k_tuples_from_basket(current_products, k):
                accumulator[combo] = accumulator.get(combo, 0) + 1


def compute_k_cooccurrences(shard_dir: str, output_path: str, k: int) -> None:
    shards = sorted(
        os.path.join(shard_dir, f)
        for f in os.listdir(shard_dir)
        if f.startswith("shard_") and (f.endswith(".csv") or f.endswith(".csv.gz"))
    )
    if not shards:
        sys.exit(f"No shard files found in '{shard_dir}'.")

    accumulator: dict = {}
    for i, shard_path in enumerate(shards, 1):
        print(f"  Processing shard {i}/{len(shards)}: {os.path.basename(shard_path)}")
        process_shard_k(shard_path, accumulator, k)

    header = [f"product_{i+1}" for i in range(k)] + ["baskets"]
    print(f"Writing {len(accumulator)} {k}-tuples to '{output_path}' …")
    with gzip.open(output_path, "wt", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for combo, count in sorted(accumulator.items(), key=lambda kv: (-kv[1], kv[0])):
            writer.writerow(list(combo) + [count])
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute k-product co-occurrences from shard files."
    )
    parser.add_argument("--shard-dir", default="shards",
                        help="Directory containing shard files (default: shards/)")
    parser.add_argument("--k", type=int, default=2,
                        help="Combination size: 2=pairs, 3=triples, … (default: 2)")
    parser.add_argument("--output", default=None,
                        help="Output file path (default: cooccurrences_k{k}.csv.gz)")
    args = parser.parse_args()

    output = args.output or f"cooccurrences_k{args.k}.csv.gz"
    compute_k_cooccurrences(args.shard_dir, output, args.k)