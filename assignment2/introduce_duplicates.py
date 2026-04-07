"""
introduce_duplicates.py  –  Test-data helper for Assignment 2
==============================================================
Takes one or more generated CSV.GZ files and writes new files with a
configurable fraction of rows duplicated (randomly).

Usage:
    python introduce_duplicates.py data_1.csv.gz --dup-ratio 0.2 --output-dir incoming/

This creates  incoming/data_1_with_dups.csv.gz  (and additional files if
you pass more than one input).
"""

import argparse
import csv
import gzip
import os
import random


def introduce_duplicates(input_path: str, output_path: str, dup_ratio: float, seed: int) -> None:
    rng = random.Random(seed)
    rows = []
    open_fn = gzip.open if input_path.endswith(".gz") else open
    with open_fn(input_path, "rt", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            rows.append(row)

    extra = [row for row in rows if rng.random() < dup_ratio]
    all_rows = rows + extra
    rng.shuffle(all_rows)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with gzip.open(output_path, "wt", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(all_rows)

    print(
        f"'{input_path}' → '{output_path}'  "
        f"({len(rows)} original + {len(extra)} duplicates = {len(all_rows)} total rows)"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Introduce duplicate rows into CSV files.")
    parser.add_argument("inputs", nargs="+", help="Input CSV / CSV.GZ file(s)")
    parser.add_argument("--dup-ratio", type=float, default=0.15,
                        help="Fraction of rows to duplicate (default: 0.15)")
    parser.add_argument("--output-dir", default="incoming",
                        help="Directory to write output files (default: incoming/)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility (default: 42)")
    args = parser.parse_args()

    for inp in args.inputs:
        basename = os.path.basename(inp).replace(".csv.gz", "").replace(".csv", "")
        out = os.path.join(args.output_dir, f"{basename}_with_dups.csv.gz")
        introduce_duplicates(inp, out, args.dup_ratio, args.seed)
