"""
benchmark.py  –  Bonus: Runtime scaling measurement
=====================================================
Generates datasets at multiple scales, runs the full pipeline (split +
co-occurrence), records wall-clock times, and prints a summary table.

Usage
-----
    python benchmark.py                          # scales 1-4, 16 shards
    python benchmark.py --scales 1 2 3           # custom scale list
    python benchmark.py --scales 1 2 --shards 8
    python benchmark.py --scales 1 2 3 --k 3    # also measure triple-cooccurrence

What to expect
--------------
Both split() and compute_cooccurrences() are O(R) in the number of rows R.
The generator creates R = scale * 2**16 basket-rows and P = scale * 2**8
products.  Doubling the scale doubles R, so wall-clock time should grow
roughly linearly (slope ≈ 1 on a log-log plot).

The pair-count dict grows with the number of *distinct* pairs observed,
which is O(P²) in the worst case but in practice far smaller because
baskets are small (≤ 5 products) and random sampling keeps most pairs rare.
"""

import argparse
import csv
import gzip
import os
import shutil
import sys
import time

# ── locate siblings even when run from a different cwd ─────────────────────
sys.path.insert(0, os.path.dirname(__file__))

from generate_data import generate_data
from split import split
from cooccurrence import compute_cooccurrences
from k_cooccurrence import compute_k_cooccurrences


# ── helpers ─────────────────────────────────────────────────────────────────

def _rmdir(path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)


def _rm(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)


def _row_count(csv_gz_path: str) -> int:
    n = 0
    with gzip.open(csv_gz_path, "rt") as f:
        for _ in csv.reader(f):
            n += 1
    return n


def _result_count(csv_gz_path: str) -> int:
    """Number of data rows (excluding header) in a result file."""
    with gzip.open(csv_gz_path, "rt") as f:
        reader = csv.reader(f)
        next(reader, None)          # skip header
        return sum(1 for _ in reader)


# ── single benchmark run ─────────────────────────────────────────────────────

def run_one(scale: int, num_shards: int, k: int, workdir: str) -> dict:
    data_file   = os.path.join(workdir, f"data_{scale}.csv.gz")
    shard_dir   = os.path.join(workdir, f"shards_{scale}")
    result_file = os.path.join(workdir, f"result_{scale}_k{k}.csv.gz")

    # ── generate ──────────────────────────────────────────────────────────
    orig_dir = os.getcwd()
    os.chdir(workdir)
    t0 = time.perf_counter()
    ok = generate_data(scale)
    t_gen = time.perf_counter() - t0
    os.chdir(orig_dir)

    if not ok:
        raise RuntimeError(f"generate_data failed for scale={scale}")

    # rename to absolute path the pipeline expects
    generated = os.path.join(workdir, f"data_{scale}.csv.gz")
    row_count = _row_count(generated)

    # ── split ─────────────────────────────────────────────────────────────
    _rmdir(shard_dir)
    t0 = time.perf_counter()
    split(generated, shard_dir, num_shards)
    t_split = time.perf_counter() - t0

    # ── compute co-occurrences ────────────────────────────────────────────
    _rm(result_file)
    t0 = time.perf_counter()
    if k == 2:
        compute_cooccurrences(shard_dir, result_file)
    else:
        compute_k_cooccurrences(shard_dir, result_file, k)
    t_cooc = time.perf_counter() - t0

    pair_count = _result_count(result_file)

    # ── clean up ─────────────────────────────────────────────────────────
    _rmdir(shard_dir)
    _rm(generated)
    _rm(result_file)

    return {
        "scale":      scale,
        "rows":       row_count,
        "k":          k,
        "t_gen_s":    round(t_gen,   3),
        "t_split_s":  round(t_split, 3),
        "t_cooc_s":   round(t_cooc,  3),
        "t_total_s":  round(t_gen + t_split + t_cooc, 3),
        "pairs":      pair_count,
    }


# ── pretty table ──────────────────────────────────────────────────────────────

def print_table(results: list[dict], k: int) -> None:
    combo_label = "pairs" if k == 2 else f"{k}-tuples"
    print()
    print(f"{'scale':>6}  {'rows':>10}  {'t_gen(s)':>9}  "
          f"{'t_split(s)':>11}  {'t_cooc(s)':>10}  "
          f"{'t_total(s)':>11}  {combo_label:>10}")
    print("-" * 80)
    for r in results:
        print(
            f"{r['scale']:>6}  {r['rows']:>10,}  {r['t_gen_s']:>9.3f}  "
            f"{r['t_split_s']:>11.3f}  {r['t_cooc_s']:>10.3f}  "
            f"{r['t_total_s']:>11.3f}  {r['pairs']:>10,}"
        )
    print()

    # linear scaling check: ratio of consecutive total times
    if len(results) >= 2:
        print("Scaling ratios (t_total[i+1] / t_total[i])  — expect ~2.0 for linear:")
        for i in range(1, len(results)):
            prev, curr = results[i - 1], results[i]
            if prev["t_total_s"] > 0:
                ratio = curr["t_total_s"] / prev["t_total_s"]
                print(f"  scale {prev['scale']} → {curr['scale']}: {ratio:.2f}x")
        print()


# ── skew analysis (in-memory, tiny synthetic data) ───────────────────────────

def analyse_skew() -> None:
    """
    Demonstrate that skewed co-occurrence counts do not affect correctness
    by constructing a tiny synthetic dataset where one pair dominates and
    verifying the counts match a brute-force reference.
    """
    import itertools, collections, io, tempfile, csv as _csv

    print("=== Skew Analysis ===")
    print("Building synthetic skewed dataset …")

    # Products 0 and 1 appear together in EVERY basket; others are rare.
    rng_state = 42
    import random
    rng = random.Random(rng_state)

    baskets = []
    for i in range(500):
        # always include products 0 and 1
        products = {0, 1}
        # add 0–3 random products from {2..20}
        products.update(rng.sample(range(2, 21), rng.randint(0, 3)))
        baskets.append((str(i), sorted(products)))

    # brute-force reference counts
    ref: dict = collections.Counter()
    for _, prods in baskets:
        for p1, p2 in itertools.combinations(prods, 2):
            ref[(str(p1), str(p2))] += 1

    # write to a temp CSV, run the pipeline, compare
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "skew_test.csv")
        with open(csv_path, "w", newline="") as f:
            w = _csv.writer(f)
            for bid, prods in baskets:
                for p in prods:
                    w.writerow([bid, str(p)])

        shard_dir  = os.path.join(tmpdir, "shards")
        result_csv = os.path.join(tmpdir, "result.csv.gz")
        split(csv_path, shard_dir, num_shards=4)
        compute_cooccurrences(shard_dir, result_csv)

        pipeline_counts: dict = {}
        with gzip.open(result_csv, "rt") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                pipeline_counts[(row["product_1"], row["product_2"])] = int(row["baskets"])

    # compare
    mismatches = 0
    for key, ref_count in ref.items():
        pipeline_count = pipeline_counts.get(key, 0)
        if pipeline_count != ref_count:
            print(f"  MISMATCH {key}: expected {ref_count}, got {pipeline_count}")
            mismatches += 1

    dominant_pair = ("0", "1")
    dom_count = pipeline_counts.get(dominant_pair, 0)
    print(f"  Dominant pair {dominant_pair} count: {dom_count}  "
          f"(expected: {ref[dominant_pair]}, matches: {dom_count == ref[dominant_pair]})")
    print(f"  Total pairs in output: {len(pipeline_counts)}")
    print(f"  Mismatches vs brute-force: {mismatches}")
    if mismatches == 0:
        print("  ✓ Skewed dataset: all counts match the brute-force reference.\n")
    else:
        print("  ✗ Skewed dataset: mismatches found!\n")


# ── main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark the co-occurrence pipeline across multiple scales."
    )
    parser.add_argument(
        "--scales", nargs="+", type=int, default=[1, 2, 3],
        help="Scale values to benchmark (default: 1 2 3)"
    )
    parser.add_argument(
        "--shards", type=int, default=16,
        help="Number of shards (default: 16)"
    )
    parser.add_argument(
        "--k", type=int, default=2,
        help="Combination size: 2=pairs, 3=triples (default: 2)"
    )
    parser.add_argument(
        "--workdir", default="/tmp/benchmark_work",
        help="Temporary directory for generated files (default: /tmp/benchmark_work)"
    )
    parser.add_argument(
        "--skip-skew", action="store_true",
        help="Skip the skew-correctness analysis"
    )
    args = parser.parse_args()

    os.makedirs(args.workdir, exist_ok=True)

    print(f"Benchmarking scales {args.scales} with {args.shards} shards, k={args.k} …\n")
    results = []
    for scale in args.scales:
        print(f"--- scale={scale} ---")
        result = run_one(scale, args.shards, args.k, args.workdir)
        results.append(result)
        print(
            f"  rows={result['rows']:,}  "
            f"t_total={result['t_total_s']:.3f}s  "
            f"pairs={result['pairs']:,}\n"
        )

    print_table(results, args.k)

    if not args.skip_skew:
        analyse_skew()