"""
tests/test_assignment1.py
=========================
Professional unit + integration test suite for the Assignment-1 pipeline.

Compatible with both pytest and the stdlib unittest runner (no external
dependencies beyond the standard library).

Coverage
--------
split.py          – shard routing, determinism, uniform distribution,
                    no-basket-split guarantee, row completeness, edge cases
cooccurrence.py   – pair generation, empty / single-product baskets,
                    accumulator merging, full pipeline correctness vs.
                    brute-force reference
k_cooccurrence.py – generalisation to k=3 (triples), header, edge cases,
                    consistency with k=2 output
Skew              – correctness when one product pair dominates heavily
Integration       – end-to-end pipeline with many / one shard, mixed k

Run
---
    # with pytest (recommended):
    pytest tests/test_assignment1.py -v

    # without pytest (stdlib only):
    python -m unittest tests.test_assignment1 -v
    # or from inside the tests/ directory:
    python -m unittest test_assignment1 -v

All tests are self-contained: they write to stdlib-managed temp directories
and clean up after themselves; no network access is required.
"""

import collections
import csv
import gzip
import itertools
import os
import random
import sys
import tempfile
import unittest

# ── resolve the assignment1 package path ─────────────────────────────────────
_HERE = os.path.abspath(os.path.dirname(__file__))
_ASSIGNMENT1_DIR = os.path.abspath(os.path.join(_HERE, "..", "assignment1"))
if _ASSIGNMENT1_DIR not in sys.path:
    sys.path.insert(0, _ASSIGNMENT1_DIR)

from split import shard_for, split
from cooccurrence import pairs_from_basket, process_shard, compute_cooccurrences
from k_cooccurrence import k_tuples_from_basket, compute_k_cooccurrences


# ════════════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════════════

def write_csv(path: str, rows) -> None:
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)


def write_csv_gz(path: str, rows) -> None:
    with gzip.open(path, "wt", newline="") as f:
        csv.writer(f).writerows(rows)


def read_result_pairs(path: str) -> dict:
    """Return {frozenset({p1, p2, …}): count} from a result CSV.GZ."""
    result = {}
    with gzip.open(path, "rt") as f:
        for row in csv.DictReader(f):
            key = frozenset(v for k, v in row.items() if k.startswith("product_"))
            result[key] = int(row["baskets"])
    return result


def brute_force_pairs(baskets: dict) -> dict:
    counts: dict = collections.Counter()
    for products in baskets.values():
        unique = sorted(set(products), key=int)
        for p1, p2 in itertools.combinations(unique, 2):
            counts[frozenset({p1, p2})] += 1
    return dict(counts)


def brute_force_k(baskets: dict, k: int) -> dict:
    counts: dict = collections.Counter()
    for products in baskets.values():
        unique = sorted(set(products), key=int)
        for combo in itertools.combinations(unique, k):
            counts[frozenset(combo)] += 1
    return dict(counts)


def run_pipeline(rows, tmp_dir: str, num_shards: int = 4) -> str:
    inp       = os.path.join(tmp_dir, "input.csv")
    shard_dir = os.path.join(tmp_dir, "shards")
    result    = os.path.join(tmp_dir, "result.csv.gz")
    write_csv(inp, rows)
    split(inp, shard_dir, num_shards)
    compute_cooccurrences(shard_dir, result)
    return result


def run_pipeline_k(rows, tmp_dir: str, k: int, num_shards: int = 4) -> str:
    inp       = os.path.join(tmp_dir, "input.csv")
    shard_dir = os.path.join(tmp_dir, "shards_k")
    result    = os.path.join(tmp_dir, f"result_k{k}.csv.gz")
    write_csv(inp, rows)
    split(inp, shard_dir, num_shards)
    compute_k_cooccurrences(shard_dir, result, k)
    return result


# ════════════════════════════════════════════════════════════════════════════
# split.py – shard_for()
# ════════════════════════════════════════════════════════════════════════════

class TestShardFor(unittest.TestCase):

    def test_deterministic(self):
        """Same basket_id must always map to the same shard."""
        for _ in range(20):
            self.assertEqual(shard_for("basket-abc", 16), shard_for("basket-abc", 16))

    def test_in_range(self):
        """Shard index must always be in [0, num_shards)."""
        for n in [1, 4, 16, 256]:
            for bid in ["a", "b", "basket-999", "some-uuid-xyz"]:
                idx = shard_for(bid, n)
                self.assertGreaterEqual(idx, 0)
                self.assertLess(idx, n)

    def test_single_shard_always_zero(self):
        """With num_shards=1 every basket maps to shard 0."""
        for bid in ["x", "y", "z", "another"]:
            self.assertEqual(shard_for(bid, 1), 0)

    def test_distribution_roughly_uniform(self):
        """With many basket IDs the shards should be roughly balanced (< 2x avg)."""
        rng = random.Random(0)
        counts = [0] * 16
        for _ in range(10_000):
            counts[shard_for(str(rng.random()), 16)] += 1
        avg = sum(counts) / len(counts)
        self.assertLess(max(counts), avg * 2)

    def test_different_ids_map_to_different_shards(self):
        """Not all basket IDs should collide onto the same shard."""
        shards = {shard_for(f"basket-{i}", 16) for i in range(100)}
        self.assertGreater(len(shards), 1)


# ════════════════════════════════════════════════════════════════════════════
# split.py – split()
# ════════════════════════════════════════════════════════════════════════════

class TestSplitFunction(unittest.TestCase):

    ROWS = [
        ("basket-1", "10"),
        ("basket-1", "20"),
        ("basket-2", "10"),
        ("basket-2", "30"),
        ("basket-3", "40"),
    ]

    def setUp(self):
        self._tmp = tempfile.mkdtemp()

    def _split(self, rows, num_shards=4, gz=False):
        if gz:
            inp = os.path.join(self._tmp, "input.csv.gz")
            write_csv_gz(inp, rows)
        else:
            inp = os.path.join(self._tmp, "input.csv")
            write_csv(inp, rows)
        out_dir = os.path.join(self._tmp, "shards")
        split(inp, out_dir, num_shards)
        return out_dir

    def _collect(self, out_dir):
        rows = []
        for fname in sorted(os.listdir(out_dir)):
            with gzip.open(os.path.join(out_dir, fname), "rt") as f:
                rows.extend(list(csv.reader(f)))
        return rows

    def test_creates_correct_number_of_shard_files(self):
        out_dir = self._split(self.ROWS, num_shards=4)
        self.assertEqual(len(os.listdir(out_dir)), 4)

    def test_all_shard_files_are_gzipped_csv(self):
        out_dir = self._split(self.ROWS, num_shards=4)
        for fname in os.listdir(out_dir):
            self.assertTrue(fname.endswith(".csv.gz"), fname)

    def test_all_rows_are_preserved(self):
        out_dir = self._split(self.ROWS, num_shards=4)
        self.assertEqual(len(self._collect(out_dir)), len(self.ROWS))

    def test_basket_never_split_across_shards(self):
        out_dir = self._split(self.ROWS, num_shards=4)
        basket_shards: dict = collections.defaultdict(set)
        for fname in sorted(os.listdir(out_dir)):
            with gzip.open(os.path.join(out_dir, fname), "rt") as f:
                for row in csv.reader(f):
                    basket_shards[row[0]].add(fname)
        for bid, shards in basket_shards.items():
            self.assertEqual(len(shards), 1,
                f"Basket {bid!r} found in multiple shards: {shards}")

    def test_handles_gzip_input(self):
        out_dir = self._split(self.ROWS, num_shards=4, gz=True)
        self.assertEqual(len(os.listdir(out_dir)), 4)

    def test_single_shard_contains_all_rows(self):
        out_dir = self._split(self.ROWS, num_shards=1)
        self.assertEqual(len(os.listdir(out_dir)), 1)
        self.assertEqual(len(self._collect(out_dir)), len(self.ROWS))

    def test_skips_rows_with_fewer_than_two_columns(self):
        rows = [("basket-1", "10"), ("",), ("basket-2", "20")]
        out_dir = self._split(rows, num_shards=2)
        self.assertEqual(len(self._collect(out_dir)), 2)

    def test_empty_input_produces_empty_shards(self):
        out_dir = self._split([], num_shards=4)
        self.assertEqual(self._collect(out_dir), [])


# ════════════════════════════════════════════════════════════════════════════
# cooccurrence.py – pairs_from_basket()
# ════════════════════════════════════════════════════════════════════════════

class TestPairsFromBasket(unittest.TestCase):

    def test_two_products_yields_one_pair(self):
        self.assertEqual(list(pairs_from_basket(["1", "2"])), [("1", "2")])

    def test_three_products_yields_three_pairs(self):
        pairs = list(pairs_from_basket(["3", "1", "2"]))
        self.assertIn(("1", "2"), pairs)
        self.assertIn(("1", "3"), pairs)
        self.assertIn(("2", "3"), pairs)
        self.assertEqual(len(pairs), 3)

    def test_single_product_yields_nothing(self):
        self.assertEqual(list(pairs_from_basket(["5"])), [])

    def test_empty_yields_nothing(self):
        self.assertEqual(list(pairs_from_basket([])), [])

    def test_deduplicates_repeated_products(self):
        self.assertEqual(list(pairs_from_basket(["1", "1", "2"])), [("1", "2")])

    def test_output_always_sorted_ascending(self):
        for p1, p2 in pairs_from_basket(["9", "1", "5"]):
            self.assertLess(int(p1), int(p2))

    def test_five_products_yields_ten_pairs(self):
        self.assertEqual(len(list(pairs_from_basket(["1","2","3","4","5"]))), 10)


# ════════════════════════════════════════════════════════════════════════════
# cooccurrence.py – process_shard() and compute_cooccurrences()
# ════════════════════════════════════════════════════════════════════════════

class TestProcessShard(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()

    def _shard(self, rows, name="shard.csv"):
        path = os.path.join(self._tmp, name)
        write_csv(path, rows)
        return path

    def _shard_gz(self, rows, name="shard.csv.gz"):
        path = os.path.join(self._tmp, name)
        write_csv_gz(path, rows)
        return path

    def test_basic_pair_counts(self):
        rows = [
            ("b1", "1"), ("b1", "2"),
            ("b2", "1"), ("b2", "2"), ("b2", "3"),
            ("b3", "1"),
        ]
        acc = {}
        process_shard(self._shard(rows), acc)
        self.assertEqual(acc[("1", "2")], 2)
        self.assertEqual(acc[("1", "3")], 1)
        self.assertEqual(acc[("2", "3")], 1)

    def test_gzipped_shard(self):
        rows = [("b1", "10"), ("b1", "20"), ("b2", "10"), ("b2", "20")]
        acc = {}
        process_shard(self._shard_gz(rows), acc)
        self.assertEqual(acc[("10", "20")], 2)

    def test_accumulates_into_existing_dict(self):
        rows = [("b1", "1"), ("b1", "2")]
        acc = {("1", "2"): 5}
        process_shard(self._shard(rows), acc)
        self.assertEqual(acc[("1", "2")], 6)

    def test_empty_shard_leaves_accumulator_unchanged(self):
        acc = {}
        process_shard(self._shard([]), acc)
        self.assertEqual(acc, {})

    def test_single_product_baskets_produce_no_pairs(self):
        rows = [("b1", "1"), ("b2", "2"), ("b3", "3")]
        acc = {}
        process_shard(self._shard(rows), acc)
        self.assertEqual(acc, {})


class TestComputeCooccurrences(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()

    def test_example_from_task_spec(self):
        """Exactly the worked example on page 2 of the PDF."""
        rows = [
            ("123", "1"), ("123", "2"),
            ("456", "1"), ("456", "2"), ("456", "3"),
            ("789", "1"),
        ]
        result = run_pipeline(rows, self._tmp)
        got = read_result_pairs(result)
        self.assertEqual(got[frozenset({"1", "2"})], 2)
        self.assertEqual(got[frozenset({"1", "3"})], 1)
        self.assertEqual(got[frozenset({"2", "3"})], 1)
        self.assertNotIn(frozenset({"1"}), got)

    def test_matches_brute_force_reference(self):
        rng = random.Random(99)
        products = [str(i) for i in range(20)]
        baskets = {
            f"basket-{i}": rng.sample(products, rng.randint(1, 5))
            for i in range(200)
        }
        rows = [(bid, p) for bid, prods in baskets.items() for p in prods]
        result = run_pipeline(rows, self._tmp, num_shards=8)
        self.assertEqual(read_result_pairs(result), brute_force_pairs(baskets))

    def test_all_single_product_baskets_yield_empty_result(self):
        rows = [("b1", "1"), ("b2", "2"), ("b3", "3")]
        result = run_pipeline(rows, self._tmp)
        self.assertEqual(read_result_pairs(result), {})

    def test_result_file_has_correct_header(self):
        rows = [("b1", "1"), ("b1", "2")]
        result = run_pipeline(rows, self._tmp)
        with gzip.open(result, "rt") as f:
            header = next(csv.reader(f))
        self.assertIn("product_1", header)
        self.assertIn("product_2", header)
        self.assertIn("baskets",   header)

    def test_all_counts_are_positive_integers(self):
        rng = random.Random(7)
        rows = []
        for i in range(50):
            prods = rng.sample([str(j) for j in range(10)], rng.randint(2, 4))
            rows += [(f"b{i}", p) for p in prods]
        result = run_pipeline(rows, self._tmp)
        for count in read_result_pairs(result).values():
            self.assertGreater(count, 0)
            self.assertIsInstance(count, int)


# ════════════════════════════════════════════════════════════════════════════
# k_cooccurrence.py – k_tuples_from_basket() and compute_k_cooccurrences()
# ════════════════════════════════════════════════════════════════════════════

class TestKTuplesFromBasket(unittest.TestCase):

    def test_triples_count_is_c_n_3(self):
        self.assertEqual(len(list(k_tuples_from_basket(["1","2","3","4"], k=3))), 4)

    def test_k2_matches_pairs_from_basket(self):
        products = ["3", "1", "5", "2"]
        self.assertEqual(
            sorted(k_tuples_from_basket(products, k=2)),
            sorted(pairs_from_basket(products))
        )

    def test_k1_yields_singletons(self):
        self.assertEqual(len(list(k_tuples_from_basket(["1","2","3"], k=1))), 3)

    def test_k_larger_than_basket_yields_nothing(self):
        self.assertEqual(list(k_tuples_from_basket(["1","2"], k=3)), [])

    def test_deduplicates_inputs(self):
        self.assertEqual(
            len(list(k_tuples_from_basket(["1","1","2","2","3"], k=2))), 3)

    def test_five_choose_three(self):
        self.assertEqual(
            len(list(k_tuples_from_basket([str(i) for i in range(5)], k=3))), 10)


class TestKCooccurrences(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()

    def test_triples_match_brute_force(self):
        rng = random.Random(7)
        products = [str(i) for i in range(15)]
        baskets = {
            f"b-{i}": rng.sample(products, rng.randint(3, 5))
            for i in range(150)
        }
        rows = [(bid, p) for bid, prods in baskets.items() for p in prods]
        result = run_pipeline_k(rows, self._tmp, k=3)
        self.assertEqual(read_result_pairs(result), brute_force_k(baskets, k=3))

    def test_triples_header_has_three_product_columns(self):
        rows = [("b1", "1"), ("b1", "2"), ("b1", "3")]
        result = run_pipeline_k(rows, self._tmp, k=3)
        with gzip.open(result, "rt") as f:
            header = next(csv.reader(f))
        self.assertEqual(header, ["product_1", "product_2", "product_3", "baskets"])

    def test_no_triples_when_baskets_too_small(self):
        rows = [("b1", "1"), ("b1", "2"), ("b2", "3"), ("b2", "4")]
        result = run_pipeline_k(rows, self._tmp, k=3)
        self.assertEqual(read_result_pairs(result), {})

    def test_single_triple_in_one_basket(self):
        rows = [("b1", "1"), ("b1", "2"), ("b1", "3")]
        result = run_pipeline_k(rows, self._tmp, k=3)
        self.assertEqual(
            read_result_pairs(result),
            {frozenset({"1", "2", "3"}): 1}
        )

    def test_triple_count_accumulates_across_baskets(self):
        rows = [
            ("b1", "1"), ("b1", "2"), ("b1", "3"),
            ("b2", "1"), ("b2", "2"), ("b2", "3"),
        ]
        result = run_pipeline_k(rows, self._tmp, k=3)
        self.assertEqual(
            read_result_pairs(result)[frozenset({"1", "2", "3"})], 2)


# ════════════════════════════════════════════════════════════════════════════
# Bonus – Skew correctness
# ════════════════════════════════════════════════════════════════════════════

class TestSkewedData(unittest.TestCase):
    """
    Verifies correctness when one product pair appears in every basket while
    all other pairs are rare.  Skewed counts must not cause over/under-counting.
    """

    def _skewed_baskets(self, n=200, seed=42):
        rng = random.Random(seed)
        baskets = {}
        for i in range(n):
            prods = {0, 1}
            prods.update(rng.sample(range(2, 21), rng.randint(0, 3)))
            baskets[str(i)] = [str(p) for p in sorted(prods)]
        return baskets

    def setUp(self):
        self._tmp = tempfile.mkdtemp()

    def test_dominant_pair_count_is_correct(self):
        baskets  = self._skewed_baskets()
        rows     = [(bid, p) for bid, prods in baskets.items() for p in prods]
        result   = run_pipeline(rows, self._tmp)
        got      = read_result_pairs(result)
        expected = brute_force_pairs(baskets)
        self.assertEqual(got[frozenset({"0","1"})], expected[frozenset({"0","1"})])

    def test_full_correctness_under_skew(self):
        baskets = self._skewed_baskets()
        rows    = [(bid, p) for bid, prods in baskets.items() for p in prods]
        result  = run_pipeline(rows, self._tmp)
        self.assertEqual(read_result_pairs(result), brute_force_pairs(baskets))

    def test_extreme_skew_only_one_pair(self):
        baskets = {str(i): ["0", "1"] for i in range(100)}
        rows    = [(bid, p) for bid, prods in baskets.items() for p in prods]
        result  = run_pipeline(rows, self._tmp)
        self.assertEqual(
            read_result_pairs(result),
            {frozenset({"0", "1"}): 100}
        )

    def test_rare_pairs_counted_correctly_alongside_dominant_pair(self):
        baskets = {str(i): ["0", "1"] for i in range(98)}
        baskets["special-a"] = ["0", "1", "2"]
        baskets["special-b"] = ["0", "1", "2"]
        rows   = [(bid, p) for bid, prods in baskets.items() for p in prods]
        result = run_pipeline(rows, self._tmp)
        got    = read_result_pairs(result)
        self.assertEqual(got[frozenset({"0","1"})], 100)
        self.assertEqual(got[frozenset({"0","2"})], 2)
        self.assertEqual(got[frozenset({"1","2"})], 2)


# ════════════════════════════════════════════════════════════════════════════
# Integration tests
# ════════════════════════════════════════════════════════════════════════════

class TestEndToEnd(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()

    def test_more_shards_than_baskets(self):
        """Empty shards must not break the pipeline."""
        rng = random.Random(1)
        baskets = {f"b-{i}": rng.sample(["1","2","3","4","5"], 3) for i in range(5)}
        rows   = [(bid, p) for bid, prods in baskets.items() for p in prods]
        result = run_pipeline(rows, self._tmp, num_shards=32)
        self.assertEqual(read_result_pairs(result), brute_force_pairs(baskets))

    def test_single_shard_degenerate_case(self):
        baskets = {"b1": ["1","2","3"], "b2": ["2","3"]}
        rows   = [(bid, p) for bid, prods in baskets.items() for p in prods]
        result = run_pipeline(rows, self._tmp, num_shards=1)
        self.assertEqual(read_result_pairs(result), brute_force_pairs(baskets))

    def test_k2_and_k3_results_are_mutually_consistent(self):
        """Every triple (a,b,c) implies all three pairs must exist in k=2 output."""
        rng = random.Random(55)
        products = [str(i) for i in range(10)]
        baskets  = {f"b-{i}": rng.sample(products, 4) for i in range(50)}
        rows     = [(bid, p) for bid, prods in baskets.items() for p in prods]

        inp        = os.path.join(self._tmp, "input.csv")
        shard_dir  = os.path.join(self._tmp, "shards")
        result2    = os.path.join(self._tmp, "result_k2.csv.gz")
        result3    = os.path.join(self._tmp, "result_k3.csv.gz")

        write_csv(inp, rows)
        split(inp, shard_dir, 4)
        compute_cooccurrences(shard_dir, result2)
        compute_k_cooccurrences(shard_dir, result3, k=3)

        pairs   = read_result_pairs(result2)
        triples = read_result_pairs(result3)

        for triple_key in triples:
            for p1, p2 in itertools.combinations(sorted(triple_key), 2):
                self.assertIn(
                    frozenset({p1, p2}), pairs,
                    f"Pair {{{p1},{p2}}} missing but triple {triple_key} exists"
                )

    def test_large_random_dataset_matches_brute_force(self):
        """Stress test: 1 000 baskets, 50 products."""
        rng = random.Random(2024)
        products = [str(i) for i in range(50)]
        baskets  = {
            f"basket-{i}": rng.sample(products, rng.randint(1, 5))
            for i in range(1_000)
        }
        rows   = [(bid, p) for bid, prods in baskets.items() for p in prods]
        result = run_pipeline(rows, self._tmp, num_shards=16)
        self.assertEqual(read_result_pairs(result), brute_force_pairs(baskets))

    def test_gzipped_input_end_to_end(self):
        rng = random.Random(3)
        products = [str(i) for i in range(10)]
        baskets  = {f"b{i}": rng.sample(products, 3) for i in range(30)}
        rows     = [(bid, p) for bid, prods in baskets.items() for p in prods]

        inp_gz    = os.path.join(self._tmp, "input.csv.gz")
        shard_dir  = os.path.join(self._tmp, "shards")
        result    = os.path.join(self._tmp, "result.csv.gz")

        write_csv_gz(inp_gz, rows)
        split(inp_gz, shard_dir, 4)
        compute_cooccurrences(shard_dir, result)
        self.assertEqual(read_result_pairs(result), brute_force_pairs(baskets))


# ════════════════════════════════════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)