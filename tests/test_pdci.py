import os
import numpy as np
from scipy.sparse import rand
import pytest

from rogers.index.pdci import PrioritizedDynamicContinuousIndex, SQLiteIndexer
from sklearn.neighbors import BallTree

INDEX_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), 'index.db'))


def test_index():
    xs = rand(1000, 100, random_state=42).tocsc()

    try:
        indexer = SQLiteIndexer(index_path=INDEX_PATH)
        index = PrioritizedDynamicContinuousIndex(indexer, composite_indices=2, simple_indices=50)
        index.fit(xs)

        x = xs[0]
        k = 10

        nn_baseline = BallTree(xs.todense())

        baseline_dist, baseline_idx = nn_baseline.query(x.todense(), k=k)
        dist, idx = index.query(x, k=k)

        np.testing.assert_equal(baseline_idx[0], idx)
    finally:
        if os.path.exists(INDEX_PATH):
            os.remove(INDEX_PATH)


def test_index_partial_fit():
    xs = rand(1000, 100, random_state=42).tocsc()

    try:
        indexer = SQLiteIndexer(index_path=INDEX_PATH)
        index = PrioritizedDynamicContinuousIndex(indexer, composite_indices=2, simple_indices=50)

        index.fit(xs[0:500])
        index.partial_fit(xs[500:1000])

        assert index.indexer.xs.shape == (1000, 100)
    finally:
        if os.path.exists(INDEX_PATH):
            os.remove(INDEX_PATH)
