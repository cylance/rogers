""" Local implementation of Prioritized Dynamic Continuous Indexing
"""
from ... import config as c

import os
import sqlite3
import scipy.sparse
import numpy as np

from queue import PriorityQueue
from sklearn.base import BaseEstimator
from sklearn import random_projection
from contextlib import contextmanager
from sklearn.metrics import pairwise_distances


class SQLiteIndexer(object):
    """ Indexer using local sqlite db as backing store
    """
    def __init__(self, index_path=None):
        index_path = index_path or os.path.join(c.settings.get("INDEX_DIR"), 'pdci.db')
        self._db = sqlite3.connect(index_path)
        self.xs = None
        self.ys = None

    @property
    def n(self):
        with self.db() as db:
            return db.execute('SELECT COUNT(value) FROM index_0;').fetchone()[0]

    def init(self, n_indicies):
        with self.db() as db:
            for i in range(n_indicies):
                db.executescript("""
                BEGIN;
                DROP TABLE IF EXISTS index_{0};
                CREATE TABLE index_{0} (key TEXT, value REAL);
                CREATE INDEX value_idx_{0} ON index_{0} (value);
                COMMIT;""".format(i))

    @contextmanager
    def db(self):
        """ db cursor
        :return:
        """
        cur = self._db.cursor()
        try:
            yield cur
        finally:
            self._db.commit()
            cur.close()

    def insert(self, xs, projected_values):
        """ Set or concat xs to self.xs and insert projected values into db
        TODO: make this better
        :param xs:
        :param projected_values:
        :return:
        """
        if self.xs is None:
            offset = 0
            self.xs = xs
        else:
            offset = self.xs.shape[0]
            self.xs = scipy.sparse.vstack([self.xs, xs]).tocsc()

        with self.db() as db:
            for indices, x in np.ndenumerate(projected_values):
                i, idx = indices
                db.execute("INSERT INTO index_{0} (key, value) VALUES ('{1}', {2});".format(idx, i + offset, x))

    def scan(self, idx, x):
        """ Scan through simple index for candidate point and yield nearest points
        TODO: make this better
        :param idx: simple index
        :param x: projected query point using
        :return:
        """
        with self.db() as cur_a, self.db() as cur_b:
            try:
                # find sequential points in index for b < x <= a
                # increasing points
                a_scan = cur_a.execute("SELECT * FROM index_{0} WHERE value >= {1} ORDER BY value ASC".format(idx, x))
                # decreasing points
                b_scan = cur_b.execute("SELECT * FROM index_{0} WHERE value < {1} ORDER BY value DESC".format(idx, x))
                a = a_scan.fetchone()
                b = b_scan.fetchone()
                while True:
                    if a is None and b is None:
                        # no points left to fetch
                        raise StopIteration
                    elif a is None and b is not None:
                        # only have b points
                        i, val = b
                        dist = abs(val - x)
                        b = b_scan.fetchone()
                    elif b is None and a is not None:
                        # only have a points
                        i, val = a
                        dist = abs(val - x)
                        a = a_scan.fetchone()
                    else:
                        # return point that's closer to x
                        a_idx, a_val = a
                        b_idx, b_val = b
                        a_dist = abs(a_val - x)
                        b_dist = abs(b_val - x)
                        if a_dist < b_dist:
                            dist = a_dist
                            i = a_idx
                            a = a_scan.fetchone()
                        else:
                            dist = b_dist
                            i = b_idx
                            b = b_scan.fetchone()
                    yield (dist, (idx, i))
            except TypeError:
                pass


class PrioritizedDynamicContinuousIndex(BaseEstimator):
    """ Nearest neighbor method described by Li and Malik in https://arxiv.org/abs/1703.00440
    """

    def __init__(self, indexer, simple_indices=10, composite_indices=2):
        """ Initialize PDCI within Indexer and parameters
        :param indexer: Indexer class instance
        :param simple_indices: Number of simple indices
        :param composite_indices:  Number of composite indices
        """
        self.simple_indices = simple_indices
        self.composite_indices = composite_indices
        self.n_indices = self.simple_indices * self.composite_indices
        self.indexer = indexer
        # placeholder for random unit vector projections
        self.random_unit_vectors = None

    @property
    def n(self):
        """ Number of samples in index
        :return:
        """
        return self.indexer.n

    @property
    def d(self):
        """ Dimensionality of samples based on fit random unit vectors
        :return:
        """
        return self.random_unit_vectors.components_.shape[1]

    def fit(self, X):
        """ Create random unit vectors and index X
        :param X: sparse csc matrix of samples
        :return:
        """
        self.indexer.init(self.n_indices)
        self.random_unit_vectors = random_projection.GaussianRandomProjection(n_components=self.n_indices)
        self.random_unit_vectors.fit(X)
        self.partial_fit(X)

    def partial_fit(self, X):
        """ Update index with X
        :param X: sparse csc matrix of samples
        :return:
        """
        xs_t = self.random_unit_vectors.transform(X)
        self.indexer.insert(X, xs_t)

    def omega_k_retrieve(self, k=1, d=None):
        """ Worst case bound on number of samples to retrieve per composite index
        :param k: nearest neighbor parameter
        :param d: intrinsic dimensionality
        :return:
        """
        d = d or self.d
        return int(k * max(np.log(self.n / k),
                           np.power((self.n / k),
                                    (1 - (self.simple_indices / d)))))

    def omega_k_visit(self, k=1, d=None):
        """ Worst case bound on number of samples to visit across all simple indices
        :param k: nearest neighbor parameter
        :param d: intrinsic dimensionality
        :return:
        """
        d = d or self.d
        return int(self.simple_indices * k * max(np.log(self.n / k),
                                                 np.power((self.n / k), (1 - (1 / d)))))

    def query(self, x, k=10, k_retrieve=None, k_visit=None):
        """ Query in an input sample x and return k nearest neighbors
        :param x: sparse csc matrix (n, d)
        :param k: nearest neighbors to return
        :param k_retrieve: optional parameter for number of samples to retrieve per composite index else use worst case
        :param k_visit: optional parameter for number of samples to visit across simple indices else use worst case
        :return: tuple np.array (raw_distances, self.indexer.xs sample idxs)
        """
        # number of points to retrieve
        k_retrieve = k_retrieve or self.omega_k_retrieve(k)
        # points to visit in each composite index
        k_visit = k_visit or self.omega_k_visit(k)
        # projected points for query
        x_t = self.random_unit_vectors.transform(x)

        candidate_sets = [set([]) for l in range(self.composite_indices)]
        composite_observations = [{} for l in range(self.composite_indices)]

        scanners = {}
        composite_queues = [PriorityQueue() for _ in range(self.composite_indices)]
        # construct priority queues from composite indices
        for jl in range(self.n_indices):
            # map simple index to composite index
            l = int(np.floor(jl / np.float(self.simple_indices)))
            scanners[jl] = iter(self.indexer.scan(jl, x_t[0, jl]))
            # get the first point in simple index and add it to composite queue
            initial_item = next(scanners[jl])
            composite_queues[l].put(initial_item)
        # only visit k_visit points in simple indicies
        for _ in range(k_visit):
            for l in range(self.composite_indices):
                # build up candidate sets to k_retrieve
                if len(candidate_sets[l]) < k_retrieve:
                    if not composite_queues[l].empty():
                        # get the furthest candidate point in composite index
                        item = composite_queues[l].get()
                        # dist is the distance from candidate point to query point in a simple index
                        dist, data = item
                        # jl is the index to simple index, i is the offset for sample in self.indexer.xs
                        jl, i = data
                        try:
                            # get the next nearest point in jl simple index
                            composite_queues[l].put(next(scanners[jl]))
                        except StopIteration:
                            # there's no points left to visit
                            pass

                        if i in composite_observations[l]:
                            composite_observations[l][i] += 1
                        else:
                            composite_observations[l][i] = 1

                        if composite_observations[l][i] == self.simple_indices:
                            # i sample has been observed in all simple indices in composite index, add to candidate sets
                            candidate_sets[l].add(int(i))
        # get the union of candidates in all candidate ets
        candidate_idxs = np.array(list(set.union(*candidate_sets)))
        # candidate vectors
        candidates = self.indexer.xs[candidate_idxs]
        # distance from query vector to candidate vectors
        raw_distances = pairwise_distances(X=x, Y=candidates, metric='euclidean', n_jobs=1)[0]
        raw_distances_idxs = np.argsort(raw_distances)[:k]
        # return k nearest candidates
        return (raw_distances[raw_distances_idxs], candidate_idxs[raw_distances_idxs].astype(int))