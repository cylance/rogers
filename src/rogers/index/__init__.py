""" Base class for different nearest neighbor search methods
"""
from .. import config as c
from .. import store
from ..sample import Sample
from ..logger import get_logger

import os
import numpy as np
from sklearn.externals import joblib
from importlib import import_module
from operator import itemgetter


log = get_logger(__name__)


def index(name, *args, **kwargs):
    """ Dynamically load a model class and instantiate
    :param name:
    :param sample_class:
    :return:
    """
    try:
        index_mod_name = "rogers.index.%s" % name
        mod = import_module("rogers.index.%s" % name)
        idx = mod.Index(*args, **kwargs)
    except ModuleNotFoundError:
        log.fatal("%s index is not available", name)
        raise SystemExit(-1)
    else:
        log.debug("Loaded %s index", index_mod_name)
        return idx


class Index(object):
    """ Base class for NN index
    """

    name = 'base'

    def __init__(self, db=None, pipeline=None, **parameters):
        """ Base attributes
        """
        self.db = db or store.Database()
        self.index = None
        self.ys = None
        self.parameters = parameters
        try:
            self.pipeline = pipeline or joblib.load(c.index_path('pipeline.pkl'))
        except FileNotFoundError:
            log.info("pipeline not available or fit")
            self.pipeline = None

    @property
    def index_file_prefix(self):
        """ Wrapper for file path based on index name
        :return:
        """
        return c.index_path(self.name)

    @staticmethod
    def list_available_index():
        """ Dynamically identify available index modules in index/
        :return: name of module file without ext
        """
        l = []
        for name in os.listdir(os.path.join(c.MODULE_DIR, "index")):
            if name.endswith(".py") and name != '__init__.py':
                l.append(name.replace('.py', ''))
        return l

    def transform(self, samples):
        """ Transform samples to xs, ys
        :param samples:
        :return:  xs, ys
        """
        ys = np.array([s.sha256 for s in samples])
        xs = self.pipeline.transform(samples)
        return xs, ys

    def load(self):
        """ Load index from local storage
        :return:
        """
        self.index = joblib.load("%s.index" % self.index_file_prefix)
        self.ys = joblib.load("%s.ys" % self.index_file_prefix)

    def save(self):
        """ Save index to local storage
        :return:
        """
        joblib.dump(self.index, "%s.index" % self.index_file_prefix)
        joblib.dump(self.ys, "%s.ys" % self.index_file_prefix)

    def fit(self, samples, ys=None):
        """ Fit samples into index
        :param samples: List of Sample instances or Numpy array / sparse matrix
        :param ys: List of index sha256 values that maps to
        :return:
        """
        if isinstance(samples, list) and isinstance(samples[0], Sample):
            xs, self.ys = self.transform(samples)
        else:
            xs = samples
            if ys is not None:
                self.ys = ys
        return self._fit(xs)

    def _fit(self, xs):
        """ Fit samples into index
        :param xs: Numpy array or sparse matrix
        :return:
        """
        raise NotImplementedError

    def partial_fit(self, samples):
        """ Insert samples into existing index
        :param samples: List of Sample
        :return:
        """
        raise NotImplementedError

    def query(self, sample, k=10, **kwargs):
        """ Wrap query index and return
        :param sample: Sample
        :param k: number of nearest neighbors to return from index
        :param kwargs: additional kwargs to pass to index query
        :return: dict of {'query': sample, 'neighbors': [(sample, similarity), ...]}
        """
        result = {'query': sample, 'neighbors': []}
        neighbors = self._query(sample, k, **kwargs)
        neighbors = self._nearest_k(sample, neighbors, k)
        return self._load_neighbor_samples(result, neighbors)

    def _query(self, sample, k=10, **kwargs):
        """ Query index and return
        :param sample: Sample
        :param k: number of nearest neighbors to return from index
        :param kwargs: additional kwargs to pass to index query
        :return:
        """
        raise NotImplementedError

    def _load_neighbor_samples(self, result, neighbors):
        """ Load neighbor samples for result
        :param result:
        :param neighbors:
        :return:
        """
        for nbr in neighbors:
            sample = self.db.load_sample(nbr['hashval'])
            result['neighbors'].append((sample, nbr['similarity']))
        return result

    @staticmethod
    def _nearest_k(sample, neighbors, k):
        """ Return nearest k
        :param sample:
        :param neighbors:
        :param k:
        :return:
        """
        neighbors = list(filter(lambda nbr: nbr['hashval'] != sample.sha256, neighbors))
        neighbors = sorted(neighbors, key=itemgetter('similarity'), reverse=True)
        if len(neighbors) > k:
            neighbors = neighbors[:k]
        return neighbors

    def query_samples(self, seed_samples, k=5, include_neighbors=False, **kwargs):
        """ Query seed samples optionally including neighbor of neighbors
        :param seed_samples: list of Sample
        :param k: return k nearest neighbors
        :param include_neighbors: lookup resulting neighbors of seeds
        :param kwargs: additional kwargs to pass to index query
        :return: list of dict results, [{'neighbors': [(<rogers.sample.pe.PE, similarity), ...],
                                            'query': <rogers.sample.pe.PE
                                        }, ...]
        """
        query_hashvals = set([s.sha256 for s in seed_samples])

        tmp_multiple_results = list(map(lambda s: {'query': s,
                                                   'neighbors': self._nearest_k(s, self._query(s, k, **kwargs), k)},
                                        seed_samples))

        neighbor_hashvals = set([])

        for results in tmp_multiple_results:
            for nbr in results['neighbors']:
                if nbr['hashval'] not in query_hashvals:
                    neighbor_hashvals.add(nbr['hashval'])

        neighbor_samples = self.db.load_samples(list(neighbor_hashvals))

        if include_neighbors:
            nbr_multiple_results = list(map(lambda s: {'query': s,
                                                       'neighbors': self._nearest_k(s, self._query(s, k, **kwargs), k)},
                                        neighbor_samples))

            expanded_neighbor_hashvals = set([])

            for results in nbr_multiple_results:
                for nbr in results['neighbors']:
                    if nbr['hashval'] not in query_hashvals and nbr['hashval'] not in neighbor_hashvals:
                        expanded_neighbor_hashvals.add(nbr['hashval'])

            neighbor_samples += self.db.load_samples(list(expanded_neighbor_hashvals))

            tmp_multiple_results += nbr_multiple_results

        sample_map = {s.sha256: s for s in seed_samples + neighbor_samples}

        results = []
        for tmp_results in tmp_multiple_results:
            neighbors = [(sample_map[nbr['hashval']], nbr['similarity']) for nbr in tmp_results['neighbors']]
            results.append({'query': tmp_results['query'], 'neighbors': neighbors})

        return results
