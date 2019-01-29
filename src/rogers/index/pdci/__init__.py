""" PrioritizedDynamicContinuousIndex

Nearest neighbor method described by Li and Malik in https://arxiv.org/abs/1703.00440
"""
from .. import Index as BaseIndex
from ...logger import get_logger

from .PrioritizedDynamicContinuousIndex import PrioritizedDynamicContinuousIndex, SQLiteIndexer
from sklearn.externals import joblib

log = get_logger(__name__)


class Index(BaseIndex):
    """ PDCI
    """

    name = 'pdci'

    def _fit(self, xs):
        """ Fit PDCI index
        :param xs: list of Samples
        :return:
        """
        self.index = self._index()
        self.index.fit(xs)

    def _index(self):
        return PrioritizedDynamicContinuousIndex(SQLiteIndexer(),
                                                 simple_indices=self.parameters.get('simple_indices', 10),
                                                 composite_indices=self.parameters.get('composite_indices', 2))

    def _query(self, sample, k=1, **kwargs):
        """ Query samples
        :param sample: Sample
        :param k:
        :param kwargs: optional query parameters
        :return:
        """
        x, _ = self.transform([sample])
        distances, idxs = self.index.query(x, k=k, k_retrieve=kwargs.get("k_retrieve", None),
                                           k_visit=kwargs.get("k_visit", None))
        neighbors = []
        for idx, d in zip(idxs, distances):
            hashval = self.ys[idx]
            neighbors.append({'hashval': hashval, 'similarity': float(d)})
        return neighbors

    def load(self):
        """ Load index from local storage
        :return:
        """
        self.parameters = joblib.load("%s.params.pk" % self.index_file_prefix)
        self.index = self._index()
        self.index.ys = self.ys
        self.index.random_unit_vectors = joblib.load("%s.random.unit_vectors.pk" % self.index_file_prefix)
        self.index.indexer.xs = joblib.load("%s.xs" % self.index_file_prefix)

    def save(self):
        """ Save index to local storage
        :return:
        """
        joblib.dump(self.ys, "%s.ys" % self.index_file_prefix)
        joblib.dump(self.index.indexer.xs, "%s.xs" % self.index_file_prefix)
        joblib.dump(self.parameters, "%s.params.pk" % self.index_file_prefix)
        joblib.dump(self.index.random_unit_vectors, "%s.random.unit_vectors.pk" % self.index_file_prefix)