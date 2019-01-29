""" Ball Tree using scikit-learn
"""
from . import Index as BaseIndex
from ..logger import get_logger

from sklearn.neighbors import NearestNeighbors


log = get_logger(__name__)


class Index(BaseIndex):
    """ Ball Tree Index
    """

    name = 'nn'

    def _fit(self, xs):
        """ Fit index
        :param samples: list of Samples
        :return:
        """
        self.index = NearestNeighbors(algorithm=self.parameters.get("algorithm", "auto"))
        self.index.fit(xs)

    def _query(self, sample,  k=5, **kwargs):
        """ Query index
        :param sample: Sample
        :param k:
        :param kwargs:
        :return:
        """
        x, _, = self.transform([sample])
        distances, idxs = self.index.kneighbors(x, n_neighbors=k+1)
        neighbors = []
        for idx, d in zip(idxs[0], distances[0]):
            hashval = self.ys[idx]
            neighbors.append({'hashval': hashval, 'similarity': min(1 - float(d), 1)})
        return neighbors
