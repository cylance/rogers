""" LSH Forest using scikit-learn
"""
from sklearn.neighbors import LSHForest

from . import Index as BaseIndex

from rogers.logger import get_logger

log = get_logger(__name__)


class Index(BaseIndex):
    """ LSH Forest Index
    """

    name = 'lsh_forest'

    def fit(self, samples, **kwargs):
        """ Fit index
        :param samples: list of Samples
        :param kwargs: optional index parameters
        :return:
        """
        xs, self.ys = self.transform(samples)
        log.info("Transformed samples to (%s, %s)" % xs.shape)
        self.index = LSHForest(n_estimators=kwargs.get('n_estimators', 20))
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
            neighbors.append({'hashval': hashval, 'similarity': 1 - float(d)})
        return neighbors
