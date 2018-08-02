""" Hierarchical Navigable Small World (HNSW) index

Based on http://arxiv.org/abs/1603.09320

"""
from . import Index as BaseIndex
from ..logger import get_logger
from .. import config as c

import nmslib
from sklearn.externals import joblib


log = get_logger(__name__)


class Index(BaseIndex):
    """ HNSW Index
    """

    name = 'hnsw'

    def fit(self, samples):
        """ Fit HNSW index
        :param samples: list of Samples
        :return:
        """
        xs, self.ys = self.transform(samples)
        self.index = nmslib.init(method='hnsw', space='cosinesimil')
        self.index.addDataPointBatch(xs)
        self.index.createIndex({'post': self.parameters.get('post', 0),
                                'efConstruction': self.parameters.get('efConstruction', 400),
                                'M': self.parameters.get('M', 12)})

    def _query(self, sample, k=1, **kwargs):
        """ Query samples
        :param sample: Sample
        :param k:
        :param kwargs: optional query parameters
        :return:
        """
        self.index.setQueryTimeParams({'ef': kwargs.get('ef', 200)})
        x, _ = self.transform([sample])
        idxs, distances = self.index.knnQuery(x, k=k)
        neighbors = []
        for idx, d in zip(idxs, distances):
            hashval = self.ys[idx]
            neighbors.append({'hashval': hashval, 'similarity': 1 - float(d)})
        return neighbors

    def load(self):
        """ Load index from local storage
        :return:
        """
        self.index = nmslib.init(method='hnsw', space='cosinesimil')
        self.index.loadIndex(c.index_path('hnsw.index'))
        self.ys = joblib.load("%s.ys" % self.index_file_prefix)

    def save(self):
        """ Save index to local storage
        :return:
        """
        self.index.saveIndex(c.index_path('hnsw.index'))
        joblib.dump(self.ys, "%s.ys" % self.index_file_prefix)
