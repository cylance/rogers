""" Hierarchical Navigable Small World (HNSW) index

Based on http://arxiv.org/abs/1603.09320

"""
import nmslib
from sklearn.externals import joblib

from . import Index as BaseIndex
from rogers.logger import get_logger
import rogers.config as c

log = get_logger(__name__)


class Index(BaseIndex):
    """ HNSW Index
    """

    name = 'hnsw'

    def fit(self, samples, **kwargs):
        """ Fit HNSW index
        :param samples: list of Samples
        :param kwargs: optional index parameters
        :return:
        """
        xs, self.ys = self.transform(samples)
        self.index = nmslib.init(method='hnsw', space='cosinesimil')
        self.index.addDataPointBatch(xs)
        self.index.createIndex({'post': kwargs.get('post', 0),
                                'efConstruction': kwargs.get('efConstruction', 400),
                                'M': kwargs.get('M', 12)})

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
