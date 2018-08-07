""" Static PE vectorizer
"""
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction import DictVectorizer


class BaseVectorizer(BaseEstimator, TransformerMixin):
    """ Base class
    """

    def __init__(self, vectorizer=None, extractor=None):
        """
        :param vectorizer:
        :param extractor:
        """
        self._vectorizer = vectorizer or DictVectorizer(sparse=True)
        self._extractor = extractor

    def explode(self, s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        if self._extractor:
            return self._extractor(s)
        raise NotImplementedError

    def transform(self, samples):
        """ Fit the
        :param samples:
        :return:
        """
        return self._vectorizer.transform(map(self.explode, samples))

    def fit(self, samples, ys=None):
        """ Fit the
        :param samples:
        :param ys:
        :return:
        """
        self._vectorizer.fit(map(self.explode, samples))
        return self
