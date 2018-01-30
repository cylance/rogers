""" TF-IDF on signatures
"""

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction.text import TfidfVectorizer


class SignatureVectorizer(BaseEstimator, TransformerMixin):

    def __init__(self):
        self.vectorizer = TfidfVectorizer(sublinear_tf=True)

    @staticmethod
    def explode(s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        sigs = s.get('static.signatures')
        return " ".join(sigs) if sigs is not None else ''

    def transform(self, samples):
        """ Fit the
        :param samples:
        :return:
        """
        return self.vectorizer.transform(map(self.explode, samples))

    def fit(self, samples, ys=None):
        """ Fit the
        :param samples:
        :param ys:
        :return:
        """
        self.vectorizer.fit(map(self.explode, samples))
        return self
