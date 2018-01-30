""" Static PE vectorizer
"""

from sklearn.base import BaseEstimator, TransformerMixin

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction import DictVectorizer


class PEVectorizer(BaseEstimator, TransformerMixin):
    """ Base class
    """

    def __init__(self):
        self.v = DictVectorizer(sparse=True)

    @staticmethod
    def explode(s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        raise NotImplementedError

    def transform(self, samples):
        """ Fit the
        :param samples:
        :return:
        """
        return self.v.transform(map(self.explode, samples))

    def fit(self, samples, ys=None):
        """ Fit the
        :param samples:
        :param ys:
        :return:
        """
        self.v.fit(map(self.explode, samples))
        return self


class HeaderVectorizer(PEVectorizer):
    """ PE header features using spare DictVectorizer
    """

    def __init__(self):
        self.v = DictVectorizer(sparse=True)

    @staticmethod
    def explode(s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        x = {}
        for k in s.features.map:
            if k.startswith('header'):
                v = s.get(k)
                if not isinstance(v, list) and not isinstance(v, dict):
                    x[k] = v
                if isinstance(v, dict):
                    for k_i in v.keys():
                        x["%s.%s" % (k, k_i)] = v[k_i]
        return x


class SymImportsVectorizer(PEVectorizer):
    """ TF-IDF on imported symbols
    """

    def __init__(self):
        self.v = TfidfVectorizer(sublinear_tf=True, min_df=2, max_df=0.80)

    @staticmethod
    def explode(s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        v = s.get("header.import_syms")
        x = " ".join(v) if v is not None else ''
        return x


class SymExportsVectorizer(PEVectorizer):
    """ TF-IDF on exported symbols
    """

    def __init__(self):
        self.v = TfidfVectorizer(sublinear_tf=True, min_df=1, max_df=0.80)

    @staticmethod
    def explode(s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        v = s.get("header.export_syms")
        x = " ".join(v) if v is not None else ''
        x = x if len(x) else ''
        return x
