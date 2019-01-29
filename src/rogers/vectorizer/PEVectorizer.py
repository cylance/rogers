""" Static PE vectorizer
"""
from .BaseVectorizer import BaseVectorizer


class HeaderVectorizer(BaseVectorizer):
    """ PE header features using spare DictVectorizer
    """

    def explode(self, s):
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


class SymImportsVectorizer(BaseVectorizer):
    """ Imported symbols
    """

    def explode(self, s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        v = s.get("header.import_syms")
        x = " ".join(v) if v is not None else ''
        return x


class SymExportsVectorizer(BaseVectorizer):
    """ Exported symbols
    """

    def explode(self, s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        v = s.get("header.export_syms")
        x = " ".join(v) if v is not None else ''
        x = x if len(x) else ''
        return x


class SymImportsDictVectorizer(BaseVectorizer):
    """ Imported symbols
    """

    def explode(self, s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        syms = {}
        for sym in s.get("header.import_syms"):
            syms[sym] = 1
        return syms


class SymExportsDictVectorizer(BaseVectorizer):
    """ Exported symbols
    """

    def explode(self, s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        syms = {}
        for sym in s.get("header.export_syms"):
            syms[sym] = 1
        return syms
