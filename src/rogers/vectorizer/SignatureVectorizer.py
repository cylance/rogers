""" TF-IDF on signatures
"""
from .BaseVectorizer import BaseVectorizer


class SignatureVectorizer(BaseVectorizer):

    def explode(self, s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        sigs = s.get('static.signatures')
        return " ".join(sigs) if sigs is not None else ''


class SignatureDictVectorizer(BaseVectorizer):

    def explode(self, s):
        """ Preprocess sample for vectorizers
        :param s: Sample instance
        :return:
        """
        sigs = {}
        for s in s.get('static.signatures'):
            sigs[s] = 1
        return sigs
