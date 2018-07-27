"""
"""
from mock import patch
import rogers.vectorizer


def test_pe():
    import rogers.vectorizer.PEVectorizer as v

    a = v.PEVectorizer()

    b = v.HeaderVectorizer()

    c = v.SymExportsVectorizer()

    d = v.SymImportsVectorizer()


def test_signature():

    import rogers.vectorizer.SignatureVectorizer
