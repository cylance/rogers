""" Vectorizer pipelines
"""
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.preprocessing import Normalizer
from sklearn.decomposition import TruncatedSVD

from rogers.vectorizer.PEVectorizer import HeaderVectorizer, SymImportsVectorizer, SymExportsVectorizer
from rogers.vectorizer.SignatureVectorizer import SignatureVectorizer

from rogers.logger import get_logger

log = get_logger(__name__)


def pe_pipeline():
    """ Offline static pe pipeline that generates dense vectors, uses ideas from Latent Semantic Index
    by representing sample features as a document with a vocabulary of terms

    TODO: This can totally be improved with parameter optimization and/or better feature representationswith a vocabulary of terms

    :return:
    """
    return Pipeline([
        ('vectorize', FeatureUnion(
            transformer_list=[
                ('signatures', Pipeline([
                    ('vectorizer', SignatureVectorizer()),
                ])),
                ('header', Pipeline([
                    ('vectorizer', HeaderVectorizer()),
                    ('normalize', Normalizer())
                    # ('projection', TruncatedSVD(n_components=100)),
                ])),
                ('sym_imports', Pipeline([
                    ('vectorizer', SymImportsVectorizer()),
                    ('projection', TruncatedSVD(n_components=256)),
                ])),
                ('sym_exports', Pipeline([
                    ('vectorizer', SymExportsVectorizer()),
                ])),
            ]
        )),
        ('projection', TruncatedSVD(n_components=512)),
    ])
