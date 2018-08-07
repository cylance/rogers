""" Vectorizer pipelines
"""
from .PEVectorizer import HeaderVectorizer, SymImportsVectorizer, SymExportsVectorizer, SymImportsDictVectorizer, SymExportsDictVectorizer
from .SignatureVectorizer import SignatureVectorizer, SignatureDictVectorizer
from ..logger import get_logger

from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.preprocessing import Normalizer
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import FeatureHasher


log = get_logger(__name__)


def offline_pe_pipeline():
    """ Offline static pe pipeline that generates dense vectors, uses ideas from Latent Semantic Index
    by representing sample features as a document with a vocabulary of terms

    TODO: This can totally be improved with parameter optimization and/or better feature representations

    :return:
    """
    return Pipeline([
        ('vectorize', FeatureUnion(
            transformer_list=[
                ('signatures', Pipeline([
                    ('vectorizer', SignatureVectorizer(TfidfVectorizer(sublinear_tf=True))),
                ])),
                ('header', Pipeline([
                    ('vectorizer', HeaderVectorizer()),
                    ('normalize', Normalizer())
                ])),
                ('sym_imports', Pipeline([
                    ('vectorizer', SymImportsVectorizer(TfidfVectorizer(sublinear_tf=True, min_df=2, max_df=0.90))),
                    ('projection', TruncatedSVD(n_components=256)),
                ])),
                ('sym_exports', Pipeline([
                    ('vectorizer', SymExportsVectorizer(TfidfVectorizer(sublinear_tf=True, min_df=1, max_df=0.90))),
                ])),
            ],
        )),
        ('projection', TruncatedSVD(n_components=512)),
    ])


def online_pe_pipeline():
    """ Online vectorizer with feature hashing
    :return:
    """
    return Pipeline([
        ('vectorize', FeatureUnion(
            transformer_list=[
                ('signatures', Pipeline([
                    ('vectorizer', SignatureDictVectorizer(vectorizer=FeatureHasher(1024))),
                ])),
                ('header', Pipeline([
                    ('vectorizer', HeaderVectorizer(FeatureHasher(2048))),
                ])),
                ('sym_imports', Pipeline([
                    ('vectorizer', SymImportsDictVectorizer(FeatureHasher(512))),
                ])),
                ('sym_exports', Pipeline([
                    ('vectorizer', SymExportsDictVectorizer(FeatureHasher(256))),
                ])),
            ],
        )),
    ])
