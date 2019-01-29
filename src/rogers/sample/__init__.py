""" Base class for file types
"""
from .. import generated as d
from ..logger import get_logger
from ..config import YARA_RULE_PATH
from ..util import load_class

import yara
import io
import gzip
import hashlib
import tempfile
from contextlib import contextmanager


log = get_logger(__name__)


# compiled yara signatures
_YARA = None


def yara_rules():
    global _YARA
    if _YARA is None:
        _YARA = yara.compile(YARA_RULE_PATH)
    return _YARA


class Unsupported(Exception):
    pass


class Sample(object):

    def __init__(self, local_path=None, features=None):
        """ Setup sample class with path to sample and optional feature data
        :param local_path:
        :param features:
        """
        self.local_path = local_path
        self._sha256 = None
        if features is None:
            self.features = d.Features()
            self.features.namespace = "%s.%s" % (self.__module__, self.__class__.__qualname__)
        else:
            self._sha256 = features.sha256
            self.features = features

    def _yara(self, path):
        """ Match Yara signatures
        :param path:
        :return:
        """
        yara_matches = []
        try:
            yara_matches = yara_rules().match(path, timeout=30)
        except (yara.Error, yara.TimeoutError):
            pass
        return [str(m) for m in yara_matches]

    def get(self, name):
        """ Get feature value
        :param name:
        :return:
        """
        return d.value(self.features.map[name].value)

    def add(self, name, val, var_type=d.Feature.Variable.CATEGORICAL, var_mode=d.Feature.Modality.STATIC):
        """ Add feature
        :param name:
        :param val:
        :param var_type:
        :param var_mode:
        :return:
        """
        self.features.map[name].CopyFrom(d.feature(val, var_type, var_mode))

    @classmethod
    def deserialize(cls, msg):
        """ Parse gzipped bytes to PB Features message
        :param msg:
        :return:
        """
        in_ = io.BytesIO()
        in_.write(msg)
        in_.seek(0)
        with gzip.GzipFile(fileobj=in_, mode='rb') as fo:
            features = d.Features()
            features.ParseFromString(fo.read())
        sample_class = load_class(features.namespace)
        return sample_class(features=features)

    def serialize(self):
        """ Serialize features message to bytes
        :return:
        """
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode='w') as fo:
            fo.write(self.features.SerializeToString())
        return out.getvalue()

    def contextual_features(self, prefix=None):
        """ Get contextual features and yara signatures
        :param prefix:  prefix to use on key
        :return:
        """
        features = {}
        if self.features is not None:
            for k in self.features.map:
                f = self.features.map[k]
                if f.mode == d.Feature.Modality.CONTEXTUAL or k == 'static.signatures':
                    if prefix is not None:
                        k = "%s.%s" % (prefix, k)
                    features[k] = d.value(f.value)
        return features

    @property
    def log(self):
        """ Get logging handler
        :return:
        """
        return get_logger('rogers.sample')

    @contextmanager
    def tempfile(self):
        """ Write sample to temp file
        :return:
        """
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            tmp.writelines(self.chunks())
            yield tmp.name

    @contextmanager
    def handle(self):
        """ File handle to sample
        :return:
        """
        with gzip.open(self.local_path, 'rb') as fin:
            yield fin

    def chunks(self, chunk_size=8192):
        """ Yield chunks for file by chunk_size
        :param chunk_size:  block/chunk size of handle
        :return:
        """
        with self.handle() as fin:
            while True:
                chunk = fin.read(chunk_size)
                if chunk:
                    yield chunk
                else:
                    break

    @property
    def sha256(self):
        """ Return or compute sha256 hashval
        :return:
        """
        if self._sha256 is None:
            h = hashlib.sha256()
            for chunk in self.chunks():
                h.update(chunk)
            self._sha256 = h.hexdigest().upper()
        return self._sha256

    @staticmethod
    def process(sample_path):
        """ Extract feature data
        :param sample_path:
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def preprocessor(sample_path):
        """ Calculate sha256 of sample
        :param sample_path:
        :return:
        """
        raise NotImplementedError

    def extract(self):
        """ Perform raw feature extraction
        :return:
        """
        raise NotImplementedError
