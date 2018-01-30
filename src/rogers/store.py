""" Local sqlite db
"""
import os
import sqlite3
from contextlib import contextmanager
import rogers.sample.pe as pe
from rogers.logger import get_logger
import rogers.config as c

log = get_logger(__name__)


SQL_INIT = """
BEGIN;

CREATE TABLE sample (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sha256 VARCHAR UNIQUE
);

CREATE TABLE sample_feature (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  features BLOB
);

COMMIT;
"""


class Database(object):

    def __init__(self, index_path=os.path.join(c.settings.get('INDEX_DIR'), 'metadata.db')):
        self.index_path = index_path
        self._db = None
        self.connect()

    def connect(self):
        self._db = sqlite3.connect(self.index_path)

    @property
    def n(self):
        """ Count of samples in db
        :return:
        """
        with self.cursor() as cursor:
            return cursor.execute('SELECT COUNT(id) FROM sample;').fetchone()[0]

    def initialize(self):
        """ Create tables
        :return:
        """
        with self.cursor() as cur:
            cur.executescript(SQL_INIT)

    def _lookup_sample(self, hashval):
        """ Lookup sample by hashval
        """
        with self.cursor() as cursor:
            ret = cursor.execute("SELECT id FROM sample WHERE sha256 = (?);", (hashval,)).fetchone()
            if ret is not None:
                return ret[0]
            else:
                return ret

    def _insert_sample(self, hashval):
        """ Insert sample hashval
        :param hashval:
        :return:
        """
        with self.cursor() as cursor:
            cursor.execute("INSERT INTO sample (sha256) VALUES ('%s');" % hashval)

    def get_samples(self):
        """ Iterate over all samples in DB
        :return:
        """
        with self.cursor() as cursor:
            ret = cursor.execute("SELECT features FROM sample_feature;")
            while True:
                batch = ret.fetchmany(100)
                if not batch:
                    break
                for feature_blob in batch:
                    yield pe.PE(None, features=pe.PE.deserialize(feature_blob[0]))

    def get_sample_feature_blob(self, hashval):
        """ Get feature bytes for sample by hashval
        :param hashval:
        :return:
        """
        sample_id = self._lookup_sample(hashval)
        with self.cursor() as cursor:
            ret = cursor.execute("SELECT features FROM sample_feature WHERE id = ?;", (sample_id,)).fetchone()
            if ret is not None:
                return ret[0]

    def load_samples(self, hashvals):
        """ Load samples for hashvals
        :param hashvals:
        :return: list of Sample
        """
        samples = []
        for h in hashvals:
            sample = self.load_sample(h)
            if sample is not None:
                samples.append(sample)
        return samples

    def load_sample(self, hashval):
        """ Load sample by hashval into PE
        :param hashval:
        :return:
        """
        if self.sample_features_exists(hashval):
            features = pe.PE.deserialize(self.get_sample_feature_blob(hashval))
            return pe.PE(None, features=features)

    def sample_features_exists(self, hashval):
        """ Check if sample features exist in database
        :param hashval:
        :return:
        """
        ret = self._lookup_sample(hashval)
        if ret is not None:
            with self.cursor() as cursor:
                ret = cursor.execute("SELECT id FROM sample WHERE sha256 = (?);", (hashval,)).fetchone()
                if ret is not None:
                    return True
        return False

    def insert_sample_features(self, hashval, features):
        """ Insert sample features into database
        :param hashval:
        :param features:
        :return:
        """
        sample_id = self.lookup_or_insert_sample(hashval)
        with self.cursor() as cursor:
            cursor.execute("INSERT OR REPLACE INTO sample_feature (id, features) VALUES (?, ?);", (sample_id, features,))

    def lookup_or_insert_sample(self, hashval):
        ret = self._lookup_sample(hashval)
        if ret is None:
            self._insert_sample(hashval)
            ret = self._lookup_sample(hashval)
        return ret

    def reset(self):
        """ Delete database and reset
        :return:
        """
        os.remove(self.index_path)
        self.connect()
        self.initialize()

    @contextmanager
    def cursor(self):
        """ DB cursor that commits on close
        :return:
        """
        cur = self._db.cursor()
        try:
            yield cur
        finally:
            self._db.commit()
            cur.close()

