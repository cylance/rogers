""" Context-Triggered Piecewise Hashing Index using Ssdeep

Implementation based on https://www.virusbulletin.com/virusbulletin/2015/11/optimizing-ssdeep-use-scale/
"""
import os
import ssdeep
import base64
from struct import unpack

from . import Index as BaseIndex

from rogers.store import Database
from rogers.logger import get_logger
import rogers.config as c

log = get_logger(__name__)


SQL_INIT = """
BEGIN;

CREATE TABLE sample (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sha256 VARCHAR UNIQUE
);

CREATE TABLE ssdeep (
  id INTEGER PRIMARY KEY,
  hash VARCHAR,
  FOREIGN KEY(id) REFERENCES sample(id)
);

CREATE TABLE ssdeep_chunks (
  id INTEGER,
  chunk_size INTEGER,
  chunk INTEGER,
  FOREIGN KEY(id) REFERENCES sample(id)
);

CREATE INDEX chunk_size_idx ON ssdeep_chunks(chunk_size);
CREATE INDEX chunk_idx ON ssdeep_chunks(chunk);

COMMIT;
"""


class Index(BaseIndex, Database):
    """ CTPH Index
    """

    name = 'ctph'

    def __init__(self):
        """ Setup ctph sqlite database connection
        """
        self.index_path = os.path.join(c.settings.get('INDEX_DIR'), 'ctph.index')
        self._db = None
        self.connect()

    def initialize(self):
        """ Create index db
        :return:
        """
        with self.cursor() as cur:
            cur.executescript(SQL_INIT)

    @staticmethod
    def get_all_7_char_chunks(h):
        """ Get 7 char chunks from ssdeep digest
        :param h:
        :return:
        """
        return list(set((unpack("<Q", base64.b64decode(h[i:i+7] + "=") + b'\x00\x00\x00')[0] for i in range(len(h) - 6))))

    @classmethod
    def preprocess_hash(cls, h):
        """ Extract ssdeep ngram chunks
        :param h:
        :return:
        """
        block_size, h = h.split(":", 1)
        block_size = int(block_size)

        for c in set(list(h)):
            while c * 4 in h:
                h = h.replace(c * 4, c * 3)

        block_data, double_block_data = h.split(":")

        return block_size, cls.get_all_7_char_chunks(block_data), cls.get_all_7_char_chunks(double_block_data)

    @classmethod
    def transform(cls, sample):
        """ Calculate sdeep for sample
        :param sample:
        :return:
        """
        h = ssdeep.Hash()
        for chunk in sample.chunks():
            h.update(chunk)
        sample.add('ssdeep.digest', h.digest())
        block_size, block_chunks, double_block_chunks = cls.preprocess_hash(sample.get('ssdeep.digest'))
        sample.add('ssdeep.block_size', block_size)
        sample.add('ssdeep.block_chunks', block_chunks)
        sample.add('ssdeep.double_block_chunks', double_block_chunks)

    def partial_fit(self, xs):
        """ Update index
        :param xs: List of Sample
        :return:
        """
        for s in xs:
            self.add(s)

    def fit(self, samples, **kwargs):
        """ Fit the index
        :param samples:
        :param kwargs:
        :return:
        """
        self.reset()
        self.partial_fit(samples)

    @staticmethod
    def compare(a, b):
        """ Calculate ssdeep similarity between two digests
        :param a: ssdeep digest
        :param b: ssdeep digest
        :return:
        """
        return ssdeep.compare(a, b)

    def _query(self, sample, k=10, **kwargs):
        """ Query the k nearest neighbors by ssdeep similarity
        :param sample:
        :param k:
        :param kwargs:
        :return:
        """
        digests = {}
        neighbors = []

        block_size = sample.get('ssdeep.block_size')
        block_chunks = sample.get('ssdeep.block_chunks')
        double_block_chunks = sample.get('ssdeep.double_block_chunks')
        digest = sample.get('ssdeep.digest')

        sample_ids = self._lookup_chunks(block_size, block_chunks)
        sample_ids_double = self._lookup_chunks(block_size, double_block_chunks)

        sample_ids = sample_ids.intersection(sample_ids_double)

        if len(sample_ids):
            clause = "ss.id = " + " OR ss.id = ".join(map(str, sample_ids))
            with self.cursor() as cursor2:
                ret = cursor2.execute("SELECT s.sha256, ss.hash FROM ssdeep AS ss "
                                      "LEFT JOIN sample AS s ON ss.id = s.id WHERE %s" % clause).fetchall()
                if ret is not None:
                    digests = {r[0]: r[1] for r in ret if r[0] and r[0] != sample.sha256}

        for d in digests.items():
            score = self.compare(digest, d[1])
            neighbors.append({'hashval': d[0], 'similarity': score})

        return neighbors

    def add(self, sample):
        """ Add sample to index
        :param sample: Sample
        :return:
        """
        block_size = sample.get('ssdeep.block_size')
        block_chunks = sample.get('ssdeep.block_chunks')
        double_block_chunks = sample.get('ssdeep.double_block_chunks')
        digest = sample.get('ssdeep.digest')

        sample_id = self.lookup_or_insert_sample(sample.sha256)
        self._insert_ssdeep(sample_id, digest)
        self._insert_chunks(sample_id, block_size, list(block_chunks) + list(double_block_chunks))

    def load(self):
        """ Load index from local storage
        :return:
        """
        pass

    def save(self):
        """ Save index to local storage
        :return:
        """
        pass

    def _insert_ssdeep(self, sample_id, ssdeep_digest):
        """ Insert ssdeep digest for sample
        :param sample_id:
        :param ssdeep_digest:
        :return:
        """
        with self.cursor() as cursor:
            cursor.execute("INSERT INTO ssdeep (id, hash) VALUES (?, ?);", (sample_id, ssdeep_digest,))

    def _insert_chunks(self, sample_id, chunk_size, chunks):
        """ Insert chunks into db
        :param sample_id:
        :param chunk_size:
        :param chunks:
        :return:
        """
        for chunk in chunks:
            self._insert_chunk(sample_id, chunk_size, chunk)

    def _insert_chunk(self, sample_id, chunk_size, chunk):
        """ Insert chunk_size and chunk for ssdeep digest
        :param sample_id: pk of sample
        :param chunk_size:
        :param chunk:
        :return:
        """
        with self.cursor() as cursor:
            cursor.execute("INSERT INTO ssdeep_chunks (id, chunk_size, chunk) VALUES (?, ?, ?);", (sample_id, chunk_size, chunk,))

    def _lookup_chunks(self, chunk_size, chunks):
        """ Lookup matching ssdeep digest by chunk size and chunks
        :param chunk_size:
        :param chunks:
        :return:
        """
        if len(chunks):
            clause = "chunk = " + " OR chunk = ".join(map(str, chunks))
            with self.cursor() as cursor:
                stmt = "SELECT sc.id FROM ssdeep_chunks AS sc " \
                                     "WHERE chunk_size = ? AND %s" % clause
                ret = cursor.execute(stmt, (chunk_size,)).fetchall()
                if ret is not None and len(ret):
                    return set(map(str, ret[0]))
        return set()
