""" Utility functions and helpers
"""
import os
import os.path as path
import yara
from multiprocessing import cpu_count
from multiprocessing.pool import Pool

import pandas as pd
import rogers.data as d
from rogers.config import YARA_RULE_PATH
from rogers.logger import get_logger
from terminaltables import SingleTable


log = get_logger(__name__)


def print_sample_details(sample, use_print=False):
    """ Print sample to stdout
    :param sample:
    :return:
    """
    data = []
    for name in sample.features.map:
        feature = sample.features.map[name]
        v = d.value(sample.features.map[name].value)
        if isinstance(v, list):
            v = " ".join([str(v_i) for v_i in v])
        elif isinstance(v, dict):
            v = " ".join(["%s:%s" % (key, v[key]) for key in v.keys()])
        offset = len(str(v))
        if offset > 60:
            v = str(v[:60]) + "..."
        data.append({"field": name,
                     # "type": d.Feature.Variable.Type.Name(feature.type),
                     "modality": d.Feature.Modality.Type.Name(feature.mode),
                     "value": v})
    print("")
    print("Sample: %s" % sample.sha256)
    if use_print:
        for r in data:
            print("%s (%s): %s" % (r['field'], r['modality'], r['value']))
    else:
        print_table(data)


def print_table(l, sniff_headers=True, keys=None):
    """ Print table to stout
    :param l: list of dicts to print in table
    :param sniff_headers:)
    :param keys:
    :return:
    """
    if len(l):
        if keys is not None:
            tmp_l = []
            for n in l:
                d = {}
                for k in keys:
                    d[k] = n[k]
                tmp_l.append(d)
            l = tmp_l
        if sniff_headers:
            headers = l[0].keys()
            l = [headers] + [d.values() for d in l]
        table = SingleTable(l)
        print(table.table)


def default_consumer(msg):
    """ Log msg
    :param msg:
    :return:
    """
    log.debug(msg)


def enumerate_dir(dir_path):
    """ Iter over files in dir
    :param dir_path:
    :return:
    """
    for root, ds, fs in os.walk(dir_path):
        for f in fs:
            yield path.join(root, f)


def load_yara_signatures(path=YARA_RULE_PATH):
    """ Load Yara signatures
    :param path:
    :return:
    """
    rules = yara.compile(YARA_RULE_PATH)
    return rules


def to_ascii(s):
    """ Force string to ascii
    :param s:
    :return:
    """
    s = s.split(b'\x00', 1)[0]
    return s.decode('ascii', 'ignore').lower()


def loword(dword):
    """ Low order word
    :param dword:
    :return:
    """
    return dword & 0x0000ffff


def hiword(dword):
    """ High order word
    :param dword:
    :return:
    """
    return dword >> 16


def chunks(l, n):
    """ Split list l into n chunks
    :param l:
    :param n:
    :return:
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def ipmap(fcn, args, pool_size=cpu_count() - 1):
    """ Iter pmap
    :param args:
    :param pool_size:
    :return:
    """
    with Pool(processes=pool_size) as pool:
        for i in pool.imap_unordered(fcn, args):
            yield i


def dispatch(fcn, args, consumer=default_consumer, pool_size=cpu_count() - 1):
    """ Distpatch a function over process pool and pass results to consumer
    :param fcn:
    :param args:
    :param consumer:
    :param pool_size:
    :return:
    """
    for i in ipmap(fcn, args, pool_size):
        consumer(i)


def preprocess(db, preprocessor, sample_paths, filter_hashvals=None):
    """ Process sample paths
    :param db:
    :param preprocessor:
    :param sample_paths:
    :param filter_hashvals:
    :return:
    """

    sample_hashval_and_filepath = {}

    def consumer(msg):
        if msg is not None:
            sample_path, hashval = msg
            sample_hashval_and_filepath[hashval] = sample_path

    dispatch(preprocessor, sample_paths, consumer=consumer)
    log.info("Identified %s unique samples", len(sample_hashval_and_filepath))

    if filter_hashvals is not None:
        sample_paths = [sample_path for hashval, sample_path in sample_hashval_and_filepath.items() if hashval in filter_hashvals]
    else:
        sample_paths = [sample_path for hashval, sample_path in sample_hashval_and_filepath.items() if not db.sample_features_exists(hashval)]
    return sample_paths, len(sample_paths)


def samples_from_args(db, args):
    """ Get sample or samples from args
    :param args:
    :return: list of Samples
    """
    if args.input:
        hashvals = set(pd.read_csv(args.input)['sha256'].tolist())
        log.info("Loading samples for %s input hashes for index command", len(hashvals))
        return [s for s in map(db.load_sample, hashvals) if s is not None]
    elif hasattr(args, 'hashval') and args.hashval:
        log.info("Loading %s hashvals" % len(args.hashval))
        return [s for s in map(db.load_sample, args.hashval) if s is not None]
    else:
        log.info("Loading %s samples from db", db.n)
        return [s for s in db.get_samples()]