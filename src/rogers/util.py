""" Utility functions and helpers
"""
from .logger import get_logger
from . import generated as d


import os
import importlib
import os.path as path

from terminaltables import SingleTable


log = get_logger(__name__)


def load_class(namespace):
    """ Python package namespace with class, e.g. module.Class
    :param namespace:
    :return:
    """
    module_name, class_name = namespace.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


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


def chunks(l, n):
    """ Split list l into n chunks
    :param l:
    :param n:
    :return:
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def sha256_key(sha256):
    """ Create key from sha256
    :param sha256: hashval
    :return: path to return
    :rtype: str
    """
    try:
        path = "%s%s/%s%s/%s%s/%s%s/%s" % (
            sha256[0], sha256[1], sha256[2], sha256[3], sha256[4],
            sha256[5], sha256[6], sha256[7], sha256)
    except IndexError:
        return sha256
    return path
