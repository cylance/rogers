""" Command line entry points
"""
import json
import os
from multiprocessing import Pool

import pandas as pd
from google.protobuf import json_format

from . import config as c
from . import generated as d
from . import index
from . import util
from . import vectorizer
from .resource import DB
from .sample import pe
from .logger import get_logger

log = get_logger(__name__)

POOL = Pool()


def extract(dir_path, filter_hashvals=None, force=False, sample_class=pe.PE):
    """ Extract command
    :param dir_path:
    :param filter_hashvals:
    :param force:
    :param sample_class:
    :return:
    """
    dir_path = dir_path or c.settings.get('SAMPLE_DIR')

    n_samples = 0
    for _ in util.enumerate_dir(dir_path):
        n_samples += 1

    sample_paths = util.enumerate_dir(dir_path)

    log.info("Preprocessing %s samples", n_samples)
    sample_hashval_and_filepath = {}
    for ret in POOL.imap(sample_class.preprocessor, sample_paths):
        try:
            sample_hashval_and_filepath[ret[1]] = ret[0]
        except Exception as e:
            log.exception(e)
    log.info("Identified %s unique samples", len(sample_hashval_and_filepath))

    if not force or filter_hashvals is not None:
        if filter_hashvals is not None:
            sample_paths = [sample_path for hashval, sample_path in sample_hashval_and_filepath.items() if
                            hashval in filter_hashvals]
        else:
            sample_paths = [sample_path for hashval, sample_path in sample_hashval_and_filepath.items() if
                            not DB.sample_features_exists(hashval)]
    else:
        sample_paths = sample_hashval_and_filepath.values()
    n_samples = len(sample_paths)

    log.info("Extracting features for %s samples", n_samples)
    for ret in POOL.imap(sample_class.process, sample_paths):
        if ret is not None:
            hashval, msg = ret
            log.debug(hashval)
            DB.insert_sample_features(hashval, msg)
    log.info("Feature extraction complete")


def transform(samples):
    """ Transform command
    :param samples:
    :return:
    """
    pipeline = vectorizer.pe_pipeline()
    index.Index.fit_transform(pipeline, samples)


def fit(index_name, samples, sample_class=pe.PE):
    """ Fit command
    :param index_name:
    :param samples:
    :param sample_class:
    :return:
    """
    log.info("Fitting index")
    idx = index.init(index_name, sample_class)
    idx.fit(samples)
    log.info("Saving index")
    idx.save()


# def partial_fit(args):
#     """
#     :param args:
#     :return:
#     """
#     idx = i.init(args.index)
#     idx.load()
#     samples = u.samples_from_args(db, args)
#     idx.partial_fit(samples)
#     log.info("Saving index")
#     idx.save()


def query(index_name, samples, k=5, sample_class=pe.PE, console_print=False, export=None):
    """ Query command
    :param index_name:
    :param samples:
    :param k:
    :param sample_class:
    :param console_print:
    :param export:
    :return:
    """
    idx = index.init(index_name, sample_class)
    idx.load()
    if len(samples):
        neighbors = []
        log.info("Querying %s samples", len(samples))
        for sample in samples:
            context = sample.contextual_features(prefix='query')
            results = idx.query(sample, k=k)
            neighbors = []
            for n in results['neighbors']:
                n_dict = {'query_hashval': sample.sha256, 'neighbor_hashval': n[0].sha256, 'similarity': n[1]}
                neighbor_context = n[0].contextual_features(prefix='neighbor')
                n_dict.update(context)
                n_dict.update(neighbor_context)
                neighbors.append(n_dict)
        if not console_print:
            util.print_table(neighbors, keys=['query_hashval', 'neighbor_hashval', 'similarity'])
        else:
            for n in neighbors:
                print("query: %s, neighbor: %s, similarity: %s" % (n['query_hashval'], n['neighbor_hashval'], n['similarity']))
        if export is not None:
            df = pd.DataFrame.from_records(neighbors)
            df.to_csv(export, index=False)
            log.info("Exported %s matches to %s", len(neighbors), export)
    else:
        raise Exception("Need to process hash")


def feature_add(input_file, var_type, var_modality, sample_class=pe.PE):
    """ Add features for samples into database from input CSV
    :param input_file:
    :param var_type:
    :param var_modality:
    :param sample_class:
    :return:
    """
    df = pd.read_csv(input_file)
    var_type = d.Feature.Variable.Type.Value(var_type)
    var_mode = d.Feature.Modality.Type.Value(var_modality)
    for _, r in df.dropna().iterrows():
        s = DB.load_sample(r['sha256'], sample_class)
        if s is None:
            log.warning("%s: Not in sample db", r['sha256'])
            continue
        for k in r.keys():
            if k != 'sha256':
                s.add(k, r[k], var_type=var_type, var_mode=var_mode)
                log.debug("%s: Added feature %s - %s", r['sha256'], k, r[k])
        DB.insert_sample_features(s.sha256, s.serialize())


def features_get(hashval, console_print=False, export=None, sample_class=pe.PE):
    """ Query sample
    :param hashval:
    :param console_print:
    :param export:
    :param sample_class:
    :return:
    """
    sample = DB.load_sample(hashval, sample_class)
    if sample is not None:
        util.print_sample_details(sample, console_print)
        if export is not None:
            with open(os.path.join(export, "%s.json" % sample.sha256), 'w') as fout:
                json.dump(json_format.MessageToDict(sample.features), fout, indent=4)
    else:
        log.info("%s is not in sample db", hashval)


def db_info():
    """ Count samples in database
    :return:
    """
    log.info("database contains %s samples", DB.n)


def db_initialize():
    """ Initialize database
    :return:
    """
    DB.initialize()


def db_reset():
    """ Reset database
    :return:
    """
    DB.reset()
