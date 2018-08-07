""" Command line entry points
"""
import numpy as np
import json
import os
from multiprocessing import Pool

import pandas as pd
from google.protobuf import json_format
from shutil import copyfile
from sklearn.externals import joblib

from . import store
from . import config as c
from . import generated as d
from . import util
from . import vectorizer
from .index import index, Index
from .sample import pe
from .logger import get_logger

log = get_logger(__name__)

POOL = Pool()


def extract(dir_path=None, filter_hashvals=None, force=False, sample_class=pe.PE, db=None):
    """ Extract command
    :param dir_path:
    :param filter_hashvals:
    :param force:
    :param sample_class:
    :param db:
    :return:
    """
    db = db or store.Database()
    sample_dir_path = c.settings.get('SAMPLE_DIR')
    n_samples = 0
    if dir_path is not None:
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
        log.info("Identified %s unique samples to copy into sample dir", len(sample_hashval_and_filepath))
        for hashval, sample_path in sample_hashval_and_filepath.items():
            new_sample_path = os.path.join(sample_dir_path, util.sha256_key(hashval))
            try:
                copyfile(sample_path, new_sample_path)
            except Exception as e:
                log.error("%s: %s - %s", hashval, sample_path, e)

    sample_hashval_and_filepath = {}
    for path in util.enumerate_dir(sample_dir_path):
        sample_hashval_and_filepath[os.path.basename(path)] = path

    if not force or filter_hashvals is not None:
        if filter_hashvals is not None:
            sample_paths = [sample_path for hashval, sample_path in sample_hashval_and_filepath.items() if
                            hashval in filter_hashvals]
        else:
            sample_paths = [sample_path for hashval, sample_path in sample_hashval_and_filepath.items() if
                            not db.sample_features_exists(hashval)]
    else:
        sample_paths = sample_hashval_and_filepath.values()

    n_samples = len(sample_paths)
    log.info("Extracting features for %s samples", n_samples)
    for ret in POOL.imap(sample_class.process, sample_paths):
        if ret is not None:
            hashval, msg = ret
            log.debug(hashval)
            db.insert_sample_features(hashval, msg)
    log.info("Feature extraction complete")


def pipeline_fit(samples):
    """ Fit pipeline
    :param samples:
    :return:
    """
    pipeline = vectorizer.offline_pe_pipeline()
    pipeline.fit(samples)
    xs = pipeline.fit_transform(samples)
    ys = np.array([s.sha256 for s in samples])
    log.info("Fit and transformed samples to %s", xs.shape)

    log.info("Exporting pipeline files")
    joblib.dump(xs, c.index_path('xs.pkl'))
    joblib.dump(ys, c.index_path('ys.pkl'))
    joblib.dump(pipeline, c.index_path('pipeline.pkl'))


def fit(index_name, samples):
    """ Fit command
    :param index_name:
    :param samples:
    :return:
    """
    log.info("Fitting index")
    idx = index(index_name)
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


def query(index_name, samples, k=5, console_print=False, export=None):
    """ Query command
    :param index_name:
    :param samples:
    :param k:
    :param console_print:
    :param export:
    :return:
    """
    idx = index(index_name)
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


def feature_add(df, var_type, var_modality, db=None):
    """ Add features for samples into database from input CSV
    :param df: Dataframe
    :param var_type:
    :param var_modality:
    :param db:
    :return:
    """
    db = db or store.Database()
    var_type = d.Feature.Variable.Type.Value(var_type)
    var_mode = d.Feature.Modality.Type.Value(var_modality)
    for _, r in df.dropna().iterrows():
        s = db.load_sample(r['sha256'])
        if s is None:
            # log.warning("%s: Not in sample db", r['sha256'])
            continue
        for k in r.keys():
            if k != 'sha256':
                s.add(k, r[k], var_type=var_type, var_mode=var_mode)
                # log.debug("%s: Added feature %s - %s", r['sha256'], k, r[k])
        db.insert_sample_features(s.sha256, s.serialize())


def features_get(hashval, console_print=False, export=None, db=None):
    """ Query sample
    :param hashval:
    :param console_print:
    :param export:
    :param sample_class:
    :return:
    """
    db = db or store.Database()
    sample = db.load_sample(hashval)
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
    log.info("database contains %s samples", store.Database().n)


def db_initialize():
    """ Initialize database
    :return:
    """
    store.Database().initialize()


def db_reset():
    """ Reset database
    :return:
    """
    store.Database().reset()
