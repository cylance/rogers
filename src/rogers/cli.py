""" Command line entry points
"""
import json
import os

import pandas as pd
import rogers.config as c
import rogers.data as d
import rogers.index as i
import rogers.logger as l
import rogers.sample.pe as pe
from rogers.index import ctph as ctph
import rogers.store as store
import rogers.util as u
import rogers.vectorizer as vectorizer
from google.protobuf import json_format

log = l.get_logger(__name__)


db = store.Database()


def preprocessor(sample_path):
    """ Calculate sha256 of sample
    :param sample_path:
    :return:
    """
    sample = pe.PE(sample_path)
    try:
        return sample_path, sample.sha256
    except Exception as e:
        log.exception("%s: %s", sample.local_path, e)


def pe_feature_extractor(sample_path):
    """ Extract PE feature data
    :param sample_path:
    :return:
    """
    sample = pe.PE(sample_path)
    ctph_idx = ctph.Index()
    try:

        # calculate static pe features
        sample.extract()
        # calculate ssdeep
        ctph_idx.transform(sample)
    except Exception as e:
        log.exception("%s: %s", sample.local_path, e)
    else:
        return sample.serialize()


def extract(args):
    """ Extract command
    :param args: argparse args
    :return:
    """
    dir_path = args.dir or c.settings.get('SAMPLE_DIR')

    n_samples = 0
    for _ in u.enumerate_dir(dir_path):
        n_samples += 1

    sample_paths = u.enumerate_dir(dir_path)
    db = store.Database()

    if args.input:
        filter_hashvals = set(pd.read_csv(args.input)['sha256'].tolist())
        log.info("Filtering out %s samples by hashval for extraction", len(filter_hashvals))
    else:
        filter_hashvals = None

    if not args.f or filter_hashvals is not None:
        log.info("Preprocessing %s samples", n_samples)
        sample_paths, n_samples = u.preprocess(db, preprocessor, sample_paths, filter_hashvals=filter_hashvals)

    def consumer(msg):
        if msg is not None:
            features = pe.PE.deserialize(msg)
            log.debug(features.sha256)
            db.insert_sample_features(features.sha256, msg)

    log.info("Extracting features for %s samples", n_samples)
    u.dispatch(pe_feature_extractor, sample_paths, consumer=consumer)

    log.info("Feature extraction complete")


def transform(args):
    """ Transform command
    :param args: argparse args
    :return:
    """
    samples = u.samples_from_args(db, args)
    pipeline = vectorizer.pe_pipeline()
    i.Index.fit_transform(pipeline, samples)


def fit(args):
    """ Fit command
    :param args:
    :return:
    """
    samples = u.samples_from_args(db, args)
    log.info("Fitting index")
    idx = i.init(args.index)
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


def query(args):
    """ Query command
    :param args: argparse args
    :return:
    """
    idx = i.init(args.index)
    idx.load()
    samples = u.samples_from_args(db, args)
    if len(samples):
        neighbors = []
        log.info("Querying %s samples", len(samples))
        for sample in samples:
            context = sample.contextual_features(prefix='query')
            results = idx.query(sample, k=args.k)
            neighbors = []
            for n in results['neighbors']:
                n_dict = {'query_hashval': sample.sha256, 'neighbor_hashval': n[0].sha256, 'similarity': n[1]}
                neighbor_context = n[0].contextual_features(prefix='neighbor')
                n_dict.update(context)
                n_dict.update(neighbor_context)
                neighbors.append(n_dict)
        if not args.print:
            u.print_table(neighbors, keys=['query_hashval', 'neighbor_hashval', 'similarity'])
        else:
            for n in neighbors:
                print("query: %s, neighbor: %s, similarity: %s" % (n['query_hashval'], n['neighbor_hashval'], n['similarity']))
        if args.export:
            df = pd.DataFrame.from_records(neighbors)
            df.to_csv(args.export, index=False)
            log.info("Exported %s matches to %s", len(neighbors), args.export)
    else:
        raise Exception("Need to process hash")


def feature_add(args):
    """ Add features for samples into database from input CSV
    :param args: argparse args
    :return:
    """
    df = pd.read_csv(args.input)
    var_type = d.Feature.Variable.Type.Value(args.type)
    var_mode = d.Feature.Modality.Type.Value(args.modality)
    for _, r in df.dropna().iterrows():
        s = db.load_sample(r['sha256'])
        if s is None:
            log.warning("%s: Not in sample db", r['sha256'])
            continue
        for k in r.keys():
            if k != 'sha256':
                s.add(k, r[k], var_type=var_type, var_mode=var_mode)
                log.debug("%s: Added feature %s - %s", r['sha256'], k, r[k])
        db.insert_sample_features(s.sha256, s.serialize())


def features_get(args):
    """ Query sample
    :param args: argparse args
    :return:
    """
    sample = db.load_sample(args.hashval)
    if sample is not None:
        u.print_sample_details(sample, args.print)
        if args.export:
            with open(os.path.join(args.export, "%s.json" % sample.sha256), 'w') as fout:
                json.dump(json_format.MessageToDict(sample.features), fout, indent=4)
    else:
        log.info("%s is not in sample db", args.hashval)


def db_info(args):
    """ Count samples in database
    :param args: argparse args
    :return:
    """
    log.info("database contains %s samples", db.n)


def db_initialize(args):
    """ Initialize database
    :param args:
    :return:
    """
    db.initialize()


def db_reset(args):
    """ Reset database
    :param args:
    :return:
    """
    db.reset()
