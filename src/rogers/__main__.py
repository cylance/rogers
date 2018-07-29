""" Entry points for Rogers malware similarity tool
"""
from . import config
from .resource import DB
from .index import list_available_index
from .generated import Feature
from .sample import pe
from . import logger as l
from . import api

import argparse
import pandas as pd


log = l.get_logger('rogers')


INDEX_OPTIONS = list_available_index()


def _samples_from_args(args, sample_class):
    """ Get sample or samples from args
    :param args:
    :param sample_class:
    :return: list of Samples
    """
    if args.input:
        hashvals = set(pd.read_csv(args.input)['sha256'].tolist())
        log.info("Loading samples for %s input hashes for index command", len(hashvals))
        return [s for s in map(lambda x: DB.load_sample(x, sample_class), hashvals) if s is not None]
    elif hasattr(args, 'hashval') and args.hashval:
        log.info("Loading %s hashvals" % len(args.hashval))
        return [s for s in map(lambda x: DB.load_sample(x, sample_class), args.hashval) if s is not None]
    else:
        log.info("Loading %s samples from db", DB.n)
        return [s for s in DB.get_samples(sample_class)]


def features_get(args):
    api.features_get(args.hashval, console_print=args.print, export=args.export)


def feature_add(args):
    api.feature_add(args.input, args.type, args.modaility)


def db_initialize(args):
    api.db_initialize()


def db_info(args):
    api.db_info()


def db_reset(args):
    api.db_reset()


def extract(args):
    if args.input:
        filter_hashvals = set(pd.read_csv(args.input)['sha256'].tolist())
        log.info("Filtering out %s samples by hashval for extraction", len(filter_hashvals))
    else:
        filter_hashvals = None
    api.extract(args.dir, filter_hashvals=filter_hashvals, force=args.f)


def transform(args):
    samples = _samples_from_args(args, pe.PE)
    api.transform(samples)


def fit(args):
    samples = _samples_from_args(args, pe.PE)
    api.fit(args.index, samples)


def query(args):
    samples = _samples_from_args(args, pe.PE)
    api.query(args.index, samples, k=args.k, export=args.export, console_print=args.print)


def main():
    parser = argparse.ArgumentParser(prog='rogers', description='Malware similarity tool')
    subparsers = parser.add_subparsers(help='commands')
    parser.set_defaults(func=None)
    parser.add_argument('-v', action='store_true', help='Set debug logging')
    parser.add_argument('--print', action='store_true', help='Print results instead of table output')
    parser.add_argument('--conf', default=None, help='Path to rogers configuration file')

    # features command
    features_parser = subparsers.add_parser('feature')
    features_subparsers = features_parser.add_subparsers(help='Feature commands')

    # features get
    features_get_parser = features_subparsers.add_parser('get')
    features_get_parser.add_argument('hashval', type=str, help='sha256 hashval')
    features_get_parser.add_argument('--export', default=config.CWD, type=str, help='Dir to export sample feature as JSON')
    features_get_parser.set_defaults(func=features_get)

    # features add
    feature_add_parser = features_subparsers.add_parser('add')
    feature_add_parser.set_defaults(func=feature_add)
    feature_add_parser.add_argument('input', default=None, type=str, help='Path to CSV containing hashval column and feature columns')
    feature_add_parser.add_argument('--type', default='CATEGORICAL', choices=Feature.Variable.Type.keys(), type=str, help='Type of feature variable')
    feature_add_parser.add_argument('--modality', default='CONTEXTUAL', choices=Feature.Modality.Type.keys(), type=str, help='Type of feature modality')

    # db command
    db_parser = subparsers.add_parser('db')
    db_subparsers = db_parser.add_subparsers(help='Sample database admin commands')

    # db init
    init_parser = db_subparsers.add_parser('init')
    init_parser.set_defaults(func=db_initialize)

    # db list
    db_info_parser = db_subparsers.add_parser('info')
    db_info_parser.set_defaults(func=db_info)

    # db reset
    reset_parser = db_subparsers.add_parser('reset')
    reset_parser.set_defaults(func=db_reset)

    # index commands
    index_parser = subparsers.add_parser('index')
    index_parser.add_argument('--dir', default=None, type=str, help='Optional directory of samples, defaults to sample dir')
    index_parser.add_argument('--input', default=None, type=str, help='Optional path to CSV containing hashval column for index command')
    index_subparsers = index_parser.add_subparsers(help='index commands')

    # extract
    extract_parser = index_subparsers.add_parser('extract')
    extract_parser.add_argument('-f', action='store_true', help='Force extraction')
    extract_parser.set_defaults(func=extract)

    # transform
    transform_parser = index_subparsers.add_parser('transform')
    transform_parser.set_defaults(func=transform)

    # fit
    fit_parser = index_subparsers.add_parser('fit')
    fit_parser.add_argument('index', choices=INDEX_OPTIONS, type=str, help='Name of index type')
    fit_parser.set_defaults(func=fit)

    # partial_fit
    # TODO
    # partial_fit_parser = index_subparsers.add_parser('partial-fit')
    # partial_fit_parser.set_defaults(func=cli.partial_fit)

    # query
    query_parser = index_subparsers.add_parser('query')
    query_parser.add_argument('index', choices=INDEX_OPTIONS, type=str, help='Name of index type')
    query_parser.add_argument('hashval', default=None, nargs='*', type=str, help='sha256 hashval or path to CSV')
    query_parser.add_argument('--k', default=5, type=int, help='Number of nearest neighbors to return in query')
    query_parser.add_argument('--export', default="query_results.csv", type=str, help='Path to export query results as csv')
    query_parser.set_defaults(func=query)

    args = parser.parse_args()

    if args.v:
        log_level = l.logging.DEBUG
    else:
        log_level = l.logging.INFO

    l.init_logging(level=log_level)

    if args.conf is not None:
        config.configure(args.conf)

    if args.func is None:
        parser.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    main()
