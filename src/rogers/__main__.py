""" Entry points for Rogers malware similarity tool
"""
import argparse

import rogers.index as i
import rogers.config as c
import rogers.data as d
import rogers.logger as l

import rogers.cli as cli

log = l.get_logger('rogers')


INDEX_OPTIONS = i.list_available_index()


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
    features_get_parser.add_argument('hashval', type=str, help='sha256 hashval or path to CSV')
    features_get_parser.add_argument('--export', default=c.CWD, type=str, help='Dir to export sample feature as JSON')
    features_get_parser.set_defaults(func=cli.features_get)

    # features add
    feature_add_parser = features_subparsers.add_parser('add')
    feature_add_parser.set_defaults(func=cli.feature_add)
    feature_add_parser.add_argument('input', default=None, type=str, help='Path to CSV containing hashval column and feature columns')
    feature_add_parser.add_argument('--type', default='CATEGORICAL', choices=d.Feature.Variable.Type.keys(), type=str, help='Type of feature variable')
    feature_add_parser.add_argument('--modality', default='CONTEXTUAL', choices=d.Feature.Modality.Type.keys(), type=str, help='Type of feature modality')

    # db command
    db_parser = subparsers.add_parser('db')
    db_subparsers = db_parser.add_subparsers(help='Sample database admin commands')

    # db init
    init_parser = db_subparsers.add_parser('init')
    init_parser.set_defaults(func=cli.db_initialize)

    # db list
    db_info_parser = db_subparsers.add_parser('info')
    db_info_parser.set_defaults(func=cli.db_info)

    # db reset
    reset_parser = db_subparsers.add_parser('reset')
    reset_parser.set_defaults(func=cli.db_reset)

    # index commands
    index_parser = subparsers.add_parser('index')
    index_parser.add_argument('--dir', default=None, type=str, help='Optional directory of samples, defaults to sample dir')
    index_parser.add_argument('--input', default=None, type=str, help='Optional path to CSV containing hashval column for index command')
    index_subparsers = index_parser.add_subparsers(help='index commands')

    # extract
    extract_parser = index_subparsers.add_parser('extract')
    extract_parser.add_argument('-f', action='store_true', help='Force extraction')
    extract_parser.set_defaults(func=cli.extract)

    # transform
    transform_parser = index_subparsers.add_parser('transform')
    transform_parser.set_defaults(func=cli.transform)

    # fit
    fit_parser = index_subparsers.add_parser('fit')
    fit_parser.add_argument('index', choices=INDEX_OPTIONS, type=str, help='Name of index type')
    fit_parser.set_defaults(func=cli.fit)

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
    query_parser.set_defaults(func=cli.query)

    args = parser.parse_args()

    if args.v:
        log_level = l.logging.DEBUG
    else:
        log_level = l.logging.INFO

    l.init_logging(level=log_level)

    if args.conf is not None:
        c.configure(args.conf)

    if args.func is None:
        parser.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    main()
