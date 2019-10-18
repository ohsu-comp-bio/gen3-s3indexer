"""Inventory a bucket and output indexs3client command lines."""

import sys
import logging
import argparse

import s3indexer.upload_bucket_indexer as upload_bucket
import s3indexer.external_bucket_indexer as external_buckets

logger = None


if __name__ == '__main__':

    argparser = argparse.ArgumentParser(description='Query blank records in indexd, call indexs3client')
    argparser.add_argument('--verbose', help='increase output verbosity', default=False, action='store_true')
    argparser.add_argument('--config_path',
                           help='read bucket config from here',
                           default='/var/s3indexer/fence-config.yaml'
                           )
    argparser.add_argument('--indexd_creds_path',
                           help='read indexd db config from here',
                           default='/var/s3indexer/indexd_creds.json'
                           )
    argparser.add_argument('--state_dir',
                           help='store offset pointer file(s) here',
                           default='/var/s3indexer/state')
    argparser.add_argument('--max_attempts',
                           help='maximum number of times to attempt file index',
                           default=4)
    argparser.add_argument('--attempt_intervals',
                           help='minutes between file index attempts',
                           nargs='+', type=int,
                           default=[1, 5, 30, 60, 120])
    argparser.add_argument('--dry_run',
                           help='echo commands',
                           default=False, action='store_true')


    args = argparser.parse_args()

    assert args.max_attempts <= len(args.attempt_intervals), f'Length of attempt_intervals {args.attempt_intervals} < max_attempts {args.max_attempts}'

    if args.verbose:
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    else:
        logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    logger = logging.getLogger('s3_inventory')

    upload_bucket.index(args, logger)
    external_buckets.index(args, logger)
