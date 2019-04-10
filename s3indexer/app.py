"""Inventory a bucket and output indexs3client command lines."""

import os
import sys
import boto3
import logging
import argparse
from botocore.client import Config
import yaml
import json
import sqlite3

logger = None


def get_db_connection(state_dir):
    """ ensure table and index exists"""
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    path = os.path.join(state_dir, 'state.db')
    conn = sqlite3.connect(path)
    conn.row_factory = dict_factory
    conn.executescript(
        '''
        CREATE TABLE IF NOT EXISTS processed_objects
            (url text UNIQUE) ;

        CREATE INDEX IF NOT EXISTS processed_objects_idx on
            processed_objects (url) ;
        '''
    )
    conn.commit()
    return conn


def get_processed(conn, url):
    """retrieve the record from the db, return none if doesn't exist"""
    cur = conn.cursor()
    cur.execute("select * from processed_objects where url = :url", {'url': url})
    processed_url = cur.fetchone()
    if not processed_url:
        return None
    return processed_url['url']


def put_processed(conn, url):
    """set the record in the db"""
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE into processed_objects values (:url)", url)
    conn.commit()


def last_run(path):
    """ parse urls and status from last run file"""
    # 2019/04/05 23:21:50 Finish to compute hashes for /76e05c66-3ea9-412b-8dca-82a33bc6af71/unique-009
    # 2019/04/05 23:21:50 Finish updating the record. Response Status: 200 OK
    try:
        with open(path) as ins:
            for line in ins:
                tokens = line.split(' ')
                if 'Finish to compute hashes' in line:
                    yield tokens[7]
    except Exception:
        pass


def get_s3_creds_list(fence_config_path):
    """parse fence_config. [{cred_key: {buckets:[{name, ...}], aws_access_key_id, aws_secret_access_key}}] """

    with open(fence_config_path) as stream:
        fence_config = yaml.load(stream, Loader=yaml.FullLoader)
        bucket_list = {}
        for k in fence_config['AWS_CREDENTIALS'].keys():
            aws_access_key_id = fence_config['AWS_CREDENTIALS'][k].get('aws_access_key_id', None)
            if not aws_access_key_id or len(aws_access_key_id) < 1:
                continue
            bucket_list[k] = fence_config['AWS_CREDENTIALS'][k]
            bucket_list[k]['buckets'] = []
        for b in fence_config['S3_BUCKETS']:
            cred_key = fence_config['S3_BUCKETS'][b]['cred']
            if cred_key not in bucket_list:
                continue
            bucket = fence_config['S3_BUCKETS'][b]
            del bucket['cred']
            bucket['name'] = b
            bucket_list[cred_key]['buckets'].append(bucket)
        return bucket_list


def get_config_file(fence_config_path):
    """parse fence config. return s3indexs3client config file"""

    with open(fence_config_path) as stream:
        fence_config = yaml.load(stream, Loader=yaml.FullLoader)
        return {'url': fence_config['INDEXD'] + '/index',
                'username': fence_config['INDEXD_USERNAME'],
                'password': fence_config['INDEXD_PASSWORD']}


def save_offset(offset, bucket_name, state_dir):
    """ save offset dict """

    with open('{}/offset.{}.txt'.format(state_dir, bucket_name), 'w'
              ) as data_file:
        data_file.write(offset['Key'])


if __name__ == '__main__':

    argparser = argparse.ArgumentParser(description='Consume events from bucket, populate indexd')
    argparser.add_argument('--endpoint_url', '-ep',
                           help='for swift, ceph, other non-aws endpoints', default=None)
    argparser.add_argument('--list_objects_api',
                           help='list_objects or list_objects_v2 default: list_objects_v2', default='list_objects_v2')
    argparser.add_argument('--verbose', help='increase output verbosity', default=False, action='store_true')
    argparser.add_argument('--state_dir',
                           help='store offset pointer file(s) here',
                           default='/var/s3indexer/state')
    argparser.add_argument('--config_path',
                           help='read bucket config from here',
                           default='/var/s3indexer/fence-config.yaml'
                           )

    args = argparser.parse_args()

    if args.verbose:
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    else:
        logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    logger = logging.getLogger('s3_inventory')
    conn = get_db_connection(args.state_dir)

    for url in last_run('./last_run.txt'):
        put_processed(url)

    s3_creds = get_s3_creds_list(args.config_path)
    config_file = json.dumps(get_config_file(args.config_path))
    for cred_key in s3_creds.keys():
        s3_cred = s3_creds[cred_key]
        endpoint_url = s3_cred.get('endpoint_url', None)
        for bucket in s3_cred['buckets']:
            bucket_name = bucket['name']
            aws_access_key_id = s3_cred['aws_access_key_id']
            aws_secret_access_key = s3_cred['aws_secret_access_key']
            session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
            list_objects_api = bucket.get('list_objects_api', 'list_objects')
            # support non aws hosts
            if endpoint_url:
                use_ssl = True
                if endpoint_url.startswith('http://'):
                    use_ssl = False
                client = session.client('s3',
                                        endpoint_url=endpoint_url, use_ssl=use_ssl,
                                        config=Config(s3={'addressing_style': 'path'}, signature_version='s3')
                                        )
            else:
                client = session.client('s3')

            paginator = client.get_paginator(list_objects_api)
            pagination_args = {'Bucket': bucket_name}
            page_iterator = paginator.paginate(**pagination_args)
            try:
                for page in page_iterator:
                    aws_region = None
                    if 'x-amz-bucket-region' in page['ResponseMetadata']['HTTPHeaders']:
                        aws_region = page['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
                    if 'Contents' not in page:
                        logger.info('Nothing to do for {}'.format(bucket_name))
                        logger.debug(page)
                    for record in page.get('Contents', []):
                        input_url = 's3://{}/{}'.format(bucket_name, record['Key'])
                        processed_url = get_processed(conn, input_url)
                        if processed_url:
                            continue
                        endpoint_env_var = None
                        if endpoint_url:
                            endpoint_env_var = 'AWS_ENDPOINT={}'.format(endpoint_url)
                        print("AWS_REGION={} AWS_ACCESS_KEY_ID={} AWS_SECRET_ACCESS_KEY={} CONFIG_FILE='{}' INPUT_URL={} {} {}".format(
                            aws_region,
                            aws_access_key_id,
                            aws_secret_access_key,
                            config_file,
                            input_url,
                            endpoint_env_var,
                            '/indexs3client',
                        ))
                print('echo done {}'.format(bucket_name))
            except Exception as e:
                logger.error('Error processing {}'.format(bucket_name))
                logger.error(e)
