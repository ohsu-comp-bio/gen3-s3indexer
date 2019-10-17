"""Inventory a bucket and output indexs3client command lines."""

import os
import sys
import logging
import argparse
import yaml
import json
import requests
import sqlite3
from datetime import datetime
from datetime import timedelta
import psycopg2

logger = None


def get_indexd_db_connection(creds):
    """Connects to postgres, ensure table and index exists"""
    return psycopg2.connect(
        user=creds['db_username'],
        password=creds['db_password'],
        host=creds['db_host'],
        database=creds['db_database'])

def get_db_connection(state_dir):
    """Connects to sqllite, ensure table and index exists"""
    # return results as a dict
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    path = os.path.join(state_dir, 'state.db')
    conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = dict_factory
    conn.executescript(
        '''
        CREATE TABLE IF NOT EXISTS processed_objects
            (url text UNIQUE, last_attempt timestamp, attempt_count integer) ;
        CREATE INDEX IF NOT EXISTS processed_objects_idx on
            processed_objects (url) ;
        '''
    )
    conn.commit()
    return conn


def get_processed(conn, url):
    """Retrieves the record from the db, return initialized if doesn't exist"""
    cur = conn.cursor()
    cur.execute("select * from processed_objects where url = :url", {'url': url})
    processed_url = cur.fetchone()
    if not processed_url:
        processed_url = {'url': url, 'last_attempt': datetime.now(), 'attempt_count': 0}
    return processed_url


def put_processed(conn, processed_url):
    """Sets the record in the db"""
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE into processed_objects values (:url, :last_attempt, :attempt_count)", (processed_url['url'], processed_url['last_attempt'], processed_url['attempt_count']))
    conn.commit()


def expired(processed_url, max_attempts):
    """Returns True if we have exhausted retries."""
    return processed_url['attempt_count'] > max_attempts


def pending(processed_url, attempt_intervals):
    """Returns True if we are between intervals."""
    if processed_url['attempt_count'] == 0:
        return False
    attempt_count = processed_url['attempt_count']
    next_interval = processed_url['last_attempt'] + timedelta(minutes=attempt_intervals[attempt_count])
    return datetime.now() < next_interval


def increment(processed_url):
    """Increments count and last attempt timestamp."""
    processed_url['attempt_count'] += 1
    processed_url['last_attempt'] = datetime.now()
    return processed_url


def get_config_file(fence_config_path):
    """Parses fence config for indexd credentials, returns indexs3client config file"""
    with open(fence_config_path) as stream:
        fence_config = yaml.load(stream, Loader=yaml.FullLoader)
        return {'url': fence_config['INDEXD'] + '/index',
                'username': fence_config['INDEXD_USERNAME'],
                'password': fence_config['INDEXD_PASSWORD']}


def get_indexd_db_creds(indexd_creds_path):
    """Parses fence config for indexd credentials, returns indexs3client config file"""
    with open(indexd_creds_path) as stream:
        return json.load(stream)


def get_data_upload_bucket(fence_config_path):
  """Parses fence config for DATA_UPLOAD_BUCKET info, return bucket config and credentials."""
  # {'region', 'signature_version', 'server_side_encryption', 'name', 'credentials': {'aws_access_key_id', 'aws_secret_access_key', 'endpoint_url'}}
  with open(fence_config_path) as stream:
      fence_config = yaml.load(stream, Loader=yaml.FullLoader)
      assert 'DATA_UPLOAD_BUCKET' in fence_config, 'fence_config missing DATA_UPLOAD_BUCKET'
      assert 'S3_BUCKETS' in fence_config, 'fence_config missing S3_BUCKETS'
      assert 'AWS_CREDENTIALS' in fence_config, 'fence_config missing AWS_CREDENTIALS'
      bucket_name = fence_config['DATA_UPLOAD_BUCKET']
      s3_buckets = fence_config['S3_BUCKETS']
      aws_credentials = fence_config['AWS_CREDENTIALS']
      assert bucket_name in s3_buckets, f'{bucket_name} not found in S3_BUCKETS'
      bucket = s3_buckets[bucket_name]
      assert 'cred' in bucket, '"cred" not found in bucket'
      cred = bucket['cred']
      aws_credential = aws_credentials[cred]
      bucket['name'] = bucket_name
      del bucket['cred']
      bucket['credentials'] = aws_credential
      return bucket


def get_blank_indexd_records_via_db(connection):
    """Fetches and yields blank records (no urls) from indexd."""
    cursor = connection.cursor()
    cursor.execute("select did, file_name from index_record where file_name is not null and size is null  ;")
    for row in cursor.fetchall():
        yield {'did': row[0], 'file_name': row[1]}
    cursor.close()

def get_blank_indexd_records_via_api(config):
    """Fetches and yields blank records (no urls) from indexd."""
    # {'acl': [],
    #  'authz': [],
    #  'baseid': 'c61c103c-b0d4-4e76-b215-dfd6f6f1e5ca',
    #  'created_date': '2019-10-16T16:50:42.653547',
    #  'did': '0aa10681-7c48-44c2-8913-adc514812a9b',
    #  'file_name': 'foo.bar',
    #  'form': None,
    #  'hashes': {},
    #  'metadata': {},
    #  'rev': '20589597',
    #  'size': None,
    #  'updated_date': '2019-10-16T16:50:42.653553',
    #  'uploader': 'foo@bar.edu',
    #  'urls': [],
    #  'urls_metadata': {},
    #  'version': None}
    url = f"{config['url'].rstrip('/')}?urls=[]&start=00000000-0000-0000-0000-000000000000&limit=1024"
    auth = (config["username"], config["password"])
    indexd_response = requests.get(url, auth=auth)
    assert indexd_response.status_code == 200, f'Retrieve index from indexd should return 200 status: {indexd_response.status_code} {indexd_response.text}'
    response = indexd_response.json()
    assert 'records' in response, f'Retrieve index from indexd should return records: {indexd_response.status_code} {indexd_response.text}'
    for record in response['records']:
        if not record.get('file_name', None):
            continue
        yield record


def index(args, logger):
    """Calls indexd, processes blank records."""
    # sql lite connection
    conn = get_db_connection(args.state_dir)

    bucket = get_data_upload_bucket(args.config_path)
    indexd_config = get_config_file(args.config_path)
    # render to string so we can pass to indexer
    config_file = json.dumps(indexd_config)
    #
    indexd_db_creds = get_indexd_db_creds(args.indexd_creds_path)
    indexd_db = get_indexd_db_connection(indexd_db_creds)


    bucket_name = bucket['name']
    endpoint_url = bucket['credentials'].get('endpoint_url', None)
    aws_access_key_id = bucket['credentials']['aws_access_key_id']
    aws_secret_access_key = bucket['credentials']['aws_secret_access_key']
    aws_region = bucket['region']
    #for record in get_blank_indexd_records_via_api(indexd_config):
    for record in get_blank_indexd_records_via_db(indexd_db):
        input_url = f"s3://{bucket_name}/{record['did']}/{record['file_name']}"
        processed_url = get_processed(conn, input_url)
        if expired(processed_url, args.max_attempts):
            logger.info(f'echo expired {json.dumps(processed_url,default=str)}')
            continue
        if pending(processed_url, args.attempt_intervals):
            logger.info(f'echo pending {json.dumps(processed_url,default=str)}')
            continue
        processed_url = increment(processed_url)
        put_processed(conn, processed_url)
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
    logger.info(f'echo done {bucket_name}')


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


    args = argparser.parse_args()

    assert args.max_attempts <= len(args.attempt_intervals) , f'Length of attempt_intervals {args.attempt_intervals} < max_attempts {args.max_attempts}'

    if args.verbose:
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    else:
        logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    logger = logging.getLogger('s3_inventory')

    index(args, logger)
