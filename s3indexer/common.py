import os
import yaml
import json
import sqlite3
from datetime import datetime
from datetime import timedelta
import psycopg2


def get_indexd_db_connection(creds):
    """Connects to postgres."""
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
                'password': fence_config['INDEXD_PASSWORD'],
                }


def get_indexd_db_creds(indexd_creds_path):
    """Parses fence config for indexd credentials, returns indexs3client config file"""
    with open(indexd_creds_path) as stream:
        return json.load(stream)


def get_buckets(fence_config_path):
    """Parses fence config all info, return bucket config and credentials."""
    # {'region', 'signature_version', 'server_side_encryption', 'name', 'credentials': {'aws_access_key_id', 'aws_secret_access_key', 'endpoint_url'}}
    with open(fence_config_path) as stream:
        fence_config = yaml.load(stream, Loader=yaml.FullLoader)
        assert 'DATA_UPLOAD_BUCKET' in fence_config, 'fence_config missing DATA_UPLOAD_BUCKET'
        assert 'S3_BUCKETS' in fence_config, 'fence_config missing S3_BUCKETS'
        assert 'AWS_CREDENTIALS' in fence_config, 'fence_config missing AWS_CREDENTIALS'
        data_upload_bucket = fence_config['DATA_UPLOAD_BUCKET']
        s3_buckets = fence_config['S3_BUCKETS']
        aws_credentials = fence_config['AWS_CREDENTIALS']
        assert data_upload_bucket in s3_buckets, f'data_upload_bucket {data_upload_bucket} not found in S3_BUCKETS'
        for bucket_name, bucket in s3_buckets.items():
            assert 'cred' in bucket, '"cred" not found in bucket'
            cred = bucket['cred']
            aws_credential = aws_credentials[cred]
            bucket['name'] = bucket_name
            del bucket['cred']
            bucket['credentials'] = aws_credential
            bucket['is_data_upload'] = data_upload_bucket == bucket_name
        return s3_buckets
