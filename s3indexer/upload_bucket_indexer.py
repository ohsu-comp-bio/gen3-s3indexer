"""Inventory the gen3 upload bucket and output indexs3client command lines."""

import json
import requests
from s3indexer.common import get_indexd_db_connection, get_db_connection, get_processed, put_processed, expired, pending, increment, get_config_file, get_indexd_db_creds, get_buckets


def get_data_upload_bucket(fence_config_path):
    """Parses fence config for DATA_UPLOAD_BUCKET info, return bucket config and credentials."""
    # {'region', 'signature_version', 'server_side_encryption', 'name', 'credentials': {'aws_access_key_id', 'aws_secret_access_key', 'endpoint_url'}}
    buckets = get_buckets(fence_config_path)
    return [b for b in buckets.values() if b['is_data_upload']][0]


def get_blank_indexd_records_via_db(connection):
    """Fetches and yields blank records (no urls) from indexd."""
    cursor = connection.cursor()
    cursor.execute("select did, file_name from index_record where file_name is not null and size is null  ;")
    for row in cursor.fetchall():
        yield {'did': row[0], 'file_name': row[1]}
    cursor.close()


def get_blank_indexd_records_via_api(config):
    """Fetches and yields blank records (no urls) from indexd.
    DEPRECATED I attempted to use indexd’s  GET /index endpoint which nearly worked,
    but there was no way to query for indexd records that either had size:null or hashes:{}."""
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
    """Calls indexd, processes blank records for DATA_UPLOAD_BUCKET."""
    # sql lite connection
    state_db = get_db_connection(args.state_dir)
    #
    bucket = get_data_upload_bucket(args.config_path)
    # indexd client creds, render to string so we can pass to indexs3client
    indexd_config = get_config_file(args.config_path)
    config_file = json.dumps(indexd_config)
    # postgres connection to indexdb
    indexd_db_creds = get_indexd_db_creds(args.indexd_creds_path)
    indexd_db = get_indexd_db_connection(indexd_db_creds)
    # formulate command line env variables
    bucket_name = bucket['name']
    endpoint_url = bucket['credentials'].get('endpoint_url', None)
    aws_access_key_id = bucket['credentials']['aws_access_key_id']
    aws_secret_access_key = bucket['credentials']['aws_secret_access_key']
    aws_region = bucket.get('region', 'default')
    #
    for record in get_blank_indexd_records_via_db(indexd_db):
        input_url = f"s3://{bucket_name}/{record['did']}/{record['file_name']}"
        processed_url = get_processed(state_db, input_url)
        if expired(processed_url, args.max_attempts):
            logger.debug(f'expired {json.dumps(processed_url,default=str)}')
            continue
        if pending(processed_url, args.attempt_intervals):
            logger.debug(f'pending {json.dumps(processed_url,default=str)}')
            continue
        processed_url = increment(processed_url)
        put_processed(state_db, processed_url)
        endpoint_env_var = None
        if endpoint_url:
            endpoint_env_var = 'AWS_ENDPOINT={}'.format(endpoint_url)
        echo = ''
        if args.dry_run:
            echo = 'echo'
        print("{} AWS_REGION={} AWS_ACCESS_KEY_ID={} AWS_SECRET_ACCESS_KEY={} CONFIG_FILE='{}' INPUT_URL={} {} {}".format(
            echo,
            aws_region,
            aws_access_key_id,
            aws_secret_access_key,
            config_file,
            input_url,
            endpoint_env_var or '',
            '/indexs3client',
        ))
    logger.info(f'done DATA_UPLOAD_BUCKET {bucket_name}')
