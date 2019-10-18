"""Inventory external buckets and output indexs3client command lines."""

import json
import boto3
from botocore.client import Config

from s3indexer.common import get_db_connection, get_processed, put_processed, increment, get_config_file, get_buckets


def get_external_buckets(fence_config_path):
    buckets = get_buckets(fence_config_path)
    return [b for b in buckets.values() if not b['is_data_upload']]


def get_blank_indexd_records_via_db(connection):
    """Fetches and yields blank records (no urls) from indexd."""
    cursor = connection.cursor()
    cursor.execute("select did, file_name from index_record where file_name is not null and size is null  ;")
    for row in cursor.fetchall():
        yield {'did': row[0], 'file_name': row[1]}
    cursor.close()


def bucket_objects(bucket, logger):
    """Iterates through bucket contents, yields (record, region)"""
    # support non aws hosts
    endpoint_url = bucket['credentials'].get('endpoint_url', None)
    session = boto3.Session(aws_access_key_id=bucket['credentials']['aws_access_key_id'],
                            aws_secret_access_key=bucket['credentials']['aws_secret_access_key'])
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
    bucket_name = bucket['name']
    paginator = client.get_paginator('list_objects')
    pagination_args = {'Bucket': bucket_name}
    page_iterator = paginator.paginate(**pagination_args)
    try:
        for page in page_iterator:
            if 'Contents' not in page:
                logger.info('Nothing to do for {}'.format(bucket_name))
                logger.debug(page)
                continue
            aws_region = bucket.get('region', None)
            if not aws_region and 'x-amz-bucket-region' in page['ResponseMetadata']['HTTPHeaders']:
                aws_region = page['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
            for record in page.get('Contents', []):
                yield record, aws_region or 'default'

    except Exception as e:
        logger.error(f'Error getting objects from {bucket_name} {e}')


def index(args, logger):
    """Calls indexd, processes blank records for external bucket(s)."""
    # sql lite connection
    state_db = get_db_connection(args.state_dir)
    #
    buckets = get_external_buckets(args.config_path)
    # indexd client creds, render to string so we can pass to indexs3client
    indexd_config = get_config_file(args.config_path)
    indexd_config['extramural_bucket'] = True
    if len(buckets) == 0:
        logger.info(f'No external_buckets defined')
    for bucket in buckets:
        # formulate command line env variables
        bucket_name = bucket['name']
        endpoint_url = bucket['credentials'].get('endpoint_url', None)
        aws_access_key_id = bucket['credentials']['aws_access_key_id']
        aws_secret_access_key = bucket['credentials']['aws_secret_access_key']
        if bucket.get('extramural_uploader', None):
            indexd_config['extramural_uploader'] = bucket['extramural_uploader']
        config_file = json.dumps(indexd_config)
        # iterate through objects
        for record, aws_region in bucket_objects(bucket, logger):
            input_url = 's3://{}/{}'.format(bucket_name, record['Key'])
            processed_url = get_processed(state_db, input_url)
            # if we passed to indexs3client already, skip it
            if processed_url['attempt_count'] > 0:
                logger.info(f'processed {json.dumps(processed_url,default=str)}')
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
        logger.info(f'done external {bucket_name}')
