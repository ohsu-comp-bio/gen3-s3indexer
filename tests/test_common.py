import os
from s3indexer.common import get_buckets


def test_get_buckets():
    """Ensures we can read fence config and identify data_upload bucket."""
    state_dir = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(state_dir, 'fence-config.yaml')
    buckets = get_buckets(path)
    assert buckets, 'should find buckets'
    assert len(buckets) == 2, 'should have 2 buckets'
    assert len([b for b in buckets.values() if b['is_data_upload']]) == 1, 'should have 1 data_upload'
    assert len([b for b in buckets.values() if not b['is_data_upload']]) == 1, 'should have 1 external bucket'
    assert buckets['gen3-dev'] == {'region': 'default', 'signature_version': 's3', 'server_side_encryption': False, 'name': 'gen3-dev', 'credentials': {'aws_access_key_id': 'XXX', 'aws_secret_access_key': 'YYY', 'endpoint_url': 'https://some.ceph/'}, 'is_data_upload': True}
    assert buckets['external-dev'] == {'name': 'external-dev', 'credentials': {'aws_access_key_id': 'AAA', 'aws_secret_access_key': 'BBB', 'endpoint_url': 'https://some.external/'}, 'is_data_upload': False}
