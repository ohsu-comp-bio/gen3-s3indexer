import os
from datetime import datetime
from datetime import timedelta

import s3indexer.upload_bucket_indexer as upload_bucket

state_dir = os.path.dirname(os.path.realpath(__file__))
path = os.path.join(state_dir, 'state.db')
if os.path.exists(path):
    os.remove(path)

def test_simple():
    conn = upload_bucket.get_db_connection(state_dir)
    input_url = 's3://foo/bar'
    processed_url = upload_bucket.get_processed(conn, input_url)
    assert processed_url['attempt_count'] == 0, f"should return 0 attempt_count {processed_url}"
    assert processed_url['last_attempt'], f"should return last_attempt {processed_url}"
    assert processed_url['url'] == input_url, f"should return input_url {processed_url}"

    last_attempt = processed_url['last_attempt']
    processed_url = upload_bucket.increment(processed_url)
    assert processed_url['attempt_count'] == 1, f"should increment by 1 attempt_count {processed_url}"
    assert processed_url['last_attempt'] != last_attempt, f"should increment last_attempt {processed_url}"

    processed_url = upload_bucket.increment(processed_url)
    assert processed_url['attempt_count'] == 2, f"should increment by 1 attempt_count {processed_url}"
    assert upload_bucket.expired(processed_url, max_attempts=1), "should be expired"
    assert not upload_bucket.expired(processed_url, max_attempts=10), "should not be expired"

    processed_url['last_attempt']  = datetime.now() - timedelta(minutes=5)
    assert upload_bucket.pending(processed_url, attempt_intervals=[1, 5, 10]), "should be pending"

    processed_url['last_attempt']  = datetime.now() - timedelta(minutes=11)
    assert not upload_bucket.pending(processed_url, attempt_intervals=[1, 5, 10]), "should not be pending"

    processed_url['attempt_count'] = 0
    assert not upload_bucket.pending(processed_url, attempt_intervals=[1, 5, 10]), "should not be pending"

    upload_bucket.put_processed(conn, processed_url)
