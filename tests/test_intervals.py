import os
from datetime import datetime
from datetime import timedelta

import s3indexer.app as app

state_dir = os.path.dirname(os.path.realpath(__file__))
path = os.path.join(state_dir, 'state.db')
if os.path.exists(path):
    os.remove(path)

def test_simple():
    conn = app.get_db_connection(state_dir)
    input_url = 's3://foo/bar'
    processed_url = app.get_processed(conn, input_url)
    assert processed_url['attempt_count'] == 0, f"should return 0 attempt_count {processed_url}"
    assert processed_url['last_attempt'], f"should return last_attempt {processed_url}"
    assert processed_url['url'] == input_url, f"should return input_url {processed_url}"

    last_attempt = processed_url['last_attempt']
    processed_url = app.increment(processed_url)
    assert processed_url['attempt_count'] == 1, f"should increment by 1 attempt_count {processed_url}"
    assert processed_url['last_attempt'] != last_attempt, f"should increment last_attempt {processed_url}"

    processed_url = app.increment(processed_url)
    assert processed_url['attempt_count'] == 2, f"should increment by 1 attempt_count {processed_url}"
    assert app.expired(processed_url, max_attempts=1), "should be expired"
    assert not app.expired(processed_url, max_attempts=10), "should not be expired"

    processed_url['last_attempt']  = datetime.now() - timedelta(minutes=5)
    assert app.pending(processed_url, attempt_intervals=[1, 5, 10]), "should be pending"

    processed_url['last_attempt']  = datetime.now() - timedelta(minutes=11)
    assert not app.pending(processed_url, attempt_intervals=[1, 5, 10]), "should not be pending"

    processed_url['attempt_count'] = 0
    assert not app.pending(processed_url, attempt_intervals=[1, 5, 10]), "should not be pending"

    app.put_processed(conn, processed_url)
