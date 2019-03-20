# s3indexer

## overview

## motivation

The [ssjdispatcher](https://github.com/uc-cdis/ssjdispatcher) is a "aws native" service that monitors changes to aws based s3 buckets via [s3 event notifications](https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html#notification-how-to-overview) and a k8s based job dispatch of [indexs3client](https://github.com/uc-cdis/indexs3client).

For implementations unable to deploy in aws, this utility service removes the dependency to s3-events and simply lists the buckets storing a the last processed bucket key in the state directory.

## flow

A docker based gen3 utility that:

* mounts `fence-config.yaml` and a `state` directory from the host system into `/var/s3indexer/` in the container.

* reads fence-config.yaml, specifically the [AWS_CREDENTIALS](https://github.com/uc-cdis/compose-services/blob/master/templates/fence-config.yaml#L287-L293), [S3_BUCKETS](https://github.com/uc-cdis/compose-services/blob/master/templates/fence-config.yaml#L295-L304) and [INDEXD](https://github.com/uc-cdis/compose-services/blob/master/templates/fence-config.yaml#L328-L333) stanzas.

* logging is done to stderr

* for each bucket:
  * A moderate degree of scalability is achieved by streaming command lines to stdout which are piped into [gnu's parallel utility](https://www.gnu.org/software/parallel/) to call [indexs3client](https://github.com/uc-cdis/indexs3client) passing AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, CONFIG_FILE, INPUT_URL and AWS_ENDPOINT.
  * saves "state" in state/offset.bucket_name.txt

* sleep and rescan starting from the last saved key

## dependencies

* gen3 - we assume a gen3 environment
* indexs3client

## extensions

* In order to support non-aws s3 stores (ceph, swift, minio,...), we read a new element `endpoint` from fence-config's `S3_BUCKETS` stanzas. This is paired with a small modification to [indexs3client:s3-endpoint] (https://github.com/ohsu-comp-bio/indexs3client/commit/2fd2d303e6023697fc9be78b972076f3dce07949).  Until and if this extension is merged to master, our docker image refers to our branch as `indexs3client:s3-endpoint`

## //todo

*  tests
