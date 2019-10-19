# FROM quay.io/cdis/indexs3client:master as indexs3client
FROM onprem/indexs3client as indexs3client

# debian:stretch
FROM python:3.7-stretch

# install parallel
RUN apt-get update && apt-get install -y parallel

# cherry pick gen3 index client from the master image
COPY --from=indexs3client /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=indexs3client /indexs3client /indexs3client

# setup our python env
WORKDIR /home/app
COPY requirements.txt requirements.txt
RUN python -m venv venv
RUN venv/bin/pip install -r requirements.txt

# copy our app into container
COPY . .
COPY boot.sh ./
RUN chmod +x boot.sh
RUN venv/bin/pip install -r requirements-dev.txt
RUN venv/bin/pip install -e .

# we will store ./state (last key read from bucket(s)) here
# we read ./config (fence yaml) from here
VOLUME ["/var/s3indexer"]

# read buckets, update indexd, sleep, repeat
ENTRYPOINT ["/bin/bash", "-c" ,"./boot.sh"]
