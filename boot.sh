# this script is used to boot a Docker container
source venv/bin/activate
while [ 1 ]
do
  python3 s3indexer/app.py | parallel --jobs 10
  sleep 60
done
