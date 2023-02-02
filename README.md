# BigQuery table data extractor
## Article


Free data extractor for Google Cloud Bigquery. Check quotas.

## Usage

Add your service account credentials to `app.py`:
```python
...
service_acount_str = { "type": "service_account", "project_id": "your-project", "private_key_id": "", "private_key": "-----BEGIN PRIVATE KEY----...\n-----END PRIVATE KEY-----\n", "client_email": "your-service-account-email@your-project.iam.gserviceaccount.com", "client_id": "123", "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs", "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bigquery-adminsdk%40client.iam.gserviceaccount.com" }
...
```

Adjust `dates` and `tables` in question:

```python
...
# from where and where to (bucket)
    # Public project source and test staging buucket
    project = 'firebase-public-project'
    dataset_id = 'analytics_153293282'
    bucket_name = 'firebase-events-archive-avro'
...

# Lambda handler:
def lambda_handler(event, context):
    print(event)

    start_date = date(2018,9,1)
    end_date = date(2018,9,3)

...
```

Install `vrtual_env`.

```sh
cd stack
cd bq_extractor
virtualenv bq_extractor_env
source bq_extractor_env/bin/activate
pip install -r requirements.txt
```

Run the Lambda:
```sh
# Test your service locally by ruunning in command line
python-lambda-local -f lambda_handler -t 10 app.py event.json
```

Output should be something like this:
```sh
...
Export table to gs://firebase-events-archive-avro/dt=2018-10-03/category=1/partitionKey/events_*.avro
[root - INFO - 2023-02-01 15:30:22,716] END RequestId: ac865a0b-05dd-4ef3-9b5f-9bfe7f0ce5b7
[root - INFO - 2023-02-01 15:30:22,717] REPORT RequestId: ac865a0b-05dd-4ef3-9b5f-9bfe7f0ce5b7	Duration: 2284.26 ms
```


Let's list our bucket to see if data is there:
```sh
gsutil ls gs://firebase-events-archive-avro/
```