# https://googleapis.dev/python/bigquery/latest/index.html
import json
import requests
from datetime import datetime, date, timedelta

from google.api_core import retry
from google.cloud import bigquery
from google.oauth2 import service_account

response = requests.get('https://api.github.com')
print(response)

# # API key usage
service_acount_str = { "type": "service_account", "project_id": "your-project", "private_key_id": "", "private_key": "-----BEGIN PRIVATE KEY----...\n-----END PRIVATE KEY-----\n", "client_email": "your-service-account-email@your-project.iam.gserviceaccount.com", "client_id": "123", "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs", "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bigquery-adminsdk%40client.iam.gserviceaccount.com" }

credentials = service_account.Credentials.from_service_account_info(service_acount_str)
# # https://googleapis.dev/python/google-api-core/latest/auth.html#overview


def bigquery_hello(txt):
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    # QUERY = ('SELECT {};'.format('"it is nice to meet you "'))
    QUERY = ('SELECT "{} nice to meet you";'.format(txt))
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    greet = list(rows)[0][0]
    return greet

def export_table_to_storage(table_name, bucket_partition, category):
    # from google.cloud import bigquery
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    project = 'your-project'
    dataset_id = 'analytics'
    bucket_name = 'firebase-events-archive-avro'

    destination_uri = "gs://{}/{}/{}/partitionKey/events_*.avro".format(bucket_name, bucket_partition, category)
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    # table_ref = dataset_ref.table("events_20220101")
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.job.ExtractJobConfig()
    # job_config.compression = bigquery.Compression.GZIP
    # https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.ExtractJobConfig
    job_config.destination_format = bigquery.DestinationFormat.AVRO
    job_config.compression = bigquery.Compression.SNAPPY

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
        job_config=job_config,
    )  # API request
    # extract_job.result()  # Waits for job to complete. Calling client.extract_table starts the job. No need to wait to finish
    print("Export table to {}".format(destination_uri))

def lambda_handler(event, context):
    print("Function invoked with event {}".format(event))

    start_date = date(2018,10,3)
    end_date = date(2018,10,4)
    
    dates= [start_date+timedelta(days=x) for x in range((end_date-start_date).days)]
    # add a loop for category here if needed:
    category_number = 1
    for dt in dates:
        table_name = dt.strftime('ml_data_%Y%m%d$1')
        partition_name = dt.strftime('dt=%Y-%m-%d')
        category = "category={}".format(category_number)
        export_table_to_storage(table_name, partition_name, category)

    bigquery_message = bigquery_hello('it is ')
    # export_table()

    message = 'Hello {} {}, {}!'.format(event['first_name'], event['last_name'], bigquery_message)  
    return { 
        'message' : message
    }
