from google.cloud import storage, bigquery
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def file_to_gcs(file_path, blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

# load data into BigQuery
def load_into_bigquery(file_path, table_name):
    dataset = bigquery_client.dataset(dataset_name)
    table_ref = dataset.table(table_name)
    
    path = 'GCS path'
    table_ref = bigquery_client.dataset(dataset_name).table(table_name)

    load_job = bigquery_client.load_table_from_path(
        path, table_ref, job_config=job_config
    )

    load_job.result()
    
project_name = 'ANZ_dataset'
dataset_name = 'transaction_data'
table_name = 'historical_data'
bucket_name = 'hist_data'

# declare GCP clients
storage_client = storage.Client(project=project_name)
bigquery_client = bigquery.Client(project=project_name)

for i in range(21):
    target_date = current_date - timedelta(days=i)
    date_str = target_date.strftime("%Y%m%d")
    
    # Assuming the data for each day in a file named 'daily_data_YYYYMMDD.csv' is available in GCS
    gcs_path = f'daily_data.csv'
    daily_data = pd.read_csv('path to daily_data.csv')
    
    file_to_gcs(daily_data, gcs_path)
    load_into_bigquery(gcs_path, table_name)