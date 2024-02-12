from google.cloud import storage, bigquery
import pandas as pd
import numpy as np

project_name = 'ANZ_dataset'
dataset_name = 'transaction_data'
table_name = 'historical_data'
bucket_name = 'hist_data'

# declare GCP clients
storage_client = storage.Client(project=project_name)
bigquery_client = bigquery.Client(project=project_name)

# upload data to GCS in chunks
#assuming 600 gb data path as file path
def file_to_gcs(file_path, blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

# load data into BigQuery
def load_into_bigquery(file_path, table_name):
    dataset = bigquery_client.dataset(dataset_name)
    table_ref = dataset.table(table_name)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    with open(file_path, "rb") as source_file:
        job = bigquery_client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )

    job.result() 

# Read the 600GB data in chunks of 30GB
chunk_size = 30  # in GB
total_size = 600
num_chunks = total_size // chunk_size

for chunk_number in range(num_chunks):
    start_byte = chunk_number * chunk_size * 1024**3
    end_byte = (chunk_number + 1) * chunk_size * 1024**3
    file_path = f'path/to/data_chunk_{chunk_number}.json'
    
    def get_data_chunk(start_byte, end_byte):
    data_size = end_byte - start_byte
    num_rows = int(data_size / 100)  # Assuming each row is approximately 100 bytes

    data_chunk = get_data_chunk(start_byte, end_byte)

    # Assuming data_chunk is a Pandas DataFrame
    data_chunk.to_json(file_path, orient='records', lines=True)

    # Upload the data chunk to GCS
    blob_name = f'data_chunks/data_chunk_{chunk_number}.json'
    file_to_gcs(file_path, blob_name)

    # Load the data into BigQuery
    load_into_bigquery(file_path, table_name)

#this will load the data chunks to historical data table , each load will be of 30gb

# this will print the view of synthetic data
def synthetic_data(data):
    np.random.seed(123)  #it will generate same set of random values if we do seeding

    data_1 = {
        'TransactionID': range(1, data + 1),
        'Amount': np.random.uniform(low=1.0, high=100.0, size=data),
    }

    return pd.DataFrame(data_1)
data = 50
synthetic_data = synthetic_data(data)
print(synthetic_data.head())