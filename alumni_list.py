# Original Data : https://data.sfgov.org/Public-Safety/Law-Enforcement-Dispatched-Calls-for-Service-Real-/gnap-fj3t
import os
import csv
import json
import logging
import pandas as pd

os.environ["no_proxy"]="*" # set this for airflow errors. https://github.com/apache/airflow/discussions/24463


def write_json_to_gcs(storage_client, bucket_name, blob_name, data):
    try:
        """Write and read a blob from GCS using file-like IO"""
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        with blob.open("w") as f:
            json.dump(data, f)
    except Exception as e:
        logging.error(f'Cannot write json to {blob_name}')
        
def read_csv_from_gcs(storage_client ,bucket_name, blob_name):
    """Write and read a blob from GCS using file-like IO"""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open("r") as f:
        return list(csv.DictReader(f, delimiter=','))
    
def file_exists_on_gcs(storage_client, bucket_name, blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    stats = blob.exists(storage_client)
    return stats

def write_csv_to_gcs(storage_client, bucket_name, blob_name, data):
    try:
        """Write and read a blob from GCS using file-like IO"""
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        with blob.open("w") as f:
            write = csv.writer(f)
            write.writerows(data)
    except Exception as e:
        logging.error(f'Cannot write csv to {blob_name}')