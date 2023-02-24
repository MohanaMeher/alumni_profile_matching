# Original Data : https://data.sfgov.org/Public-Safety/Law-Enforcement-Dispatched-Calls-for-Service-Real-/gnap-fj3t
import os
import logging
os.environ["no_proxy"]="*" # set this for airflow errors. https://github.com/apache/airflow/discussions/24463


def write_json_to_gcs(bucket_name, blob_name, service_account_key_file, dct):
    """Write and read a blob from GCS using file-like IO"""
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        json.dump(dct, f)
        
def read_csv_from_gcs(bucket_name, blob_name, service_account_key_file):
    """Write and read a blob from GCS using file-like IO"""
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # read alumnis list
    with blob.open("r") as f:
        df = pd.read_csv(f)
    return df

        







