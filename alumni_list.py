# Original Data : https://data.sfgov.org/Public-Safety/Law-Enforcement-Dispatched-Calls-for-Service-Real-/gnap-fj3t
import os
import logging
os.environ["no_proxy"]="*" # set this for airflow errors. https://github.com/apache/airflow/discussions/24463


def upload_to_bucket(storage_client, bucket_name, blob_path, local_path):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_path)
    except Exception as e:
        logging.error(f'Failed to upload main file: e')

        







