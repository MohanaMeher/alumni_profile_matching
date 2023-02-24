from datetime import date, datetime, timedelta
import json
import requests

from google.cloud import storage


def retreive_api_data(noaa_token, api_url):
    head = {'token':f'{noaa_token}'}
    response = requests.get(api_url, headers=head)
    return response.json()

def write_json_to_gcs(bucket_name, blob_name, service_account_key_file, data):
    """Write and read a blob from GCS using file-like IO"""
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        json.dump(data, f)