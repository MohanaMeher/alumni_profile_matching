import json
import requests


def retreive_api_data(noaa_token, api_url):
    head = {'token':f'{noaa_token}'}
    response = requests.get(api_url, headers=head)
    return response.json()

def write_json_to_gcs(storage_client, bucket_name, blob_name, data):
    """Write and read a blob from GCS using file-like IO"""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open("w") as f:
        json.dump(data, f)

def read_file_from_gcs(storage_client ,bucket_name, blob_name):
    """Write and read a blob from GCS using file-like IO"""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open("r") as f:
        return f
