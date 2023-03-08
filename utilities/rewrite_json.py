import json
from google.cloud import storage
from user_definition import *
from alumni_list import *

def read_json_from_gcs(storage_client ,bucket_name, blob_name):
    """Write and read a blob from GCS using file-like IO"""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open("r") as f:
        return list(json.load(f))

for cohort_id in range(1, 11):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    blob_name = f'{profiles_folder_path}Cohort_'+str(cohort_id)+'.json'
    _file = read_json_from_gcs(storage_client, bucket_name, blob_name)
    data = [x for x in _file if type(x) is dict]
    print(cohort_id, len(_file), len(data))
    write_json_to_gcs(storage_client, bucket_name, blob_name, data)
