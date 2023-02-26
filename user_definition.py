import os

bucket_name = os.environ.get("GS_BUCKET_NAME")
service_account_key_file = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")
ref_file_path = os.environ.get("GS_REF_FILE_PATH")
profiles_folder_path = os.environ.get("GS_PROFILES_FOLDER_PATH")

mongo_username = os.environ.get("MONGO_USERNAME")
mongo_password =  os.environ.get("MONGO_PASSWORD")
mongo_ip_address = os.environ.get("MONGO_IP")
database_name = os.environ.get("MONGO_DB_NAME")
collection_name = os.environ.get("MONGO_COLLECTION_NAME")

linkedin_rapid_api = {
    'key': os.environ.get("RAPID_API_KEY"),
    'host': os.environ.get("RAPID_API_HOST"),
    'url': os.environ.get("RAPID_API_URL")
}

spark_application = os.environ.get("SPARK_APPLICATION")