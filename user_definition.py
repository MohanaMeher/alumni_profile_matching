import os
from datetime import date, datetime, timedelta

bucket_name = os.environ.get("GS_BUCKET_NAME")
service_account_key_file = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")

mongo_username = os.environ.get("MONGO_USERNAME")
mongo_password =  os.environ.get("MONGO_PASSWORD")
mongo_ip_address = os.environ.get("MONGO_IP")
database_name = os.environ.get("MONGO_DB_NAME")
collection_name = os.environ.get("MONGO_COLLECTION_NAME")

main_file_remote_path = "refs/alumni_list.csv"

linkedin_rapid_api = {
    'key': "e5631c58fbmsh8aa1d2740168739p1a2666jsn369e6d965356",
    'host': "linkedin-profile-data.p.rapidapi.com",
    'url':"https://linkedin-profile-data.p.rapidapi.com/linkedin-data"
}