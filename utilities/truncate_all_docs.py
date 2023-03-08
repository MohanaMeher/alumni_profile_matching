from mongodb import *
from user_definition import *

mongodb = MongoDBCollection(mongo_username,
                                mongo_password,
                                mongo_ip_address,
                                database_name,
                                collection_name)
filter = {}
mongodb.delete_many(filter)