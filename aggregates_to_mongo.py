import sys
from mongodb import *
from pyspark.sql import Row, SparkSession
from user_definition import *
import logging


def insert_aggregates_to_mongo(profiles_path_by_cohort):
    spark = SparkSession.builder.getOrCreate()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("google.cloud.auth.service.account.json.keyfile", service_account_key_file)
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    alumni_data = spark.read.format("json")\
        .option("header", False)\
        .load(f"gs://{bucket_name}/{profiles_path_by_cohort}")
    cols_to_drop = ["connections","background_cover_image_url","profile_pic_url"]
    df = alumni_data.drop(*cols_to_drop)
    df = df.withColumnRenamed('public_identifier','_id')
    rdd1 = df.rdd
    aggregates = rdd1.map(lambda x:{x[-6]: x})

    mongodb = MongoDBCollection(mongo_username,
                                mongo_password,
                                mongo_ip_address,
                                database_name,
                                collection_name)
    for aggregate in aggregates.collect():
        mongodb.insert_one(aggregate)

if __name__=="__main__":
    profiles_path_by_cohort = sys.argv[1]
    try:
        insert_aggregates_to_mongo(profiles_path_by_cohort)
    except:
        logging.error(f'Error inserting aggregate record to mongodb')
