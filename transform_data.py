import json
import pandas as pd
import sys
import pyspark
from google.cloud import storage
from mongodb import *
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
from user_definition import *
from alumni_profiles import *


def return_profile_data_json(spark, x, profiles_path_by_cohort,json_field_name):

    alumni_data = spark.read.format("json")\
        .option("header", False)\
        .load(f"gs://{bucket_name}/{profiles_path_by_cohort}")
    y = x.split("/") #replace x by x[3]
    profile_id  = y[-2]
    #for testing only

    # alumni_data = spark.read.json("./file.json")
    cols_to_drop = ["connections","background_cover_image_url","profile_pic_url"]

            
    df = alumni_data.drop(*cols_to_drop)
    df = df.withColumnRenamed('public_identifier','_id')
    df = df.where(df._id==profile_id)

    profile_data_json = df.toJSON().collect()[0]
    profile_data_json = json.loads(profile_data_json)
    print(profile_data_json)

    # PASS JSON FIELD NAME

    rdd_dict = x.asDict() #single row from profile list to dict
    rdd_dict[json_field_name] = profile_data_json
    id = profile_data_json['_id']
    rdd_dict['_id'] = id
    rdd_dict.pop('id', None)
    return rdd_dict     #return full row along with profile json data



