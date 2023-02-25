import json
import pandas as pd
import pyspark
from google.cloud import storage
from mongodb import *
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
from user_definition import *
from alumni_profiles import *




def return_profile_data_json(x,profile_data_path,json_field_name):

    conf = pyspark.SparkConf().set("spark.jars", '/Users/renukavallimarepalli/Documents/MyDocuments/USF/Spring 1/Distributed Data Systems/Final Project/alumni_profile_matching/gcs-connector-hadoop2-latest.jar')
    sc = pyspark.SparkContext(conf=conf).getOrCreate()
    conf = sc.sparkContext._jsc.hadoopConfiguration()
    conf.set("google.cloud.auth.service.account.json.keyfile", service_account_key_file)
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")

    alumni_data= (
    sc.read.format("json")
    .load(f"gs://{bucket_name}/{profile_data_path}")
    # .load(f"gs://{bucket_name}/profiles/Cohort_9.json")
    )
    # alumni_list.show()

    spark = SparkSession.builder.getOrCreate()

    #Get profile identifier from url # For testing only
    x = "https://www.linkedin.com/in/justin-battles-75929554/"
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



