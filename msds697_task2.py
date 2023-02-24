import os
import airflow
import logging
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from user_definition import *
from alumni_list import *
from alumni_profiles import *

def _download_alumni_list():
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    upload_to_bucket(storage_client, bucket_name, main_file_remote_path, main_file_local_path)
    
def _crawl_alumni_profiles():
    pass
    

with DAG(
    dag_id="msds697-task2",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    create_insert_aggregate = SparkSubmitOperator(
        task_id="aggregates_to_mongo",
        packages="com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        exclude_packages="javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri",
        conf={"spark.driver.userClassPathFirst":True,
             "spark.executor.userClassPathFirst":True,
            #  "spark.hadoop.fs.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            #  "spark.hadoop.fs.AbstractFileSystem.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            #  "spark.hadoop.fs.gs.auth.service.account.enable":True,
            #  "google.cloud.auth.service.account.json.keyfile":service_account_key_file,
             },
        verbose=True,
        application='aggregates_to_mongo.py'
    )
    download_alumni_list = PythonOperator(task_id = "download_alumni_list",
                                                  python_callable = _download_alumni_list,
                                                  dag=dag)

    crawl_alumni_profiles = PythonOperator(task_id = "crawl_alumni_profiles",
                                                    python_callable = _crawl_alumni_profiles,
                                                    dag=dag)
    
    download_alumni_list >> crawl_alumni_profiles >> create_insert_aggregate

