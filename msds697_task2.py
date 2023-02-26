import os
import airflow
import logging
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage

from user_definition import *
from alumni_list import *
from alumni_profiles import *

def get_cohorts():
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    cohorts = dict()
    try:
        for rec in read_csv_from_gcs(storage_client, bucket_name, ref_file_path):
            cohort_id = int(rec['Cohort'].split()[-1])
            if cohort_id not in cohorts:
                cohorts[cohort_id] = []
            cohorts[cohort_id].append(rec['LinkedIn'])
    except:
        logging.error('Couldn\'t get Alumni LinkedIn URLs')
    return cohorts

def _crawl_alumni_profiles(**kwargs):
    try:
        cohort_id = kwargs['cohort_id']
        profile_urls = kwargs['profile_urls']
    except:
        logging.error('cohort_id or profile_urls not found in kwargs')
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    people = []
    url = linkedin_rapid_api['url']
    headers = {
        "X-RapidAPI-Key": linkedin_rapid_api['key'],
        "X-RapidAPI-Host": linkedin_rapid_api['host']
    }
    for profile_url in profile_urls:
        try:
            querystring = {"url":profile_url}
            response = requests.request("GET", url, headers=headers, params=querystring)
            people.append(response.json())
        except:
            logging.warning(f'Profile {profile_url} not found. Skipping.')
    write_json_to_gcs(storage_client, bucket_name, f'{profiles_folder_path}Cohort_'+str(cohort_id)+'.json', people)


with DAG(
    dag_id="msds697-task2",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    for cohort, profiles in get_cohorts().items():

        create_insert_aggregate = SparkSubmitOperator(
            task_id=f"aggregates_to_mongo_cohort_{cohort}",
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
            application='/Users/mohanameher/airflow/dags/aggregates_to_mongo.py',
            # Pass args to Spark job
            application_args=[f'{profiles_folder_path}Cohort_'+str(cohort)+'.json']
        )

        crawl_alumni_profiles = PythonOperator(task_id = f"crawl_alumni_profiles_cohort_{cohort}",
                                                        provide_context=True,
                                                        python_callable=_crawl_alumni_profiles,
                                                        op_kwargs={'cohort_id': cohort , 'profile_urls': profiles},
                                                        dag=dag)
        
        crawl_alumni_profiles >> create_insert_aggregate

