import zipfile
import requests
from os import getenv
from os import listdir
from os.path import isfile, join
from datetime import datetime
from datetime import timedelta
import os
import psycopg2
import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow import models
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator



"""
Extract data from url to bucket
"""

CONTENT_URL = "http://3.141.34.217/user_purchase.csv"
BUCKET_NAME = "capstone-db-terra-us"
PATH_FILE = "/tmp/user_purchase.csv"

def upload_blob():
    """Uploads a file to the bucket."""
    destination_blob_name = "user_purchase.csv"
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob()
    print(f"Uploading file {PATH_FILE} to {destination_blob_name}")
    blob.upload_from_filename(PATH_FILE)

def download_upload():
    
    req = requests.get(CONTENT_URL, allow_redirects=True)
    open(PATH_FILE, 'wb').write(req.content)
    #upload_blob()
    return "OK"

default_args = {
    'owner': 'alfredo.davila',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 1),
    'email': ['alfredo.davila@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

#name the DAG and configuration
dag = DAG('get_csv_data',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor()
    # CSV loading to table
    with open(file_path("cities_clean.csv"), "r") as f:
        next(f)
        curr.copy_from(f, 'cities', sep=",")
        get_postgres_conn.commit()

# [START howto_gcs_environment_variables]
BUCKET_NAME = os.environ.get('GCP_GCS_BUCKET', BUCKET_NAME)
PATH_TO_UPLOAD_FILE = os.environ.get('GCP_GCS_PATH_TO_UPLOAD_FILE', PATH_FILE)
DESTINATION_FILE_LOCATION = os.environ.get('GCP_GCS_DESTINATION_FILE_LOCATION', 'user_purchase.csv')
# [END howto_gcs_environment_variables]


task1 = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=download_upload,
                   dag=dag)
# [START howto_operator_local_filesystem_to_gcs]
upload_file = LocalFilesystemToGCSOperator(
                    task_id="upload_file",
                    src=PATH_TO_UPLOAD_FILE,
                    dst=DESTINATION_FILE_LOCATION,
                    bucket=BUCKET_NAME,
                    dag=dag
                )
# [END howto_operator_local_filesystem_to_gcs]
"""
task2 = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)
                   """


task1 >> upload_file