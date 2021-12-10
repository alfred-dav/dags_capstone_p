import requests
import io
from datetime import datetime
from datetime import timedelta
import os
import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow import models
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from google.cloud import storage





"""
Extract data from url to bucket
Consulta el archivo de la url 
"""



CONTENT_URL = "http://3.141.34.217/user_purchase.csv"
BUCKET_NAME = "capstone-db-terra-us"
FILE_NAME = "user_purchase_clean.csv"
PATH_FILE = "/tmp/" + FILE_NAME

# [START howto_gcs_environment_variables]
BUCKET_NAME = os.environ.get('GCP_GCS_BUCKET', BUCKET_NAME)
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'capstone-db-alfredo')
INSTANCE_NAME = os.environ.get('GCSQL_POSTGRES_INSTANCE_NAME', 'dbname')
DB_NAME = os.environ.get('GCSQL_POSTGRES_DATABASE_NAME', 'testdb')
IMPORT_URI = os.environ.get('GCSQL_POSTGRES_IMPORT_URI', 'gs://' + BUCKET_NAME+ '/' + FILE_NAME)
PATH_TO_UPLOAD_FILE = os.environ.get('GCP_GCS_PATH_TO_UPLOAD_FILE', PATH_FILE)
DESTINATION_FILE_LOCATION = os.environ.get('GCP_GCS_DESTINATION_FILE_LOCATION', 'user_purchase_clean.csv')
# [END howto_gcs_environment_variables]

# [START howto_operator_cloudsql_import_body]
import_body = {"importContext": {"fileType": "csv", "uri": IMPORT_URI}}
# [END howto_operator_cloudsql_import_body]

def download_upload():
    import pandas as pd
    req = requests.get(CONTENT_URL, allow_redirects=True)
    urlData = req.content

    df = pd.read_csv(io.StringIO(urlData.decode('utf-8')))

    int_fields = ["Quantity", "CustomerID"]

    df[list(int_fields)] = df[int_fields].fillna(0.0).astype(int)

    df = df.rename(columns =
        {
            'InvoiceNo': 'invoice_number',
            'StockCode': 'stock_code',
            'Description': 'detail',
            'Quantity': 'quantity',
            'InvoiceDate': 'invoice_date',
            'UnitPrice': 'unit_price',
            'CustomerID': 'customer_id',
            'Country': 'country',
        })

    df.to_csv(PATH_FILE, sep='|', encoding='utf-8',index=False)

    return "OK"

default_args = {
    'owner': 'alfredo.davila',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 1),
    'email': ['alfredo.davila@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

#name the DAG and configuration
dag = DAG('get_csv_bucket_db',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

def csv_to_postgres():
    
    client = storage.Client()

    bucket = client.get_bucket(BUCKET_NAME)

    blob = bucket.get_blob(FILE_NAME)

    f = blob.download_as_string()

    #Open Postgres Connection
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor()
    # CSV loading to table
    
    curr.copy_from(f, 'user_purchase', sep='|')
    get_postgres_conn.commit()




get_clean_csv = PythonOperator(task_id='get_csv',
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

create_table = PostgresOperator(task_id = 'create_table',
                        sql="""
                        CREATE TABLE IF NOT EXISTS user_purchase (    
                            invoice_number varchar(10),
                            stock_code varchar(20),
                            detail varchar(1000),
                            quantity INTEGER,
                            invoice_date timestamp,
                            unit_price numeric(8,3),
                            customer_id INTEGER,
                            country varchar(20));
                            """,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)


insert_table = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)

#get_clean_csv >> upload_file >> create_table >> sql_import_task >> sql_import_task2
get_clean_csv >> upload_file >> create_table >> insert_table
