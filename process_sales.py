from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

# Constants for the API
API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'
API_TOKEN = '2b8d97ce57d401abd89f4'
HEADERS = {'Authorization': f'Bearer {API_TOKEN}'}

def extract_data_from_api():
    """Extracts sales data from the API for the last 3 days."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=3)
    response = requests.get(f'{API_URL}?start_date={start_date}&end_date={end_date}', headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f'API Request Failed with status code {response.status_code}')

def convert_to_avro(data):
    """Converts JSON data to Avro format."""
    schema = avro.schema.parse(open('sales_schema.avsc', 'rb').read())
    with open('sales_data.avro', 'wb') as out_file:
        writer = DataFileWriter(out_file, DatumWriter(), schema)
        for record in data:
            writer.append(record)
        writer.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='process_sales',
    default_args=default_args,
    description='DAG for processing sales data',
    schedule_interval='0 1 * * *',  # Runs daily at 01:00
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['sales'],
) as dag:

    task1 = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=extract_data_from_api
    )

    task2 = PythonOperator(
        task_id='convert_to_avro',
        python_callable=convert_to_avro,
        op_kwargs={'data': task1.output}
    )

    task1 >> task2

