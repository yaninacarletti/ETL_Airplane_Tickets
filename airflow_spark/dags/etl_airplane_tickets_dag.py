from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from airflow.models import Variable

from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
from pandas_profiling import ProfileReport
from zipfile import ZipFile
import random
import pytz
from os import environ as env


env['KAGGLE_USERNAME']
env['KAGGLE_KEY']

TZ = pytz.timezone('America/Buenos_Aires')
PROCESS_DATE = datetime.now(TZ).strftime('%Y-%m-%d')

# Función para autenticarse en Kaggle y descargar archivo
def _authenticate_and_download(path):
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files('francuzovd/airplane-tickets-from-moscow-2022', path= path)
    return print("Done")

# Función para descompresión de archivo comprimido
def _file_decompression(zip_path, destination_path):
    with ZipFile(zip_path, 'r') as zip:
        zip.extractall(destination_path)
        print("Done")

# Función para obtener una muestra aleatoria del Fact.json y perfilamiento del mismo
def _sampling_and_profiling(json_path, html_path):
    df = pd.read_json(json_path)
    random.seed(0)
    selected_dates = random.sample(list(df['found_at'].unique()), k=10000) 
    df = df[df['found_at'].isin(selected_dates)]
    profile = ProfileReport(df, title= 'Data Quality Report')
    profile.to_file(html_path)
    return print("Done")

# create function to get process_date and push it to xcom
def _get_process_date(**kwargs):
    # If process_date is provided take it, otherwise take today
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)


QUERY_CREATE_TABLE = (f"""
CREATE TABLE IF NOT EXISTS {env['REDSHIFT_SCHEMA']}.airplane_tickets (
    found_at VARCHAR(50) DISTKEY,
    class VARCHAR(5),
    value DECIMAL(10,2),
    number_of_changes INT,
    depart_date VARCHAR(12),
    search_date VARCHAR(12),
    airline_code VARCHAR(12),
    airline_name_translations VARCHAR(100),
    origin VARCHAR(12),
    destination VARCHAR(12),
    country_code VARCHAR(12),
    time_zone VARCHAR(50),
    flightable BOOLEAN,
    iata_type VARCHAR(12),
    airport_name VARCHAR(50),
    airport_name_translations VARCHAR(100),
    airport_latitude DECIMAL(10,2),
    airport_longitude DECIMAL(10,2),
    process_date VARCHAR(12)
) SORTKEY(found_at);
""")

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM airplane_tickets WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""

defaul_args = {
    "owner": "Yanina Carletti",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="etl_airplane_tickets",
    default_args=defaul_args,
    description="ETL de la tabla airplane tickets",
    schedule_interval="@once",
    tags= ['airports', 'airlines', 'airplane_tickets'],
    catchup=False
) as dag:
    
# Tareas
    with TaskGroup("GetFiles", tooltip= "Obtencion de archivos") as GF:
        authenticate_download = PythonOperator(
            task_id="authenticate_download",
            python_callable= _authenticate_and_download,
            op_kwargs= {
                'path':'/opt/airflow/tmp/zip_file'
                },
            dag=dag
        )

        file_decompression = PythonOperator(
            task_id="file_decompression",
            python_callable= _file_decompression,
            op_kwargs= {
                'zip_path': '/opt/airflow/tmp/zip_file/airplane-tickets-from-moscow-2022.zip',
                'destination_path' : '/opt/airflow/tmp/data'
            },
            dag=dag
        )
        authenticate_download >> file_decompression

    with TaskGroup("DataQuality", tooltip = "Calidad de datos") as DQ:
        sampling_profiling = PythonOperator(
            task_id="sampling_profiling",
            python_callable= _sampling_and_profiling,
            op_kwargs= {
                'json_path': '/opt/airflow/tmp/data/ticket_dataset_RusAirports.json',
                'html_path' : '/opt/airflow/tmp/data/data_quality_report_{}.html'.format(PROCESS_DATE)
            },
            dag=dag
        )
        
        spark_data_transformation = SparkSubmitOperator(
            task_id="spark_data_transformation",
            application=f'{Variable.get("spark_scripts_dir")}/spark_data_transformation.py',
            conn_id="spark_default",
            dag=dag,
            driver_class_path=Variable.get("driver_class_path"),
        )
        sampling_profiling >> spark_data_transformation

    create_table = PostgresOperator(
            task_id="create_table",
            postgres_conn_id="redshift_default",
            sql= QUERY_CREATE_TABLE,
            dag=dag
        ) 

    with TaskGroup("RecordCleaning", tooltip= "Limpieza de registros") as RC:
        get_process_date_task = PythonOperator(
            task_id="get_process_date",
            python_callable= _get_process_date,
            provide_context=True,
            dag=dag
        )
        
        clean_process_date = PostgresOperator(
            task_id="clean_process_date",
            postgres_conn_id="redshift_default",
            sql= QUERY_CLEAN_PROCESS_DATE,
            dag=dag
        )   
        get_process_date_task >>  clean_process_date

    data_load = PostgresOperator(
        task_id="data_load",
        postgres_conn_id="redshift_default",
        sql= "/opt/airflow/tmp/data/airplane_tickets_{}.sql".format(PROCESS_DATE),
        dag = dag
    )

GF >> DQ >> create_table >> RC >> data_load


 






























   # data_load = SparkSubmitOperator(
    #     task_id="data_load",
    #     application=f'{Variable.get("spark_scripts_dir")}/data_load.py',
    #     conn_id="spark_default",
    #     dag=dag,
    #     driver_class_path=Variable.get("driver_class_path"),
    # )







