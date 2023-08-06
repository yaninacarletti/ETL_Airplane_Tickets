from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_common.operators.redshift_custom_operator import PostgreSQLOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
import smtplib


TZ = pytz.timezone('America/Buenos_Aires')
PROCESS_DATE = datetime.now(TZ).strftime('%Y-%m-%d') 

env['KAGGLE_USERNAME']
env['KAGGLE_KEY']

# Función para autenticarse en Kaggle y descargar archivo
def _authenticate_and_download(path, ti):
    try:
        # raise Exception('Error')
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files('francuzovd/airplane-tickets-from-moscow-2022', path= path)
        return print("Done")
    except Exception as exception:
        ti.xcom_push(key = 'tarea_1', value = 'Falla en la tarea authenticate_and_download')
        print(exception)
        print('Failure')
        raise Exception('Error')

# Función para descompresión de archivo comprimido
def _file_decompression(zip_path, destination_path, ti):
    try:
        # raise Exception('Error')
        with ZipFile(zip_path, 'r') as zip:
            zip.extractall(destination_path)
            print("Done")
    except Exception as exception:
        ti.xcom_push(key = 'tarea_2', value = 'Falla en la tarea file_decompression')
        print(exception)
        print('Failure')
        raise Exception('Error')    

# Función para obtener una muestra aleatoria del Fact.json y perfilamiento del mismo
def _sampling_and_profiling(json_path, html_path, ti):
    try:
        # raise Exception('Error')
        df = pd.read_json(json_path)
        random.seed(0)
        selected_dates = random.sample(list(df['found_at'].unique()), k=10000) 
        df = df[df['found_at'].isin(selected_dates)]
        profile = ProfileReport(df, title= 'Data Quality Report')
        profile.to_file(html_path)
        return print("Done")
    except Exception as exception:
        ti.xcom_push(key='tarea_3', value= 'Falla en la tarea sampling_and_profiling')
        print(exception)
        print('Failure')
        raise Exception('Error')

# Función que valida el umbral
def _threshold_validator(path, ti):
    Threshold = 4
    df = pd.read_csv(path + '/airplane_tickets_{}.csv'.format(PROCESS_DATE))
    for elem in df.number_of_changes:
        if elem >= Threshold:                       
            ti.xcom_push(key = 'threshold_validator', value = 'Threshold > 4')
            raise Exception('Error')

# Función para obtener process_date y enviarlo a xcom
def _get_process_date(**kwargs):
    # Si se proporciona la fecha del proceso, tómela, de lo contrario, tómela hoy.
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

# Función que notifica ante fallo de threshold 
def _enviar(ti):
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()#
        # send email using password save in python variable
        x.login(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_PASSWORD'))
        subject="Airflow Alert"
        body_text = "DAG for id 'etl_airplane_tickets' failed\nTask failed 'threshold_validator'\nFor not fulfilling the condition: " + ti.xcom_pull(key= 'threshold_validator', task_ids='threshold_validator')
        # body_text = ti.xcom_pull(key= 'tarea_1', task_ids='authenticate_download')
        # body_text = ti.xcom_pull(key= 'tarea_2', task_ids='file_decompression') 
        # body_text = ti.xcom_pull(key= 'tarea_3', task_ids='sampling_profiling')
        message='Subject: {}\n\n{}'.format(subject, body_text)
        x.sendmail(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_EMAIL_TO'),message)
        print('Exito')
        print(body_text)
    except Exception as exception:
        print(exception)
        print('Failure')
        raise exception


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
    "email": ['yanina.carletti@gmail.com'],
    "email_on_failure" : True,
    "email_retry": False,
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
            task_id='authenticate_download',
            python_callable= _authenticate_and_download,
            op_kwargs= {
                'path':'/opt/airflow/tmp/zip_file'
                },
            provide_context = True,
            dag=dag
        )

        file_decompression = PythonOperator(
            task_id="file_decompression",
            python_callable= _file_decompression,
            op_kwargs= {
                'zip_path': '/opt/airflow/tmp/zip_file/airplane-tickets-from-moscow-2022.zip',
                'destination_path' : '/opt/airflow/tmp/data'
                },
            provide_context = True,
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
            provide_context = True,
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
    
    threshold_validator = PythonOperator(
            task_id = "threshold_validator",
            python_callable= _threshold_validator,
            op_kwargs={
                'path' : Variable.get('csv_path')
                },
            dag=dag
    )


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
            provide_context = True,
            dag=dag
        )
        
        clean_process_date = PostgresOperator(
            task_id="clean_process_date",
            postgres_conn_id="redshift_default",
            sql= QUERY_CLEAN_PROCESS_DATE,
            dag=dag
        )   
        get_process_date_task >>  clean_process_date

    data_load = PostgreSQLOperator(
            task_id="data_load",
            postgres_conn_id="redshift_default",
            sql= 'sql/airplane_tickets_{}.sql'.format(PROCESS_DATE)
        )

    notification = PythonOperator(
        task_id ='notification',
        python_callable = _enviar,
        trigger_rule ='all_failed'
    )

GF  >> DQ >> threshold_validator  >> create_table >> RC >> data_load >>  notification


 























 




