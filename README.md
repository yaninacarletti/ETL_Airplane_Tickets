# ETL_Airplane_Tickets

Este es un ETL para la ingesta de datos desde una API hacia un Data-warehouse.

Concretamente, desde la API de Kaggle se extraen tres archivos en formato json (dos Dim y el último Fact) se los de-normaliza para su posterior carga en AWS Redshift. Para efectuar este proceso de de-normalización, se emplea el framework Pyspark.

# Dependencias
- Kaggle
- Psicopg2
- Pyspark

# Infraestructura necesaria
Este ETL se encuentra montado en un Docker:
- Jupyter Notebook + Pyspark
- Redshift
Expuesto en el port: 8888

Para correrlo se deben crear las siguientes carpetas en las rutas especificadas:
carpeta: postgres_data ----------> airplane_tickets_ETL/docker_shared_folder/postgres_data
carpeta: working_dir ----------> airplane_tickets_ETL/docker_shared_folder/working_dir
carpeta: spark_drivers ----------> airplane_tickets_ETL/docker_shared_folder/working_dir/spark_drivers

Se debe además descargar el driver: postgresql-42.5.2.jar y ubicarlo en la carpeta 'spark_drivers' creada anteriormente.
Se debe crear el archivo '.env' con las siguientes variables de entorno en la carpeta: airplane_tickets_ETL/docker_shared_folder/.env

POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST_AUTH_METHOD=trust
POSTGRES_PORT=5435
POSTGRES_HOST=postgres_sem7

REDSHIFT_USER=...
REDSHIFT_PASSWORD=...
REDSHIFT_HOST=data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DB=data-engineer-database
REDSHIFT_SCHEMA=...

JUPYTER_ENABLE_LAB=1
JUPYTER_TOKEN=coder
DRIVER_PATH=/home/coder/working_dir/spark_drivers/postgresql-42.5.2.jar

Finalmente, para ejecutar el Docker Compose, ejecutar el siguiente comando:

docker-compose up --build
