# ETL_Airplane_Tickets

Este es un ETL para la ingesta de datos desde una API hacia un Data-warehouse.

Concretamente, desde la API de Kaggle se extraen tres archivos en formato json (dos Dim y el último Fact) se los de-normaliza para su posterior carga en AWS Redshift. Para efectuar este proceso de de-normalización, se emplea el framework Pyspark.

# Dependencias
- Kaggle
- Psycopg2
- Pyspark
- Docker

# Infraestructura necesaria
Este ETL se encuentra montado en un Docker:
- Jupyter Notebook + Pyspark
- Redshift
  
Expuesto en el port: 8888



Para correrlo se deben crear las siguientes carpetas en las rutas especificadas:

- carpeta: postgres_data ----------> airplane_tickets_ETL/docker_shared_folder/postgres_data

- carpeta: working_dir ----------> airplane_tickets_ETL/docker_shared_folder/working_dir

- carpeta: spark_drivers ----------> airplane_tickets_ETL/docker_shared_folder/working_dir/spark_drivers


Se debe además descargar el driver: 'postgresql-42.5.2.jar' y ubicarlo en la carpeta 'spark_drivers' creada anteriormente.

Se debe crear el archivo '.env' con las siguientes variables de entorno y ubicarlo en la carpeta indicada acontinuación: 

- archivo: .env ----------> airplane_tickets_ETL/docker_shared_folder/.env

![image](https://github.com/yaninacarletti/ETL_Airplane_Tickets/assets/97068537/0b5d6d07-1977-46f0-9f2e-da4d4ef1f311)


Finalmente, para ejecutar el Docker Compose, ejecutar el siguiente comando:

docker-compose up --build
