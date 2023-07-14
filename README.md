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

![image](https://github.com/yaninacarletti/ETL_Airplane_Tickets/assets/97068537/5e6454e2-2d6a-4398-9502-9fffea528862)



Se debe además descargar el driver: 'postgresql-42.5.2.jar' y ubicarlo en la carpeta 'spark_drivers' creada anteriormente.

Se debe crear el archivo '.env' con las siguientes variables de entorno y ubicarlo en la carpeta indicada acontinuación: 

![image](https://github.com/yaninacarletti/ETL_Airplane_Tickets/assets/97068537/347e9543-49a0-413b-9dec-62c32fb9317e)


![image](https://github.com/yaninacarletti/ETL_Airplane_Tickets/assets/97068537/f1cfd711-50bc-44f4-b503-ae6fa1433667)


Finalmente, para ejecutar el Docker Compose, ejecutar el siguiente comando:

docker-compose up --build
