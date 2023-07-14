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

![image](https://github.com/yaninacarletti/ETL_Airplane_Tickets/assets/97068537/cd251328-ee38-4109-8de6-7a65c7d134cc)





Se debe además descargar el driver: 'postgresql-42.5.2.jar' y ubicarlo en la carpeta 'spark_drivers' creada anteriormente.

Se debe crear el archivo '.env' con las siguientes variables de entorno y ubicarlo en la carpeta indicada acontinuación: 

![image](https://github.com/yaninacarletti/ETL_Airplane_Tickets/assets/97068537/e02d4928-3025-4e10-8924-cc846c86a6ac)



![image](https://github.com/yaninacarletti/ETL_Airplane_Tickets/assets/97068537/6ef56b88-dcf6-4db4-af04-01e7bd877ec7)




Finalmente, para ejecutar el Docker Compose, ejecutar el siguiente comando:

docker-compose up --build
