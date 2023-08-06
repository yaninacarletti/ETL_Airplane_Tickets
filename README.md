# ETL_Airplane_Tickets

Este es un ETL para la ingesta de datos desde una API hacia un Data-warehouse.

Concretamente, desde la API de Kaggle se extraen tres archivos en formato json (dos Dim y el último Fact) se los de-normaliza para su posterior carga en AWS Redshift. Para efectuar este proceso de de-normalización, se emplea el framework Pyspark.

# Dependencias
- apache-airflow-providers-apache-spark
- apache-airflow [amazon]
- apache-airflow-providers-amazon
- apache-airflow [postgres]
- pandas-profiling
- kaggle

# Infraestructura necesaria
Este ETL se encuentra montado en un Docker, conteniendo los siguientes servicios:
- Spark
- Postgres
- Airflow
  
Expuesto en el port: 8080

Para correrlo se deben crear los siguientes directorios:

![image](https://github.com/yaninacarletti/EntregaFinal_YaninaCarletti_DATENG_51940/assets/97068537/b279abec-54a2-46f1-9486-80dd92c9ac81)

A su vez, se deberán crear las siguientes carpetas en los directorios 'dags' y 'tmp':

![image](https://github.com/yaninacarletti/EntregaFinal_YaninaCarletti_DATENG_51940/assets/97068537/03c78340-97ef-474a-8823-939210647ef7)

Se debe además descargar el driver: 'postgresql-42.5.2.jar' y ubicarlo en el path:

![image](https://github.com/yaninacarletti/EntregaFinal_YaninaCarletti_DATENG_51940/assets/97068537/6f0a4177-ea58-419f-b317-503a506a40cc)


Se debe crear el archivo '.env' con las siguientes variables de entorno: 

![image](https://github.com/yaninacarletti/EntregaFinal_YaninaCarletti_DATENG_51940/assets/97068537/25f380e5-62ee-43be-88ed-6bd7fa6ec74a)

Finalmente, para ejecutar el Docker Compose, ejecutar el siguiente comando:

docker-compose up --build

# Testeo
En el archivo 'spark_data_transformation.py' ubicado dentro del directorio 'scripts'. Concretamente en la línea 123, se encuentra la sentencia que genera que el dag termine cargándose en la tabla 'yanina_carletti_coderhouse.airplane_tickets' o enviándose una notificación por fallo. A continuación, se muestran ambas posibilidades:

Threshold definido = 'Número de cambios de tickets de avión' >= 4 

Cuando el 'número de cambios' es inferior a dicho threshold, se produce la carga de datos en la tabla mencionada como ilustra la imágen a continuación:

![image](https://github.com/yaninacarletti/EntregaFinal_YaninaCarletti_DATENG_51940/assets/97068537/36b1b837-a737-444d-9224-e3c858c57d18)

Cuando el 'número de cambios' es superior o igual al threshold definido, se genera la notificación como se muestra seguidamente:

![image](https://github.com/yaninacarletti/EntregaFinal_YaninaCarletti_DATENG_51940/assets/97068537/294ef9c9-6cc7-49d2-92e0-5463111a673c)

Ejemplo de notificación ante fallo de threshold 

![image](https://github.com/yaninacarletti/EntregaFinal_YaninaCarletti_DATENG_51940/assets/97068537/58dfbc8c-6e56-4642-ba56-d16d2163f313)

