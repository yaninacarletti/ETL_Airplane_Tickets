from os import environ as env
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, udf 
from pyspark.sql.types import *
import pandas as pd
import json
from datetime import datetime
import pytz
import random


DRIVER_PATH = env["DRIVER_PATH"]

env['PYSPARK_SUBMIT_ARGS'] = f'--driver-class-path {DRIVER_PATH} --jars {DRIVER_PATH} pyspark-shell'
env['SPARK_CLASSPATH'] = DRIVER_PATH

TZ = pytz.timezone('America/Buenos_Aires')
PROCESS_DATE = datetime.now(TZ).strftime('%Y-%m-%d')
# INSERTION_SCRIPT_PATH = "/opt/airflow/tmp/data/airplane_tickets_{}.sql".format(PROCESS_DATE)
INSERTION_SCRIPT_PATH = "/opt/airflow/dags/sql/airplane_tickets_{}.sql".format(PROCESS_DATE)

# Crear sesión de Spark
spark = (
    SparkSession.builder.master("local[1]")
    .appName("data transformation with Spark")
    .config("spark.jars", DRIVER_PATH)
    .config("spark.executor.extraClassPath", DRIVER_PATH)
    .getOrCreate()
)


# Transformación de Datos
# Creando Dataframe 'Fact'
with open(r"/opt/airflow/tmp/data/ticket_dataset_RusAirports.json") as file:
    dic1 = json.load(file)

# fact_df1 = spark.read.json(spark.sparkContext.parallelize(dic1), multiLine=True)
pandasdf1 =pd.json_normalize(dic1)
fact_df1 = spark.createDataFrame(pandasdf1)

# Cambio del tipo de dato
# fact_df1 = fact_df1.withColumn("search_date", f.col("search_date").cast(DateType()))
# fact_df1 = fact_df1.withColumn("depart_date", f.col("depart_date").cast(DateType()))
# fact_df1 = fact_df1.withColumn("found_at", f.col("found_at").cast(TimestampType()))
fact_df1 = fact_df1.withColumn("number_of_changes", col("number_of_changes").cast(IntegerType()))

# Dropeo de columnas
fact_df1 = fact_df1.drop('trip_class')

# Dropeo de filas duplicadas
fact_df1 = fact_df1.dropDuplicates()

# Creando Dataframe 'Dim_1'
with open(r"/opt/airflow/tmp/data/IATA_Airports.json") as file:
    dic2 = json.load(file)

# dim_df2 = spark.read.json(spark.sparkContext.parallelize(dic2), multiLine=True)
pandasdf2 =pd.json_normalize(dic2)
dim_df2 = spark.createDataFrame(pandasdf2)

#Dropeo de columnas
dim_df2 = dim_df2.drop("city_code")
dim_df2 = dim_df2.drop("name")

# Renombramiento de columnas
dim_df2 = dim_df2.withColumnRenamed("name_translations.en", "airport_name")
dim_df2 = dim_df2.withColumnRenamed("coordinates.lat", "airport_latitude")
dim_df2 = dim_df2.withColumnRenamed("coordinates.lon", "airport_longitude")

# Creando Dataframe 'Dim_2'
with open(r"/opt/airflow/tmp/data/IATA_Airlines.json") as file:
    dic3 = json.load(file)

# dim_df3 = spark.read.json(spark.sparkContext.parallelize(dic3), multiLine=True)
pandasdf3 =pd.json_normalize(dic3)
dim_df3 = spark.createDataFrame(pandasdf3)

# Dropeo de la columna 'name'
dim_df3 = dim_df3.drop("name")

# Union de Dataframes "fact_df1" and "dim_df3"
merged_df =fact_df1.join(dim_df3, fact_df1.airline == dim_df3.code, "inner")

# Dropeo de la columna 'airline'
merged_df = merged_df.drop("airline")

# Renombramiento de columnas
merged_df = merged_df.withColumnRenamed("code", "airline_code")
merged_df = merged_df.withColumnRenamed("name_translations.en", "airline_name_translations")

# Union de Dataframes "merged_df" and "dim_df2" y creación del Dataframe final "sparkdf"
sparkdf = merged_df.join(dim_df2, merged_df.destination == dim_df2.code, "inner")

# Dropeo de la columna 'code'
sparkdf = sparkdf.drop("code")

# Creación de la columna 'Class' (función de la columna 'Value')
sparkdf = sparkdf.withColumn("class", \
   when((sparkdf.value > 100000), lit("A")) \
     .when((sparkdf.value >= 15000) & (sparkdf.value <= 100000), lit("B")) \
     .otherwise(lit("C")) \
  )

# Función definida por el usuario "replace_character" e implementada en las columnas 'found_at', 'airport_name', 
# 'airline_name_translations' y 'destination'
replace_character = udf(lambda x: x.replace("-","/"), StringType())
sparkdf = sparkdf.withColumn("found_at", replace_character("found_at"))

replace_character = udf(lambda x: x.replace("'"," "), StringType())
sparkdf = sparkdf.withColumn("airport_name", replace_character("airport_name"))

replace_character = udf(lambda x: x.replace("'"," "), StringType())
sparkdf = sparkdf.withColumn("airline_name_translations", replace_character("airline_name_translations"))

replace_character = udf(lambda x: x.replace("ТАУ","ТАY"), StringType())
sparkdf = sparkdf.withColumn("destination", replace_character("destination"))


# Filtro basado en la columna "origin"
sparkdf = sparkdf.filter(sparkdf.origin == 'MOW')

# La siguiente línea comentarla (o no) si se quiere que se cumpla o no  el umbral: "number_of_changes >= 4"
# sparkdf = sparkdf.filter(sparkdf.number_of_changes < 1)

# añadir columna "process_date"
sparkdf = sparkdf.withColumn("process_date", lit(PROCESS_DATE))

# Ordenamiento de columnas
sparkdf = sparkdf.select("found_at", "class", "value", "number_of_changes",\
                         "depart_date", "search_date", "airline_code", "airline_name_translations",\
                         "origin", "destination", "country_code", \
                         "time_zone", "flightable", "iata_type", "airport_name", \
                         "airport_latitude","airport_longitude","process_date")

sparkdf.printSchema()
sparkdf.show(truncate = False)

# Generación del script de sql para la inserción de datos en la tabla 'airplane_tickets'
print("Generando script INSERT...")
pandasdf = sparkdf.toPandas()

# Las siguientes líneas son a los efectos que la carga sea rápida y probar fallos
# Muestreo aleatorio de 1000 registros
random.seed(0)
selected_dates = random.sample(list(pandasdf['found_at'].unique()), k=1000) 
pandasdf = pandasdf[pandasdf['found_at'].isin(selected_dates)]

with open (INSERTION_SCRIPT_PATH, 'w') as f:
    for index, row in pandasdf.iterrows():
        values = f"('{row['found_at']}', '{row['class']}', {row['value']}, {row['number_of_changes']},\
         '{row['depart_date']}', '{row['search_date']}', '{row['airline_code']}',\
          '{row['airline_name_translations']}', '{row['origin']}', '{row['destination']}', '{row['country_code']}',\
           '{row['time_zone']}', {row['flightable']}, '{row['iata_type']}', '{row['airport_name']}',\
            {row['airport_latitude']}, {row['airport_longitude']}, '{row['process_date']}')"
        insert = f"INSERT INTO {env['REDSHIFT_SCHEMA']}.airplane_tickets VALUES {values};\n"
        f.write(insert)

# Lo guardamos como csv
pandasdf.to_csv('/opt/airflow/tmp/data/airplane_tickets_{}.csv'.format(PROCESS_DATE), index= False)








