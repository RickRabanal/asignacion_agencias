import sys
import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr,coalesce,to_date,lower,current_timestamp,trim
from pyspark.conf import SparkConf
from datetime import datetime

from pyspark.sql import DataFrame
#from pyspark.sql import functions as F
from functools import reduce
from pyspark import StorageLevel

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

mongo_conn = sys.argv[1]
mongo_db = sys.argv[2]
coll_input=sys.argv[3]  ####ENTRADA POST MAQUINA DE DECISIONES
coll_output=sys.argv[4]  ####SALIDA MARCA AGENCIA


#.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.2") \
spark = SparkSession.builder.config("spark.mongodb.input.uri", mongo_conn)\
    .config("spark.mongodb.input.database", mongo_db)\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.mongodb.output.uri", mongo_conn) \
    .config("spark.mongodb.output.database", mongo_db)\
    .getOrCreate()



df = spark.read.format("mongo").option("schema_infer_mode", "dynamic").option("collection",coll_input).load()
df.printSchema()


# Regla 1: agencia 14946--PYPPER
estrategias_14946 = [
"F009","C009","F010","C010","F011",
"C011","F012","C012","F014","C014","G003","D002",
"F017","C017","F005","C003","F007","C007","G105","D105","G012","D012","F047","C047"]#Estrategias que van a pyper por default al tener AGE



# Regla 2: agencia 10000--CAHM
estrategias_10000 = ["C038", "F023", "C036", "F038"]

# Regla 2: agencia 77777 ALFA
estrategias_ALFA =  ["F018", "C018","F015","C015"]

# Regla MAC: agencia 33333
estrategias_MAC = ["F013", "C013", "F016", "C016"]

df = df.withColumn(
    "agencia",
    when(
        (col("Sub_estrategia").isin(estrategias_14946)) &
        ((~(col("agencia").isin("14946", "10000","77777", "33333"))) | col("agencia").isNull() | (trim(col("agencia")) == "")) &
        (col("propietario")=='000002011') &
        (~(col("Sub_estrategia").isin('F105','C105'))),
        lit("14946")
    ).when(
        (col("Sub_estrategia").isin(estrategias_10000)) &
        ((~(col("agencia").isin("14946", "10000","77777", "33333"))) | col("agencia").isNull() | (trim(col("agencia")) == "")),
        lit("10000")    
    ).when(
        (col("Sub_estrategia").isin(estrategias_ALFA)) &
        ((~(col("agencia").isin("14946", "10000","77777", "33333"))) | col("agencia").isNull() | (trim(col("agencia")) == "")),
        lit("77777")    
    ).when(
        (col("Sub_estrategia").isin(estrategias_MAC)) &
        ((~(col("agencia").isin("14946", "10000", "77777", "33333"))) | col("agencia").isNull() |(trim(col("agencia")) == "")),
        lit("33333")
    ).otherwise(trim(col("agencia"))) 
    #).otherwise(col("agencia"))
)

# Normalización final: quitar espacios y convertir a ""
df = df.withColumn(
    "agencia",
    when(trim(col("agencia")) == "", lit("")).otherwise(trim(col("agencia")))
)


if "fecha_inicio_asignacion" not in df.columns:
    df = df.withColumn("fecha_inicio_asignacion", lit(None).cast("timestamp"))
"""
df = df.withColumn(
    "agencia",  # o la columna que quieras modificar/asignar
    when(col("cc_ult_gestor").isNotNull(), col("agencia"))  # No se hace nada
    .when(col("cc_ult_gestor").isNull() & col("CANAL").contains("PRE"), "14946")
    .otherwise(col("agencia"))  # Mantener valor actual o null
)
"""

df = df.withColumn(
    "fecha_inicio_asignacion",
    when(
        (col("agencia").isNotNull()) & (col("fecha_inicio_asignacion").isNull()),
        current_timestamp()
    ).otherwise(col("fecha_inicio_asignacion"))
)


df.write.format("mongo") \
        .option("collection", coll_output) \
        .option("replaceDocument", "false") \
        .option("upsert", "true") \
        .mode("append") \
        .save()
print('TERMINA ','INICIAL')



spark.stop()
