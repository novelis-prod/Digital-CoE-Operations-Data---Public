# Databricks notebook source
# MAGIC %run /config/secret_gen2

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/000-General/00.rml_config

# COMMAND ----------

# import  libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
import collections
import datetime
import re

# set the plant code
PLANT ='PBA'
log_plant = 'pba'
PLANT_CODE = '663'

# COMMAND ----------

field = [StructField("master_batch_id",StringType(), True),StructField("batch_id", StringType(), True),StructField("ingot_id", StringType(), True),StructField("metal_code", StringType(), True),StructField("caster", StringType(), True),StructField("pit_id", StringType(), True),StructField("year", StringType(), True),StructField("month", StringType(), True),StructField("day", StringType(), True),StructField("misc", StringType(), True)]
schema = StructType(field)
df = sqlContext.createDataFrame(sc.emptyRDD(), schema)

entity = 'ingot'
PLANT = 'pba'
out_path,tablename,schema=path_details(PLANT.lower(), entity.lower())
# write to table
df.write.format('delta').mode('append').partitionBy('year','month','day','master_batch_id').option("mergeSchema", "true").option('path',out_path).saveAsTable(schema + '.' +tablename)