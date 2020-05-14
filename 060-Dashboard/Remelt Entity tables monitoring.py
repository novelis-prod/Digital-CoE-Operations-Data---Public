# Databricks notebook source
# MAGIC %md # Overview of this notebook
# MAGIC 
# MAGIC This script generate statistics for the file processing monitoring purposes , which will be consumed by Power BI dashboard
# MAGIC   • Read various entity tables(Ex: osw_melter,osw_holder) and populate number of records and unique master batch ids
# MAGIC 
# MAGIC Input to this notebook: No parameters
# MAGIC 
# MAGIC Output of this notebook: Overwrite of the rml_entity_data

# COMMAND ----------

# MAGIC %run /config/secret_gen2 

# COMMAND ----------

#define details of database connection
store_name = "novelisadlsg2"
container = "plantdata"

# COMMAND ----------

#define variables
env = 'prod'
table_name = 'rml_entity_data'
trn_db = 'opstrnprodg2'
entity_table = trn_db + '.' + table_name
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"
destination_path = base_path + 'enterprise/' + env + '/remelt/master_tables/file_log'

# COMMAND ----------

print(entity_table)

# COMMAND ----------

#intializing file system 
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://"+container+"@"+store_name+".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

#create file system client to access the files from gen2
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import *

from azure.storage.filedatalake import DataLakeServiceClient
#define service client 
try:  
    global service_client
        
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", store_name), credential=key)
    
except Exception as e:
    print(e)

file_system_client = service_client.get_file_system_client(file_system=container)

# COMMAND ----------

#Import necessary from library
from pyspark.sql.types import StringType,IntegerType 
from datetime import  date, datetime, timedelta
from pyspark.sql import functions as f
from pyspark.sql.functions import when,lit
from pyspark.sql.functions import *


# COMMAND ----------

delta_tables = {
  "osw_melter": {
    "plant": "osw",
    "entity": 'melter'
  },
  "osw_holder": {
    "plant": "osw",
    "entity": 'holder'
  },
  "osw_caster": {
    "plant": "osw",
    "entity": 'caster'
  },
  "osw_charge": {
    "plant": "osw",
    "entity": 'charger'
  },
  "osw_chemistry": {
    "plant": "osw",
    "entity": 'chemistry'
  },
  "osw_phase": {
    "plant": "osw",
    "entity": 'phase'
  },
  "osw_cycle": {
    "plant": "osw",
    "entity": 'cycle'
  },
  "osw_ingot": {
    "plant": "osw",
    "entity": 'ingot'
  },
  "yej_melter": {
    "plant": "yej",
    "entity": 'melter'
  },
  "yej_holder": {
    "plant": "yej",
    "entity": 'holder'
  },
  "yej_caster": {
    "plant": "yej",
    "entity": 'caster'
  },
  "yej_decoter": {
    "plant": "yej",
    "entity": 'decoter'
  },
  "yej_sm": {
    "plant": "yej",
    "entity": 'sm'
  },
}

# COMMAND ----------

df = None
for k, v in delta_tables.items():
  table_sel = "select '{3}' as table , '{0}' as plant ,'{1}' as entity, year,month,day , count(*) as count from opsentprodg2.{2} group by year,month,day ".format(v["plant"],v["entity"],k,k)
#   print(table_sel)
  if df is None:
    df = spark.sql(table_sel)
  else:
    df2 = spark.sql(table_sel)
    df = df.union(df2)

# COMMAND ----------

df = df.withColumn("date",to_timestamp(to_date(concat(col("year"),lit("-"),col("month"),lit("-"),col("day")))))
df = df.withColumn("count",col("count").cast('integer'))
column_list = ["table","plant","entity","date","count"]
df = df.select(*column_list)  

# COMMAND ----------

delta2_tables = {
  "osw_melter": {
    "plant": "osw",
    "entity": 'melter'
  },
  "osw_holder": {
    "plant": "osw",
    "entity": 'holder'
  },
  "osw_caster": {
    "plant": "osw",
    "entity": 'caster'
  },
  "osw_charge": {
    "plant": "osw",
    "entity": 'charger'
  },
  "osw_chemistry": {
    "plant": "osw",
    "entity": 'chemistry'
  },
  "osw_phase": {
    "plant": "osw",
    "entity": 'phase'
  },
  "osw_cycle": {
    "plant": "osw",
    "entity": 'cycle'
  },
  "osw_ingot": {
    "plant": "osw",
    "entity": 'ingot'
  },
  "yej_melter": {
    "plant": "yej",
    "entity": 'melter'
  },
  "yej_holder": {
    "plant": "yej",
    "entity": 'holder'
  },
  "yej_caster": {
    "plant": "yej",
    "entity": 'caster'
  }
}

# COMMAND ----------

df3 = None
for k, v in delta2_tables.items():
  table_sel = "select '{3}' as table , '{0}' as plant ,'{1}' as entity, year,month,day , count(distinct(master_batch_id)) as mbcount from opsentprodg2.{2} group by year,month,day ".format(v["plant"],v["entity"],k,k)
#   print(table_sel)
  if df3 is None:
    df3 = spark.sql(table_sel)
  else:
    df4 = spark.sql(table_sel)
    df3 = df3.union(df4)

# COMMAND ----------

df3 = df3.withColumn("date",to_timestamp(to_date(concat(col("year"),lit("-"),col("month"),lit("-"),col("day")))))
df3 = df3.withColumn("mbcount",col("mbcount").cast('integer'))
column_list = ["table","plant","entity","date","mbcount"]
df3 = df3.select(*column_list)  

# COMMAND ----------

df4 = df.join(df3,on=['table','plant','entity','date'],how="left")
# display(df4)

# COMMAND ----------

#option('path',destination_path)
df4.write.format('delta').mode('overwrite').partitionBy("plant","entity").option("overwriteSchema", "true").saveAsTable((entity_table))

# COMMAND ----------

# %sql
# select  * from  opstrnprodg2.rml_entity_data

# COMMAND ----------

