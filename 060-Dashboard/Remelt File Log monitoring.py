# Databricks notebook source
# MAGIC %md # Overview of this notebook
# MAGIC 
# MAGIC 
# MAGIC This script generate statistics for the file processing monitoring purposes , which will be consumed by Power BI dashboard
# MAGIC 
# MAGIC • Read the tables rml_file_log and rml_file_log_archive and generate number files per day.
# MAGIC 
# MAGIC Input to this notebook: No parameters
# MAGIC 
# MAGIC Output of this notebook: Overwrite of the table rml_monitor 

# COMMAND ----------

# MAGIC %run /config/secret_gen2 

# COMMAND ----------

#define details of database connection
store_name = "novelisadlsg2"
container = "plantdata"

# COMMAND ----------

#define variables
env = 'prod'
table_name = 'rml_file_log'
trn_db = 'opstrnprodg2'
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"
destination_path = base_path + 'enterprise/' + env + '/remelt/master_tables/file_log'

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

# MAGIC %sql
# MAGIC optimize opstrnprodg2.rml_file_log_archive 
# MAGIC -- Optime archive table

# COMMAND ----------

#Below SQL updates the table rml_monitor

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE opstrnprodg2.rml_monitor (
# MAGIC  select  'file_log' as source, plant_name ,
# MAGIC   case  
# MAGIC  when file_path like '%Melter%' then 'Melter' 
# MAGIC  when file_path like '%Holder%'  then 'Holder'
# MAGIC  when file_path like '%Caster%'  then 'Caster'
# MAGIC  when file_path like '%DC%' then 'DC'
# MAGIC  when file_path like '%DC_Caster%' then 'Caster'
# MAGIC  when file_path like '%SM%' then 'SM'
# MAGIC  when file_path like '%Decoter%' then 'Decoter'
# MAGIC  when file_path like '%remeltsamplebatchsample%' or file_path like '%RemeltSample-BatchSample%' then 'BatchSample'
# MAGIC  when file_path like '%castingingot%' or file_path like '%Casting-Ingot%' then 'CastingIngot'
# MAGIC  when file_path like '%remeltprocessphase%' then 'RemeltProcessPhase'
# MAGIC  when file_path like '%remeltprocesscycle%' then 'RemeltProcessCycle'
# MAGIC  when file_path like '%remeltbatchmoltenbatchinput%' or file_path like '%RemeltBatch-MoltenBatchInput%' then 'RemeltMoltenBatchInput'
# MAGIC  ELSE 'Others'  end as  entity , 
# MAGIC    year, month, day, is_read ,  count(*) 
# MAGIC    fROm opstrnprodg2.rml_file_log  
# MAGIC    group by plant_name , entity, year, month, day,is_read
# MAGIC  union
# MAGIC  select 'file_archive' as source ,plant_name , 
# MAGIC   case  
# MAGIC  when file_path like '%Melter%' then 'Melter' 
# MAGIC  when file_path like '%Holder%'  then 'Holder'
# MAGIC  when file_path like '%Caster%'  then 'Caster'
# MAGIC  when file_path like '%DC%' then 'DC'
# MAGIC  when file_path like '%DC_Caster%' then 'Caster'
# MAGIC  when file_path like '%SM%' then 'SM'
# MAGIC  when file_path like '%Decoter%' then 'Decoter'
# MAGIC  when file_path like '%remeltsamplebatchsample%' or file_path like '%RemeltSample-BatchSample%' then 'BatchSample'
# MAGIC  when file_path like '%castingingot%' or file_path like '%Casting-Ingot%' then 'CastingIngot'
# MAGIC  when file_path like '%remeltprocessphase%' then 'RemeltProcessPhase'
# MAGIC  when file_path like '%remeltprocesscycle%' then 'RemeltProcessCycle'
# MAGIC  when file_path like '%remeltbatchmoltenbatchinput%' or file_path like '%RemeltBatch-MoltenBatchInput%' then 'RemeltMoltenBatchInput'
# MAGIC  ELSE 'Others'  end as  entity , 
# MAGIC  year, month, day,is_read , count(*) 
# MAGIC  fROm opstrnprodg2.rml_file_log_archive 
# MAGIC   group by plant_name , entity,year, month, day,is_read )

# COMMAND ----------

