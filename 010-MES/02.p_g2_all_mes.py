# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview of this notebook
# MAGIC 
# MAGIC This notebook is to read raw data and create a MES delta table in transform folder.
# MAGIC 
# MAGIC • Read data into a data frame with columns identified for coil_index table 
# MAGIC 
# MAGIC • Update the file log table if any errors raised while reading  data into dataframe
# MAGIC 
# MAGIC • Add new columns with plant code from SAP, metal code with default value and metal code time stamp
# MAGIC 
# MAGIC • Make schema compatible for coil index table(column names and column data types)
# MAGIC 
# MAGIC • Check and update the metal code value for a coil id which has a generated metal code in previous runs
# MAGIC 
# MAGIC • Append this new data to mes delta table in transform zone and update is read column value in file log table

# COMMAND ----------

# DBTITLE 1,Connect adls to databricks
# MAGIC %run /config/secret_gen2

# COMMAND ----------

# MAGIC %md Define parameters

# COMMAND ----------

#import libraries to perform functional operations 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, DoubleType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import expr,lit
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from delta.tables import *

#define data storage table details 
env = 'prod'
trn_db = 'opstrnprodg2'
spark.sql('create database if not exists '+trn_db)
table_name = 'mes'

#define details of folder locations
store_name = "novelisadlsg2"
container = "plantdata"

#input file path
input_path = dbutils.widgets.get("input_file")
file_name = input_path.split('/')[-1]

#details of folder locations
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

#destination path to store new data
destination_path = base_path + 'transform/' + env + '/mes'

#input path to read plant index table
plant_index_input_path = base_path + "transform/master_tables/plant_index.csv"


#input path for file log delta table
file_log_path= base_path + 'transform/' + env + '/file_log'

#file log delta table
file_log_tbl = DeltaTable.forPath(spark, file_log_path )

#date pattern for time related columns
if 'pinda' in input_path:
  if '2019' in input_path:
    input_date_pattern='dd/MM/yyyy HH:mm'
  elif '2020' in input_path:
    input_date_pattern='MM/dd/yyyy HH:mm'
elif 'yeongju' in input_path:
  input_date_pattern='yyyy-MM-dd HH:mm'

default_date = '0001-01-01 00:00:00'

#define required columns for coil index data table
mes_columns = [ 'COIL_ID',
                'PARENT_COIL_ID',
                'OPERATION_SEQUENCE_CODE',
                'OPERATION_SEQUENCE_DESC',
                'OPERATION_START_DATE',
                'OPERATION_END_DATE',
                'MACHINE_CENTER_CODE',
                'ALLOY_CODE']

#define schema for data frame
schema = StructType([StructField('COIL_ID',StringType(),True),
                     StructField('PARENT_COIL_ID',StringType(),True),
                     StructField('OPERATION_SEQUENCE_CODE',IntegerType(),True),
                     StructField('OPERATION_SEQUENCE_DESC',StringType(),False), 
                     StructField('OPERATION_START_DATE',StringType(),True), 
                     StructField('OPERATION_END_DATE',StringType(),True), 
                     StructField('MACHINE_CENTER_CODE',StringType(),True), 
                     StructField('ALLOY_CODE',StringType(),True)])

# COMMAND ----------

#get plant code from plant index table
mes_pinda_plant_name = 'pinda'
mes_oswego_plant_name = 'oswego'
mes_yeongju_plant_name = 'yeongju'
mes_other_plant_name = 'others'

sap_pinda_plant_name = 'PINDAMONHANGABA'
sap_oswego_plant_name = 'NOVELIS CORP: OSWEGO'
sap_yeongju_plant_name = 'NOVELIS YEONGJU'
sap_other_plant_name = 'OTHERS'

if mes_pinda_plant_name in (base_path+input_path).split('/'):
  plant = sap_pinda_plant_name
elif mes_oswego_plant_name in (base_path+input_path).split('/'):
  plant = sap_oswego_plant_name
elif mes_yeongju_plant_name in (base_path+input_path).split('/'):
  plant = sap_yeongju_plant_name
else:
  plant = sap_other_plant_name
  
#read plant index data
plant_index_df = spark.read.format('csv').options(header='true', inferSchema='true').load(plant_index_input_path) 

#get plant code
plant_code = plant_index_df.filter(plant_index_df['src_plant_name']==plant).select(plant_index_df['src_plant_cd']).collect()[0][0]

# COMMAND ----------

# MAGIC %md Read data into dataframe

# COMMAND ----------

#read data into a data frame with selected columns
try:
  df = spark.read.format('csv').options(header = True).schema(schema).load(base_path+input_path).select(mes_columns)
except:
  file_log_tbl.update(col("file_name") == file_name,{"error_code": lit('File not found')})
  dbutils.notebook.exit('File not found')

# drop the rows with all null values
df =df.na.drop(how='all')

#check for data in the file 
if df.count()==0:
  file_log_tbl.update(col("file_name") == file_name,{"error_code": lit('Bad file')})
  dbutils.notebook.exit('Bad file')

# COMMAND ----------

#convert columns name from upper to lower case 
df = df.toDF(*[x.lower() for x in df.columns])

#convert operation start and end date from string type  to timestamp format
df = df\
  .withColumn('operation_start_date', to_timestamp(df['operation_start_date'],  input_date_pattern))\
  .withColumn('operation_end_date',   to_timestamp(df['operation_end_date'],  input_date_pattern))

#create a new column to find out the coil intialized date for every machine center
df = df.withColumn('coil_init_date',F.when(least('operation_start_date','operation_end_date').isNull(),greatest('operation_start_date','operation_end_date')).otherwise(least('operation_start_date','operation_end_date')))

#assign coil init date for null values in timestamp columns
df = df.withColumn('operation_start_date',F.when(col('operation_start_date').isNull(),col('coil_init_date')).otherwise(col('operation_start_date')))\
        .withColumn('operation_end_date',F.when(col('operation_end_date').isNull(),col('coil_init_date')).otherwise(col('operation_end_date')))\
        .drop('coil_init_date')

#create a new column having year values
df = df.withColumn('year',year(col('operation_start_date')))

#create new columns with plant code, metal code and metal code time stamp
df = df\
  .withColumn('plant_code', lit(plant_code).cast('int'))\
  .withColumn('metal_code', lit(1000000000000000).cast('double'))\
  .withColumn('metal_code_timestamp',expr('operation_start_date'))

# COMMAND ----------

#check and update the metal code value for a coil id which has a generated metal code in previous runs
try:
  #create a dataframe from mes delta table data
  mes_old_df =spark.sql('select distinct parent_coil_id , metal_code , metal_code_timestamp,plant_code,year from '+trn_db+'.'+table_name)
except:
  mes_new_df =df  
else:  
  #join mes new data and old data 
  mes_new_df = df.join(mes_old_df,(df.parent_coil_id==mes_old_df.parent_coil_id) & (df.plant_code==mes_old_df.plant_code) &(df.year==mes_old_df.year),how='left').select(df['*'],mes_old_df['metal_code'].alias('mc'),mes_old_df['metal_code_timestamp'].alias('mct'))

  #assign metal_code values to coil id which already has got generated metal code in previous runs 
  mes_new_df = mes_new_df.withColumn('metal_code_timestamp',F.when(col('metal_code')<col('mc'),lit(col('mct'))).otherwise(col('metal_code_timestamp')))\
                         .withColumn('metal_code',F.when(col('metal_code')<col('mc'),lit(col('mc'))).otherwise(col('metal_code')))
  mes_new_df=mes_new_df.drop('mc','mct')
  mes_new_df =mes_new_df.drop_duplicates()

# COMMAND ----------

# MAGIC %md Write data into delta table

# COMMAND ----------

#store mes data frame in transform folder as a delta table
mes_new_df.repartition(1).write.format("delta").mode("append").option('path',destination_path).saveAsTable(trn_db+'.'+table_name)

#update is read column in file log delta table
file_log_tbl.update(col("file_name") == file_name,{"is_read": lit(1)}) # update is_read column in file_log table