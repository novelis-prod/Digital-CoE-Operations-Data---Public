# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook is to read data from mes delta table and create coil index table in enterprise folder.
# MAGIC 
# MAGIC • Read data from mes data where metal code has default(1000000000000000) value
# MAGIC 
# MAGIC • Create a coil index table with identified columns(parent_coil_id as coil_id,alloy_code,coil_init_date,plant_code,metal_code,metal_code_timestamp)
# MAGIC 
# MAGIC • Generate metal code values for each unique parent coil id and create a timestamp for every metal code generated
# MAGIC 
# MAGIC • Store coil index table in enterprise folder as a delta table
# MAGIC 
# MAGIC • Update metal code values and timestamp values for each parent coil id in mes delta table
# MAGIC 
# MAGIC • Update coil index table for every x minutes to append the new data

# COMMAND ----------

# MAGIC %run /config/secret

# COMMAND ----------

#define details of database connection
DL = 'daa'
adl = connection(DL)

# COMMAND ----------

#import libraries for performing functional operations
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from pyspark.sql.functions import max,first,col
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import Row
from delta.tables import *
import pandas as pd
import datetime

#define data storage table details of coil index table
env = 'prod'
DB = 'opsentprod'
spark.sql('create database if not exists '+DB)
tbl_dev_coil_index = 'coil_index'

#define mes delta table details
trn_db = 'opstrnprod'
mes_table_name = 'mes'
metal_code_default = '1000000000000000'

#define mes delta table details
column_name='auto_number'
auto_increment_tbl = 'auto_increment'

#define variables to access columns in mes tables
parent_coil_id = 'parent_coil_id'
alloy_code = 'alloy_code'
plant_code = 'plant_code'
default_date = '0001-01-01 00:00:00'

#schema to create an empty dataset
schema = StructType([StructField('coil_id',StringType(),True), StructField('plant_code',IntegerType(),True), StructField('alloy_code',StringType(),True),StructField('coil_init_date',TimestampType(),True),StructField('metal_code',LongType(),False), StructField('metal_code_timestamp',TimestampType(),False)])

#input path for mes delta table
mes_tbl_input_path = 'adl://novelisadls.azuredatalakestore.net/transform/' + env +'/mes'

#destination path for storing coil index table in transform folder
coil_index_destination_path ='adl://novelisadls.azuredatalakestore.net/enterprise/' + env + '/coil_index' 

# COMMAND ----------

#query to get rows having default metal code values from mes dev delta table
query = 'select * from '+ trn_db+'.'+ mes_table_name+' where metal_code =' + metal_code_default

#reading mes data into a dataframe
mes_df = spark.sql(query)

if mes_df.count() ==0:
  dbutils.notebook.exit('No files')

# COMMAND ----------

#create df with min operation start date and end date and max alloy code for a particular parent coil id and plant code
coil_index_df = mes_df.groupBy(parent_coil_id,plant_code).agg({'alloy_code':'max','operation_start_date': 'min', 'operation_end_date':'min'}).toDF('coil_id', 'plant_code', 'min_start_date', 'alloy_code', 'min_end_date')

#assign min value in between operation start and end date to new col coil_init_date
coil_index_df = coil_index_df.withColumn('coil_init_date',least('min_start_date','min_end_date'))\
                             .drop('min_start_date')\
                             .drop('min_end_date')

#assing default date to coil init date column for null values
coil_index_df = coil_index_df.withColumn("coil_init_date", F.when(F.col("coil_init_date").isNull(), default_date).otherwise(F.col("coil_init_date")))
coil_index_df = coil_index_df.withColumn('coil_init_date',col('coil_init_date').cast('timestamp'))

# COMMAND ----------

#generate metal codes for coil ids in coil index table
increment_number_init = spark.sql("SELECT {} FROM {}.{}".format(column_name, trn_db, auto_increment_tbl)).collect()[0][0] # get max number from auto increment table
increment_number_final = increment_number_init+coil_index_df.count()
increment_number_list = [x for x in range(increment_number_init, increment_number_final)]

metal_code_df = pd.DataFrame(increment_number_list,columns=['metal_code'],dtype='int')
metal_code_df['metal_code_timestamp']= datetime.datetime.now()

#convert spark dataframe into pandas dataframe
coil_index_df1 = coil_index_df.toPandas()
coil_index_updated_df = pd.concat([coil_index_df1, metal_code_df], axis=1)
coil_index_new_df = spark.createDataFrame(coil_index_updated_df,schema=schema)

# COMMAND ----------

#store coil index  data in enterprise folder
coil_index_new_df.write.format('delta').mode('append').option('path',coil_index_destination_path).saveAsTable(DB+'.'+tbl_dev_coil_index)

# COMMAND ----------

#update increment table with final number
spark.sql("UPDATE {}.{} SET {}={} WHERE {}={}".format(trn_db, auto_increment_tbl, column_name, increment_number_final, column_name, increment_number_init))

#update metal code and metal code timestamp in mes table
mes_tbl = DeltaTable.forPath(spark, mes_tbl_input_path)

mes_tbl.alias("mes").merge(
    coil_index_new_df.alias("coil_index"),
    "mes.plant_code = coil_index.plant_code and mes.parent_coil_id = coil_index.coil_id")\
  .whenMatchedUpdate(set = { "mes.metal_code" : "coil_index.metal_code","mes.metal_code_timestamp":"coil_index.metal_code_timestamp" })\
  .execute()