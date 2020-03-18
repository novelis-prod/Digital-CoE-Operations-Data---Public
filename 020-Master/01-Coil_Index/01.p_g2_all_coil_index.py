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

# DBTITLE 1,Connect adls to data bricks
# MAGIC %run /config/secret_gen2

# COMMAND ----------

# MAGIC %md Define parameters

# COMMAND ----------

#import libraries for performing functional operations
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from pyspark.sql.functions import max,first,col
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import Row
from delta.tables import *
import pandas as pd
import datetime

#define data storage table details of coil index table
env = 'prod'
ent_db = 'opsentprodg2'
spark.sql('create database if not exists '+ent_db)
coil_index_tbl = 'coil_index'

#define mes delta table details
trn_db = 'opstrnprodg2'
mes_tbl = 'mes'
metal_code_default = '1000000000000000'

#define mes delta table details
column_name='auto_number'
auto_increment = 'auto_increment'

#define details of folders
store_name = "novelisadlsg2"
container = "plantdata"

#details of folder locations
base_path =  "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

#input path for mes delta table
mes_tbl_path = base_path+'transform/' + env +'/mes'

#destination path for storing coil index table in transform folder
destination_path = base_path +'enterprise/' + env + '/coil_index' 

#define variables to access columns in mes tables
parent_coil_id = 'parent_coil_id'
alloy_code = 'alloy_code'
plant_code = 'plant_code'
year = 'year'
coil_num = 'coil_num'
default_date = '0001-01-01 00:00:00'

#schema to create an empty dataset
schema = StructType([StructField('coil_id',StringType(),True),
                     StructField('plant_code',IntegerType(),True),
                     StructField('year',IntegerType(),False),
                     StructField('alloy_code',StringType(),True),
                     StructField('coil_init_date',TimestampType(),True),
                     StructField('metal_code',LongType(),False), 
                     StructField('metal_code_timestamp',TimestampType(),False)])

# COMMAND ----------

# MAGIC %md Read data into dataframe

# COMMAND ----------

#query to get rows having default metal code values from mes dev delta table
query = 'select * from '+ trn_db+'.'+ mes_tbl+' where metal_code =' + metal_code_default+ ' and year >= 2018 and year < 2021'

#reading mes data into a dataframe
mes_df = spark.sql(query).dropna()

if mes_df.count() ==0:
  dbutils.notebook.exit('No files')

# COMMAND ----------

#create df with min operation start date and end date and max alloy code for a particular parent coil id and plant code and coil processed year
coil_index_df = mes_df.groupBy(parent_coil_id,plant_code,year).agg({'alloy_code':'max','operation_start_date': 'min', 'operation_end_date':'min'}).toDF('coil_id', 'plant_code', 'year','min_start_date', 'alloy_code', 'min_end_date')

#assign min value in between operation start and end date to new col coil_init_date
coil_index_df = coil_index_df.withColumn('coil_init_date',least('min_start_date','min_end_date'))\
                             .drop('min_start_date')\
                             .drop('min_end_date')

#assing default date to coil init date column for null values
coil_index_df = coil_index_df.withColumn("coil_init_date", F.when(F.col("coil_init_date").isNull(), default_date).otherwise(F.col("coil_init_date")))
coil_index_df = coil_index_df.withColumn('coil_init_date',col('coil_init_date').cast('timestamp'))

# COMMAND ----------

#define a window  to calculate days difference between same coil id in different year
window = Window.partitionBy('coil_id').orderBy('coil_init_date')

#calculate days difference 
coil_index_df = coil_index_df.withColumn("days_diff", F.datediff(coil_index_df.coil_init_date,F.lag(coil_index_df.coil_init_date, 1).over(window)))

#assign same days diff value to each coil id of different year
coil_index_df = coil_index_df.withColumn('days_diff',F.when(col('days_diff').isNull(),F.lead(coil_index_df.days_diff, 1).over(window)).otherwise(col('days_diff')))

#assign zero to days diff column if it is still null
coil_index_df = coil_index_df.withColumn('days_diff',F.when(col('days_diff').isNull(),lit(0)).otherwise(col('days_diff')))

#define the coil num to each coil id from different year based on days diff (1 if days diff is less than 90 days, otherwise assign row number based on date)
coil_index_df = coil_index_df.withColumn('coil_num',F.when(col('days_diff')<90,lit(1)).otherwise(row_number().over(window)))

# COMMAND ----------

#define a df with records having days diff less than 90 days and take the  min of year and coil init value based on coil id, plant code and coil num
df1 = coil_index_df.where((col('days_diff')<90) | (col('days_diff').isNull())).groupBy('coil_id','plant_code','coil_num').agg({'year':'min','coil_init_date':'min'}).toDF('coil_id', 'plant_code','coil_num','year', 'coil_init_date')

#define a df with records having days diff greater than 90 days
df2 = coil_index_df.where(col('days_diff')>90).select('coil_id', 'plant_code','coil_num','year', 'coil_init_date')

#append both df1 and df2 into single dataframe
df3 = df1.union(df2)

# COMMAND ----------

# MAGIC %md Generate metal code

# COMMAND ----------

#generate metal codes for coil ids in coil index table
increment_number_init = spark.sql("SELECT {} FROM {}.{}".format(column_name, trn_db, auto_increment)).collect()[0][0] # get max number from auto increment table
increment_number_final = increment_number_init+df3.count()
increment_number_list = [x for x in range(increment_number_init, increment_number_final)]

#generate metal code and metal code time stamp
metal_code_df = pd.DataFrame(increment_number_list,columns=['metal_code'],dtype='int')
metal_code_df['metal_code_timestamp']= datetime.datetime.now()

#convert spark dataframe into pandas dataframe
coil_index_df1 = df3.orderBy('year','coil_init_date').toPandas()
coil_index_updated_df = pd.concat([coil_index_df1, metal_code_df], axis=1)
coil_index_updated_df1 = spark.createDataFrame(coil_index_updated_df)

# COMMAND ----------

#add the metal code and metal code timestamp columns to coil index df by joining with coil_index_updated_df1  on coil id and coil num records
coil_index_new_df = coil_index_df.join(coil_index_updated_df1,(coil_index_df.coil_id == coil_index_updated_df1.coil_id) & (coil_index_df.coil_num == coil_index_updated_df1.coil_num),how='inner').select(coil_index_df['*'],coil_index_updated_df1['metal_code'],coil_index_updated_df1['metal_code_timestamp'])

#drop the columns which are not required
coil_index_new_df = coil_index_new_df.drop('coil_num','days_diff')

# COMMAND ----------

# MAGIC %md Write data into delta table

# COMMAND ----------

#store coil index  data in enterprise folder
coil_index_new_df.repartition(1).write.format('delta').mode('append').option('path',destination_path).saveAsTable(ent_db+'.'+coil_index_tbl)

# COMMAND ----------

# MAGIC %md Update metal code in MES delta table

# COMMAND ----------

#update increment table with final number
spark.sql("UPDATE {}.{} SET {}={} WHERE {}={}".format(trn_db, auto_increment, column_name, increment_number_final, column_name, increment_number_init))

#mes delta table
mes_tbl = DeltaTable.forPath(spark, mes_tbl_path)

#update start and end date and year in mes table
mes_tbl.alias("mes").merge(
    coil_index_new_df.alias("coil_index"),
    "mes.plant_code = coil_index.plant_code and mes.parent_coil_id = coil_index.coil_id and mes.operation_start_date = 'null' and mes.operation_end_date = 'null' ")\
  .whenMatchedUpdate(set = { "mes.operation_start_date" : "coil_index.coil_init_date","mes.operation_end_date":"coil_index.coil_init_date","mes.year":"coil_index.year" })\
  .execute()

#update metal code and metal code timestamp in mes table
mes_tbl.alias("mes").merge(
    coil_index_new_df.alias("coil_index"),
    "mes.plant_code = coil_index.plant_code and mes.parent_coil_id = coil_index.coil_id and mes.year = coil_index.year")\
  .whenMatchedUpdate(set = { "mes.metal_code" : "coil_index.metal_code","mes.metal_code_timestamp":"coil_index.metal_code_timestamp" })\
  .execute()