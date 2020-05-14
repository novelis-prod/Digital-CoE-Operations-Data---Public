# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC 
# MAGIC This notebook is to read raw data of rolling machine centers from landing zone and store it as delta tables in enterprise zone.
# MAGIC 
# MAGIC • Read the data of rolling machine center files from file log delta table and drop the rows having null values in every column
# MAGIC 
# MAGIC • Update the file log table if any errors raised while reading data into dataframe and change the column names from xx//yy//zz//abc to 'abc'
# MAGIC 
# MAGIC • Update the coilid column values with coilid value that has highest number of occurences in the data (maximum count value) 
# MAGIC 
# MAGIC • Add columns which has filename,plantcode,metalcode(default),year,month,day and sync length(default) in each of files
# MAGIC 
# MAGIC • Check the new schema and update file log table error code column if that doesn't match with old delta table schema
# MAGIC 
# MAGIC • Store the data as delta table with partition of year, month ,day and coil id in enterprise zone and update file log table is read column value to 1
# MAGIC 
# MAGIC Input: Unprocessed ( is_read = 0) files from file log table
# MAGIC 
# MAGIC Output: Append data to respective delta table

# COMMAND ----------

# DBTITLE 1,Connect adls to databricks
# MAGIC %run config/secret_gen2

# COMMAND ----------

# MAGIC %md Define parameters

# COMMAND ----------

#import libraries to perform functional operations
from pyspark.sql.functions import input_file_name
from collections import Counter
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from delta.tables import *

#details of delta table database
env = 'prod'
ent_db ='opsentprodg2'
spark.sql('create database if not exists '+ent_db)

#define file log delta table details
trn_db = 'opstrnprodg2'
file_log = 'file_log_time'
fl_filename = 'file_name'
machine_center_seq = 'machine_center_seq'

#details of folder locations
store_name = "novelisadlsg2"
container = "plantdata"

#input file path
input_path = dbutils.widgets.get('input_file')
print(input_path)
file_name = input_path.split('/')[-1]

#base path to read data 
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

#input path to read plant index table
plant_index_input_path = base_path +'transform/master_tables/plant_index.csv'

# input path for file log delta table
file_log_path= base_path+'transform/'+env+'/file_log_time'

#delta table of file log
file_log_tbl = DeltaTable.forPath(spark, file_log_path )

# define metal code defualt value
metal_code_default = 1000000000000000

# COMMAND ----------

# MAGIC %md Get plant and machine center details

# COMMAND ----------

#define  plant shortcuts
plant_abb = {'pinda':'pba', 'yeongju':'yej', 'sierre':'sie'}

#get machine center from input path
if 'hrm' in input_path:
  machine_center = 'hrm'
elif 'hfm' in input_path:
  machine_center = 'hfm'
elif 'cm1' in input_path:
  machine_center = 'cm1'
elif 'cm2' in input_path:
  machine_center = 'cm2'
elif 'cm3' in input_path:
  machine_center = 'cm3'
elif 'tcm3' in input_path:
  machine_center = 'cm3'
else:
  pass

#get plant name from input path
plant_name = input_path.split('/')[1]

#destination path
destination_path   = base_path +'/'+'enterprise'+ '/' + env + '/' + plant_name + '/' +'material_time'+'/'+ machine_center

#table_name
table_name = plant_abb.get(plant_name) +'_'+machine_center+'_time'

#define coilid column
if machine_center in ('cm1','cm2','cm3'):
  coil_id ='cg_Gen_CoilID'
else:
  coil_id ='cg_Gen_Coilid'

# COMMAND ----------

# MAGIC %md Get plant code from plant index data

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

#read data from files into a data frame
try:
  df = spark.read.format('parquet').options(header=True,inferSchema=True).load(base_path+input_path)
except:
  file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"error_code": lit('No file found')})
  dbutils.notebook.exit('No file found')

#check for data in the file 
if df.count()==0:
  file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"error_code": lit('Bad file')})
  dbutils.notebook.exit('Bad file')

#change column names
df = df.toDF(*[x.split('\\')[-1] for x in df.columns])

#test whether coilid column is in data or not
if coil_id not in df.columns:
  file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"error_code": lit('No Coilid Column')})
  dbutils.notebook.exit('No Coilid')

#delete the recrods with all null values
df = df.na.drop(how='all')

# COMMAND ----------

# MAGIC %md Update Coil id column values

# COMMAND ----------

try:
  #get coil id which has maximum count 
  coil_id_max = df.where(col(coil_id)!='NaN').groupBy(coil_id).count().orderBy(desc('count')).first()[0]
except:
  pass
else:
  #check for coilid values
  if coil_id_max < 1000:
    file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"error_code": lit('Incorrect coilid value')})
    dbutils.notebook.exit('Incorrect Coilid Value')
  
  #check for coilid values
  if coil_id_max == 0 or str(coil_id_max) == 'nan':
    file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"error_code": lit('No value in coilid')})
    dbutils.notebook.exit('No Coilid Value')

  #update coil id column with coli id max
  df = df.withColumn(coil_id,F.when(col(coil_id)!=coil_id_max,lit(coil_id_max)).otherwise(col(coil_id)).cast('float'))

# COMMAND ----------

# MAGIC %md Add new columns to data frame 

# COMMAND ----------

#add additional columns to the dataframe
x=input_path.split('/')
df =df.withColumn('filename',lit((x[-1].split('.'))[0]))\
      .withColumn('metal_code', lit(metal_code_default))\
      .withColumn('plant_code', lit(plant_code))\
      .withColumn('year', lit(x[-4]).cast('integer'))\
      .withColumn('month', lit(x[-3]).cast('integer'))\
      .withColumn('day', lit(x[-2]).cast('integer'))

#convert time column data type to timestamp
df =df.withColumn('Time', col('Time').cast('timestamp'))

# COMMAND ----------

# MAGIC %md Validate the schema before writing into delta tables

# COMMAND ----------

try:
  #query to get data from HRM/HFM delta table
  query = 'select * from ' + ent_db +'.'+ table_name + ' limit 1'
  
  #define a dataframe with HRM/HFM delta table data
  df_rec = spark.sql(query)
except:
  pass
else:
  #check if the schema of new df and HRM/HFM df matches or not
  count = 0
  for i in df_rec.columns:
    if i not in df.columns:
      count=count+1
  
  if count>0 or len(df.columns)!=len(df_rec.columns):
    file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"error_code": lit('Schema mismatch')})

# COMMAND ----------

# MAGIC %md Write data into delta table

# COMMAND ----------

#store data into a delta table
df.repartition(1).write.format("delta").mode("append").option('mergeSchema',True)\
  .partitionBy('year','month','day',coil_id).option('path',destination_path).saveAsTable(ent_db+'.'+table_name)

# COMMAND ----------

# MAGIC %md Update file log table 

# COMMAND ----------

#update is read column in file log delta table
file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"is_read": lit(1)})