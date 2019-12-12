# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC 
# MAGIC This notebook is to read raw data of CM1/CM2/CM3 machine centers from landing zone and store it as delta tables in enterprise zone.
# MAGIC 
# MAGIC • Read the data of CM1/CM2/CM3 machine center files from file log delta table and drop the rows having null values in every column
# MAGIC 
# MAGIC • Update the file log table if any errors raised while reading data into dataframe and change the column names from xx//yy//zz//abc to 'abc'
# MAGIC 
# MAGIC • Update the coilid column values with coilid value that has highest number of occurences in the data (maximum count value) 
# MAGIC 
# MAGIC • Update some specific column values with zeros for NaN and negative values and round the remaining values to 2 decimal places
# MAGIC 
# MAGIC • Add columns which has filename,plantcode,metalcode(default),year,month,day and sync length(default) in each of files
# MAGIC 
# MAGIC • Check the new schema and update file log table error code column if that doesn't match with old delta table schema
# MAGIC 
# MAGIC • Store the data as delta table with partition of year, month and day in enterprise zone and update file log table is read column value to 1

# COMMAND ----------

# MAGIC %run config/secret

# COMMAND ----------

#import libraries to perform functional operations
from pyspark.sql.functions import input_file_name
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from delta.tables import *

#details of database connection
DL = 'daa'
adl = connection(DL)

#details of delta table database
env = 'prod'
DB='opsentprod'
spark.sql('create database if not exists '+DB)

#input path to read data for MES qa table
Base = 'adl://novelisadls.azuredatalakestore.net/'
input_path = dbutils.widgets.get("input_file")
print(input_path)
#input path to read plant index table
plant_index_input_path = "adl://novelisadls.azuredatalakestore.net/transform/master_tables/plant_index.csv"

#define metal code defualt value
metal_code_default = 1000000000000000

#details of plant and table names
plant_name = 'pinda'
cm1_tbl = 'pba_cm1'
cm2_tbl = 'pba_cm2'
cm3_tbl = 'pba_cm3'

#details of destination folder
cm1_dest = 'adl://novelisadls.azuredatalakestore.net/enterprise/'+ env +'/'+ plant_name + '/cm1'
cm2_dest = 'adl://novelisadls.azuredatalakestore.net/enterprise/'+ env +'/'+ plant_name + '/cm2'
cm3_dest = 'adl://novelisadls.azuredatalakestore.net/enterprise/'+ env +'/'+ plant_name + '/cm3'

#input path for file log delta table
tbl_file_log_path='adl://novelisadls.azuredatalakestore.net/transform/'+ env +'/file_log'

#file log delta table
file_log_tbl = DeltaTable.forPath(spark, tbl_file_log_path )

# COMMAND ----------

#destination path to store new data based on machine centers
if 'cm1' in input_path:
  destination_path= cm1_dest
  table_pinda_cm = cm1_tbl
  coil_id ='cg_Gen_CoilID'
elif 'cm2' in input_path:
  destination_path= cm2_dest
  table_pinda_cm = cm2_tbl
  coil_id = 'cg_Gen_CoilID'
elif 'cm3' in input_path:
  destination_path= cm3_dest
  table_pinda_cm = cm3_tbl
  coil_id = 'cg_Gen_CoilID'
else:
  pass

# define columns
columns_to_cln = ['eg_LengthIndex',
                  'eg_ReverseLengthIndex',
                  'eRg_EntryThickness',
                  'eRg_ExitThickness',
                  'eRg_EntryHeadDiscard',
                  'eRg_EntryTailDiscard',
                  'eRg_ExitHeadDiscard',
                  'eRg_ExitTailDiscard',
                  'eRg_EntryRolledLength',
                  'eRg_ExitRolledLength',
                  'eRg_Ratio']

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

if mes_pinda_plant_name in (Base+input_path).split('/'):
  plant = sap_pinda_plant_name
elif mes_oswego_plant_name in (Base+input_path).split('/'):
  plant = sap_oswego_plant_name
elif mes_yeongju_plant_name in (Base+input_path).split('/'):
  plant = sap_yeongju_plant_name
else:
  plant = sap_other_plant_name
  
#read plant index data
plant_index_df = spark.read.format('csv').options(header='true', inferSchema='true').load(plant_index_input_path) 

#get plant code
plant_code = plant_index_df.filter(plant_index_df['src_plant_name']==plant).select(plant_index_df['src_plant_cd']).collect()[0][0]

# COMMAND ----------

#read data from files into a data frame
try:
  df = spark.read.format('parquet').options(header=True,inferSchema=True).load(Base+input_path)
except:
  file_log_tbl.update(col("file_path") ==input_path,{"error_code": lit('No file found')})
  dbutils.notebook.exit('No file found')

#check for data in the file 
if df.count()==0:
  file_log_tbl.update(col("file_path") ==input_path,{"error_code": lit('Bad file')})
  dbutils.notebook.exit('Bad file')

#change column names
df = df.toDF(*[x.split('\\')[-1] for x in df.columns])

#test whether coilid column is in data or not
if coil_id not in df.columns:
  file_log_tbl.update(col("file_path") ==input_path,{"error_code": lit('No Coilid Column')})
  dbutils.notebook.exit('No Coilid')

#delete the recrods with all null values
df = df.na.drop(how='all')

# COMMAND ----------

#get coil id which has maximum count
coil_id_max = df.groupBy(coil_id).count().orderBy(desc('count')).select(coil_id).collect()[0][0]

#check for coilid values
if coil_id_max == 0 or str(coil_id_max) == 'nan':
  file_log_tbl.update(col("file_path") ==input_path,{"error_code": lit('No value in coilid')})
  dbutils.notebook.exit('No Coilid Value')
  
#update coil id column 
df = df.withColumn(coil_id,F.when(col(coil_id)!=coil_id_max,lit(coil_id_max)).otherwise(col(coil_id)).cast('int'))

# COMMAND ----------

#assign 0 to negative and NaN values for columns
for i in columns_to_cln:
  df = df.withColumn(i,F.when(col(i)<0,lit(0)).otherwise(F.round(col(i),2)))\
       .withColumn(i,F.when(isnan(col(i)),lit(0)).otherwise(F.round(col(i),2)))
       
#add additional columns to the dataframe
x=input_path.split('/')
df =df.withColumn('filename',lit((x[-1].split('.'))[0]))\
      .withColumn('metal_code', lit(metal_code_default))\
      .withColumn('plant_code', lit(plant_code))\
      .withColumn('year', lit(x[-4]).cast('integer'))\
      .withColumn('month', lit(x[-3]).cast('integer'))\
      .withColumn('day', lit(x[-2]).cast('integer'))\
      .withColumn('sync_length',lit(0.0).cast('double'))

#convert time column data type to timestamp
cm_df =df.withColumn('Time', col('Time').cast('timestamp'))

# COMMAND ----------

try:
  #query to get data from HRM/HFM delta table
  query = 'select * from ' + DB +'.'+ table_pinda_cm + ' limit 1'

  #define a dataframe with HRM/HFM delta table data
  cm_df_rec = spark.sql(query)
except:
  pass
else:
  #check if the schema of new df and HRM/HFM df matches or not
  count = 0
  for i in cm_df_rec.columns:
    if i not in cm_df.columns:
      count=count+1
  
  try:
    if count>1 and len(cm_df.columns)!=len(cm_df_rec.columns):
      file_log_tbl.update(col("file_path") == input_path,{"error_code": lit('Schema mismatch')})
      dbutils.notebook.exit('Schema mismatch')
  except:
    print('error in updating file log table')

# COMMAND ----------

#store data into a delta table
cm_df.repartition(1).write.format("delta").mode("append").partitionBy('year','month','day').option('path',destination_path).saveAsTable(DB+'.'+table_pinda_cm)

# COMMAND ----------

#update is read column in file log delta table
try:
  file_log_tbl.update(col("file_path") == input_path,{"is_read": lit(1)}) 
except:
  file_log_tbl.update(col("file_path") == input_path,{"is_read": lit(1)}) 

# COMMAND ----------

#%sql OPTIMIZE tbl_pinda_cm ZORDER by cg_Gen_Coilid