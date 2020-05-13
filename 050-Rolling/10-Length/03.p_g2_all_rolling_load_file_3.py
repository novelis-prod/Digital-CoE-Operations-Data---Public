# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC 
# MAGIC This notebook is to read data of rolling machine centers from landing zone and store it as delta tables in enterprise zone.
# MAGIC 
# MAGIC • Read the data of rolling machine center files from file log delta table and drop the rows having null values in every column
# MAGIC 
# MAGIC • Update the file log table if any errors raised while reading data into dataframe and change the column names from xx//yy//zz//abc to 'abc'
# MAGIC 
# MAGIC • Update the coilid column values with coilid value that has highest number of occurences in the data (maximum count value)
# MAGIC 
# MAGIC • Check coilid column values to ensure that they are not decimals
# MAGIC 
# MAGIC • Update the alloyid column values with coilid value that has highest number of occurences in the data (maximum count value)
# MAGIC 
# MAGIC • Update some specific column values with zeros for NaN and negative values and round the remaining values to 2 decimal places
# MAGIC 
# MAGIC • Add columns: filename,plantcode,metalcode(default),year,month,day and sync length(default) in each of files
# MAGIC 
# MAGIC • Check the new schema and update file log table error code column if that doesn't match with old delta table schema
# MAGIC 
# MAGIC • Store the data as delta table with partition of year, month and day in enterprise zone and update file log table is read column value to 1
# MAGIC 
# MAGIC Input: IBA files from file log table
# MAGIC 
# MAGIC Output: Store data into delta tables

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
file_log = 'file_log'
fl_filename = 'file_name'
machine_center_seq = 'machine_center_seq'

#details of folder locations
store_name = "novelisadlsg2"
container = "plantdata"

#input file path
input_path = dbutils.widgets.get("input_file")
print(input_path)
file_name = input_path.split('/')[-1]

#base path to read data 
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

#input path to read plant index table
plant_index_input_path = base_path +'transform/master_tables/plant_index.csv'

# input path for file log delta table
file_log_path= base_path+'transform/'+env+'/file_log'

#delta table of file log
file_log_tbl = DeltaTable.forPath(spark, file_log_path )

# define metal code defualt value
metal_code_default = 1000000000000000

#get seq_num from file log table
seq_num=spark.sql("SELECT {} FROM {}.{} WHERE {}={}".format(machine_center_seq,trn_db,file_log,fl_filename,'"'+file_name+'"')).collect()[0][0]

# define columns to validate 
DE_cols        = ['eg_LengthIndex',
                  'eg_ReverseLengthIndex',
                  'eRg_EntryHeadDiscard',
                  'eRg_EntryTailDiscard',
                  'eRg_ExitHeadDiscard',
                  'eRg_ExitTailDiscard',
                  'eRg_EntryRolledLength',
                  'eRg_ExitRolledLength',
                  'eRg_Ratio']

# COMMAND ----------

# MAGIC %md Get plant  and machine center details

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
destination_path   = base_path +'/enterprise'+ '/' + env + '/' + plant_name + '/' + machine_center

#table_name
table_name = plant_abb.get(plant_name) +'_'+machine_center

#define alloyid column
alloy_id = 'cg_Gen_Alloy'
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

# dropping duplicates
df = df.drop_duplicates()

# COMMAND ----------

# MAGIC %md Update Coil id column values

# COMMAND ----------

#get coil id which has maximum count
try:
  from pyspark.sql.functions import desc, col
  coil_id_max = df.select(coil_id).na.drop(how="all").groupby(coil_id).count().sort(desc('count')).first()[0]
  #coil_id_max = df.where(col(coil_id)<>'NaN').groupBy(coil_id).count().orderBy(desc('count')).first()[0]
except:
  file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"error_code": lit('No value in coilid')})
  dbutils.notebook.exit('No Coilid Value')
  
#check for coilid values
b = str(coil_id_max).split('.')  # splitting the number on decimal
c = int(b[-1]) # getting the value after decimal and converting it into decimal

if ((coil_id_max < 1000) and (c>0)): # checking if coil_id_max is less than 1000 or decimal value is greater than 0, then it is incorrect coil_id values
  file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"error_code": lit('Incorrect coilid value')})
  dbutils.notebook.exit('Incorrect Coilid Value')

#update coil id column with coli id max
df = df.withColumn(coil_id,F.when(col(coil_id)!=coil_id_max,lit(coil_id_max)).otherwise(col(coil_id)).cast('float'))

#update alloy id column with alloy id max
alloy_id_max = df.select(alloy_id).na.drop(how="all").groupBy(alloy_id).count().orderBy('count', ascending=False).first()[0]
df = df.withColumn(alloy_id, F.when(col(alloy_id)!=alloy_id_max,lit(alloy_id_max)).otherwise(col(alloy_id)))

# COMMAND ----------

# MAGIC %md Add new columns to data frame 

# COMMAND ----------

#assign 0 to negative and NaN values for columns
try:
  for i in DE_cols:
    df = df.withColumn(i,F.when(col(i)<0,lit(0)).otherwise(F.round(col(i),2)))\
           .withColumn(i,F.when(isnan(col(i)),lit(0)).otherwise(F.round(col(i),2)))
except:
  file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"error_code": lit('Columns missing')})
  dbutils.notebook.exit('Columns missing')    

#add additional columns to the dataframe
x=input_path.split('/')
df =df.withColumn('filename',lit((x[-1].split('.'))[0]))\
      .withColumn('metal_code', lit(metal_code_default))\
      .withColumn('plant_code', lit(plant_code))\
      .withColumn('year', lit(x[-4]).cast('integer'))\
      .withColumn('month', lit(x[-3]).cast('integer'))\
      .withColumn('day', lit(x[-2]).cast('integer'))\
      .withColumn('sync_length',lit(0.0).cast('double'))\
      .withColumn('machine_center_seq',lit(seq_num).cast('integer'))\
      .withColumn('data_updated',lit(0))

#convert time column data type to timestamp
df =df.withColumn('Time', col('Time').cast('timestamp'))

#convert the column datatypes to make schema compatible 
if machine_center.upper() == 'HFM' :
  df = df.withColumn('dg_TensionReel_Loaded',col('dg_TensionReel_Loaded').cast('boolean'))
elif machine_center.upper() =='CM3':
  df =df.withColumn('dg_ST2_AFC_Control_Enable', col('dg_ST2_AFC_Control_Enable').cast('boolean'))     
elif machine_center.upper() == 'CM1':
  df=df.withColumnRenamed(coil_id, "cg_Gen_Coilid")
  coil_id = "cg_Gen_Coilid"
  
else:
  pass

# COMMAND ----------

# MAGIC %md Validate the schema and write data into delta table

# COMMAND ----------

#validate the data frame schema with detla table schema
try:
  #query to get data from delta table
  query = 'select * from ' + ent_db +'.'+ table_name + ' limit 1'

  #define a dataframe with  delta table data
  df_rec = spark.sql(query)
except:
  pass
else:
  #check if the schema of new df and  df matches or not
  count = 0
  for i in df_rec.columns:
    if i not in df.columns:
      count=count+1
  #update file log table if df schema didn't match with delta table schema
  if count>0 or len(df.columns)!=len(df_rec.columns):
    file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"error_code": lit('Schema mismatch')})
      
#store data into a delta table
df.repartition(1).write.format("delta").mode("append")\
    .option('mergeSchema',True)\
    .partitionBy('year','month','day',coil_id)\
    .option('path',destination_path).saveAsTable(ent_db+'.'+table_name)  

# COMMAND ----------

# MAGIC %md Update file log table 

# COMMAND ----------

#update is read column in file log delta table
file_log_tbl.update((col('machine_center')==machine_center.upper()) & (col("file_name") == file_name),{"is_read": lit(1)})