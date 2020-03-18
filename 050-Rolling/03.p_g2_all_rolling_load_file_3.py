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
# MAGIC • Update some specific column values with zeros for NaN and negative values and round the remaining values to 2 decimal places
# MAGIC 
# MAGIC • Add columns which has filename,plantcode,metalcode(default),year,month,day and sync length(default) in each of files
# MAGIC 
# MAGIC • Check the new schema and update file log table error code column if that doesn't match with old delta table schema
# MAGIC 
# MAGIC • Store the data as delta table with partition of year, month and day in enterprise zone and update file log table is read column value to 1

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

# COMMAND ----------

#details of machine center delta tables
#plant pinda
pba_hrm_tbl = 'pba_hrm'
pba_hfm_tbl = 'pba_hfm'
pba_cm1_tbl = 'pba_cm1'
pba_cm2_tbl = 'pba_cm2'
pba_cm3_tbl = 'pba_cm3'

#plant yeongju
yj_hrm_tbl = 'yj_hrm'
yj_hfm_tbl = 'yj_hfm'
yj_cm1_tbl = 'yj_cm1'
yj_cm2_tbl = 'yj_cm2'
yj_cm3_tbl = 'yj_cm3'


# details of destination folder
#plant pinda
pba_hrm_dest = base_path+'enterprise/' + env +'/pinda/hrm'
pba_hfm_dest = base_path+'enterprise/' + env +'/pinda/hfm'
pba_cm1_dest = base_path+'enterprise/' + env +'/pinda/cm1'
pba_cm2_dest = base_path+'enterprise/' + env +'/pinda/cm2'
pba_cm3_dest = base_path+'enterprise/' + env +'/pinda/cm3'

#plant yeongju
yj_hrm_dest = base_path+'enterprise/' + env +'/yeongju/hrm'
yj_hfm_dest = base_path+'enterprise/' + env +'/yeongju/hfm'
yj_cm1_dest = base_path+'enterprise/' + env +'/yeongju/cm1'
yj_cm2_dest = base_path+'enterprise/' + env +'/yeongju/cm2'
yj_cm3_dest = base_path+'enterprise/' + env +'/yeongju/cm3'

# COMMAND ----------

#destination path to store new data based on machine centers
if 'pinda' in input_path:
  if 'hrm' in input_path:
    destination_path= pba_hrm_dest
    table_name = pba_hrm_tbl
    machine_center = 'HRM'
  elif 'hfm' in input_path:
    destination_path= pba_hfm_dest
    table_name = pba_hfm_tbl
    machine_center = 'HFM'
  if 'cm1' in input_path:
    destination_path= pba_cm1_dest
    table_name = pba_cm1_tbl
    machine_center = 'CM1'
  elif 'cm2' in input_path:
    destination_path= pba_cm2_dest
    table_name = pba_cm2_tbl
    machine_center = 'CM2'
  elif 'cm3' in input_path:
    destination_path= pba_cm3_dest
    table_name = pba_cm3_tbl
    machine_center = 'CM3'
elif 'yeongju' in input_path:
  if 'hrm' in input_path:
    destination_path= yj_hrm_dest
    table_name = yj_hrm_tbl
    machine_center = 'HRM'
  elif 'hfm' in input_path:
    destination_path= yj_hfm_dest
    table_name = yj_hfm_tbl
    machine_center = 'HFM'
  if 'cm1' in input_path:
    destination_path= yj_cm1_dest
    table_name = yj_cm1_tbl
    machine_center = 'CM1'
  elif 'cm2' in input_path:
    destination_path= yj_cm2_dest
    table_name = yj_cm2_tbl
    machine_center = 'CM2'
  elif 'cm3' in input_path:
    destination_path= yj_cm3_dest
    table_name = yj_cm3_tbl
    machine_center = 'CM3'
else:
  pass

#define coilid column
if machine_center in ('CM1','CM2','CM3'):
  coil_id ='cg_Gen_CoilID'
else:
  coil_id ='cg_Gen_Coilid'

# COMMAND ----------

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
  file_log_tbl.update((col('machine_center')==machine_center) & (col("file_name") == file_name),{"error_code": lit('No file found')})
  dbutils.notebook.exit('No file found')

#check for data in the file 
if df.count()==0:
  file_log_tbl.update((col('machine_center')==machine_center) & (col("file_name") == file_name),{"error_code": lit('Bad file')})
  dbutils.notebook.exit('Bad file')

#change column names
df = df.toDF(*[x.split('\\')[-1] for x in df.columns])

#test whether coilid column is in data or not
if coil_id not in df.columns:
  file_log_tbl.update((col('machine_center')==machine_center) & (col("file_name") == file_name),{"error_code": lit('No Coilid Column')})
  dbutils.notebook.exit('No Coilid')

#delete the recrods with all null values
df = df.na.drop(how='all')

# COMMAND ----------

#get coil id which has maximum count
coil_id_max = df.groupBy(coil_id).count().orderBy(desc('count')).first()[0]

#check for coilid values
if coil_id_max < 1000:
  file_log_tbl.update((col('machine_center')==machine_center) & (col("file_name") == file_name),{"error_code": lit('Incorrect coilid value')})
  dbutils.notebook.exit('Incorrect Coilid Value')
  
#check for coilid values
if coil_id_max == 0 or str(coil_id_max) == 'nan':
  file_log_tbl.update((col('machine_center')==machine_center) & (col("file_name") == file_name),{"error_code": lit('No value in coilid')})
  dbutils.notebook.exit('No Coilid Value')

#update coil id column with coli id max
df = df.withColumn(coil_id,F.when(col(coil_id)!=coil_id_max,lit(coil_id_max)).otherwise(col(coil_id)).cast('float'))

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
      .withColumn('sync_length',lit(0.0).cast('double'))\
      .withColumn('machine_center_seq',lit(seq_num).cast('integer'))\
      .withColumn('data_updated',lit(0))

#convert time column data type to timestamp
df =df.withColumn('Time', col('Time').cast('timestamp'))

#convert the column datatypes to make schema compatible 
if machine_center == 'HFM':
  df = df.withColumn('dg_TensionReel_Loaded',col('dg_TensionReel_Loaded').cast('boolean'))
elif machine_center =='CM3':
  df =df.withColumn('dg_ST2_AFC_Control_Enable', col('dg_ST2_AFC_Control_Enable').cast('boolean'))             
else:
  pass

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
    file_log_tbl.update((col('machine_center')==machine_center) & (col("file_name") == file_name),{"error_code": lit('Schema mismatch')})
    dbutils.notebook.exit('Schema mismatch')

# COMMAND ----------

# MAGIC %md Write data into delta table

# COMMAND ----------

#store data into a delta table
df.repartition(1).write.format("delta").mode("append").partitionBy('year','month','day',coil_id).option('path',destination_path).saveAsTable(ent_db+'.'+table_name)

# COMMAND ----------

#update is read column in file log delta table
file_log_tbl.update((col('machine_center')==machine_center) & (col("file_name") == file_name),{"is_read": lit(1)})