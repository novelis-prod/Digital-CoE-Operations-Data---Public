# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook is to store the file path of all the files dropped in the landing zone. This will have path and an additional column to read the files. 
# MAGIC 
# MAGIC • Read the newly uploaded files from landing zone and store it in file log delta table in transform zone 
# MAGIC 
# MAGIC • Extract file name, file path, plant name, machine center name, coil id etc. information from file path
# MAGIC 
# MAGIC • Store this data into a delta table in transform zone 
# MAGIC 
# MAGIC Input to this notebook: Details of latest uploaded files in landing zone 
# MAGIC 
# MAGIC Output of this notebook: Append to the existing delta table with new data in transform zone

# COMMAND ----------

# DBTITLE 1,Connect ADLS Gen2 to Databricks
# MAGIC %run /config/secret_gen2

# COMMAND ----------

#define details of store and container name
store_name = "novelisadlsg2"
container = "plantdata"

# COMMAND ----------

#intializing adls gen2 file system 
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://"+container+"@"+store_name+".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

#create file system client to access the files from gen2
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from delta.tables import *

from azure.storage.filedatalake import DataLakeServiceClient
#define service client 
try:  
    global service_client
        
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", store_name), credential=key)
    
except Exception as e:
    print(e)

#file system client
fsc = service_client.get_file_system_client(file_system=container)

# COMMAND ----------

#define variables
env = 'prod'
table_name = 'file_log_time'
trn_db = 'opstrnprodg2'

#creating a database
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(trn_db))

file_format_par = '.parquet'
file_format_csv = '.csv'

#base path for adls gen2
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

#input path for machine center iba files
raw_path_par = ['landing/pinda/iba/standard/material_time/']

#destination path for file log delta table
destination_path = base_path + 'transform/' + env + '/file_log_time'

#column names
raw_file_path = 'file_path'
raw_file_name = 'file_name'
raw_file_year = 'year'
raw_file_month= 'month'
raw_file_day  = 'day'
raw_file_source = 'file_source'
raw_machine_center = 'machine_center'
raw_plant_name ='plant_name'
is_read = 'is_read'
error_code = 'error_code'
uploaded_date = 'uploaded_date'
iba= 'IBA'
mes = 'MES'
hrm ='HRM'
hfm ='HFM'
cm1 ='CM1'
cm2 ='CM2'
cm3 ='CM3'

# COMMAND ----------

#get max date in the file log table
"""try:
  max_date = spark.sql('select max({}) from {}.{} '.format(uploaded_date,trn_db,table_name)).collect()[0][0]
except:
  max_date = ''
  """

#define a function to get machine center name  
def machine_center(file):
  if 'hrm' in file:
    return hrm
  elif 'hfm' in file:
    return hfm
  elif 'cm1' in file:
    return cm1
  elif 'cm2' in file:
    return cm2
  elif 'cm3' in file:
    return cm3

# COMMAND ----------

#get all the file path details of parquet files from raw path 
file_paths = []
file_names = []
file_year  = []
file_month = []
file_day   = []
file_source = []
file_plant_name=[]
file_machine_center = []
file_uploaded_date = []
for i in raw_path_par:
  paths = fsc.get_paths(path=i)
  for path in paths:
    if file_format_par in path.name:  
      file_paths.append(path.name)                        #extract file path
      file_plant_name.append(path.name.split('/')[1])     #extract plant name
      file_source.append(path.name.split('/')[2])         #extract file source name
      file_names.append(path.name.split('/')[-1])         #extract file name
      file_year.append(path.name.split('/')[-4])          #extract year
      file_month.append(path.name.split('/')[-3])         #extract month
      file_day.append(path.name.split('/')[-2])           #extract day  
      file_uploaded_date.append(path.last_modified)       #extract uploaded time
      file_machine_center.append(machine_center(path.name))#extract machine center

# COMMAND ----------

#creating a dataframe from two lists and then adding a new column to the dataframe
from pyspark.sql.functions import lit
df = spark.createDataFrame(zip(file_names, file_paths,file_plant_name,file_source,file_machine_center, file_year, file_month, file_day,file_uploaded_date), schema=[raw_file_name, raw_file_path,raw_plant_name,raw_file_source,raw_machine_center,raw_file_year,raw_file_month,raw_file_day,uploaded_date]).withColumn(is_read, lit(0))

# COMMAND ----------

#convert year, month and day to integer type
df = df.withColumn(raw_file_year,col(raw_file_year).cast('integer'))\
       .withColumn(raw_file_month,col(raw_file_month).cast('integer'))\
       .withColumn(raw_file_day,col(raw_file_day).cast('integer'))\
       .withColumn(error_code,lit(None).cast('string'))

#create date column 
df=df.withColumn('proc_date',F.to_timestamp(F.concat(col('year'),col('month'),col('day')), format='yyyyMdd'))

# COMMAND ----------

try:
  #get file_names and plant name from the file_log delta table
  df_read = spark.sql("SELECT {},{} FROM {}.{} WHERE {} in {} ".format(raw_file_name,raw_plant_name,trn_db, table_name,raw_file_source,('iba','mes')))
    
except:
  df_insert = df
else:   
  #filter dataframe to keep files from the append_files_names_list
  df_insert = df.join(df_read,on=[df.file_name == df_read.file_name,df.plant_name==df_read.plant_name], how="left_anti")  

#check for new records
if df_insert.count() ==0:
  dbutils.notebook.exit('No new files')
  
#write the data into a delta table
df_insert.write.format('delta').mode('append').partitionBy('plant_name','machine_center','file_name').option('path',destination_path).saveAsTable((trn_db+'.'+table_name))