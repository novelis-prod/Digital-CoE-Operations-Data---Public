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
file_system_client = service_client.get_file_system_client(file_system=container)

# COMMAND ----------

#define variables
env = 'prod'
table_name = 'file_log'
trn_db = 'opstrnprodg2'

#creating a database
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(trn_db))

file_format_par = '.parquet'
file_format_csv = '.csv'

#base path for adls gen2
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

#input path for machine center iba files
raw_path_par = ['landing/pinda/iba/standard/material_length','landing/yeongju/iba/standard/material_length/201_hfm1']
#input path for mes files
raw_path_csv = ['landing/pinda/mes','landing/yeongju/mes']

#destination path for file log delta table
destination_path = base_path + 'transform/' + env + '/file_log'

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
processed_number = 'processed_number'
coil_id ='coil_id'
iba= 'iba'
mes = 'MES'
hrm ='HRM'
hfm ='HFM'
cm1 ='CM1'
cm2 ='CM2'
cm3 ='CM3'

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
coil_processed_number = []
file_coilid=[]
for i in raw_path_par:
  paths = file_system_client.get_paths(path=i)
  for path in paths:
    if file_format_par in path.name:  
      file_paths.append(path.name)                 #extract file path
      file_source.append(path.name.split('/')[2])  #extract file source name
      file_names.append(path.name.split('/')[-1])  #extract file name
      file_year.append(path.name.split('/')[-4])   #extract year
      file_month.append(path.name.split('/')[-3])  #extract month
      file_day.append(path.name.split('/')[-2])    #extract day
      if 'pinda' in path.name:
        file_plant_name.append('pinda')            #extract plant name
        if 'hrm' in path.name:
          file_machine_center.append(hrm)          #extract machine center details
          if 'PBA' in path.name:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[-7])  #extract coil processed number
            file_coilid.append(str(int((path.name.split('/')[-1]).split('_')[-6])))  #extract coil id
          else:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[0])
            file_coilid.append(str(int((path.name.split('/')[-1]).split('_')[1])))
        elif 'hfm' in path.name:
          file_machine_center.append(hfm)
          if 'PBA' in path.name:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[-6])
            file_coilid.append(str(int((path.name.split('/')[-1]).split('_')[-5])))
          else:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[-5])
            file_coilid.append(str(int((path.name.split('/')[-1]).split('_')[-4])))
        elif 'cm1' in path.name:
          file_machine_center.append(cm1)
          if 'PBA' in path.name:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[-8])
            file_coilid.append(str(int((path.name.split('/')[-1]).split('_')[-7])))
          else:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[-7]) 
            file_coilid.append((path.name.split('/')[-1]).split('_')[-6])
        elif 'cm2' in path.name:
          file_machine_center.append(cm2)
          if 'PBA' in path.name:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[-9])
            file_coilid.append(str(int((path.name.split('/')[-1]).split('_')[-8])))
          else:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[-7])
            file_coilid.append((path.name.split('/')[-1]).split('_')[-6])
        elif 'cm3' in path.name:
          file_machine_center.append(cm3)
          if 'PBA' in path.name:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[-9])
            file_coilid.append(str(int((path.name.split('/')[-1]).split('_')[-8])))
          else:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[1])
            a = path.name.split('/')[-1].split('_')[2]
            if a == '':
              file_coilid.append('0')
            else:
              file_coilid.append(a)
        elif 'tcm3' in path.name:
          file_machine_center.append(cm3)
          if 'PBA' in path.name:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[-9])
            file_coilid.append(str(int((path.name.split('/')[-1]).split('_')[-8])))
          else:
            coil_processed_number.append((path.name.split('/')[-1]).split('_')[1])
            a = path.name.split('/')[-1].split('_')[2]
            if a == '':
              file_coilid.append('0')
            else:
              file_coilid.append(a)
      elif 'yeongju' in path.name:
        file_plant_name.append('yeongju')
        if 'hrm' in path.name:
          file_machine_center.append(hrm)
          coil_processed_number.append((path.name.split('/')[-1]).split('_')[3])
          file_coilid.append(str(int((path.name.split('/')[-1]).split('_')[4])))
        elif 'hfm' in path.name:
          file_machine_center.append(hfm)
          coil_processed_number.append((path.name.split('/')[-1]).split('_')[3])
          file_coilid.append((str(int((path.name.split('/')[-1]).split('_')[4]))))
        elif 'cm1' in path.name:
          file_machine_center.append(cm1)
          coil_processed_number.append('0')
          file_coilid.append('0')
        elif 'cm2' in path.name:
          file_machine_center.append(cm2)
          coil_processed_number.append('0')
          file_coilid.append('0')
        elif 'cm3' in path.name:
          file_machine_center.append(cm3)
          coil_processed_number.append('0')
          file_coilid.append('0')
        elif 'tcm3' in path.name:
          file_machine_center.append(cm3)
          coil_processed_number.append('0')
          file_coilid.append('0')
      else:
        pass

# COMMAND ----------

#get all the file paths of csv files from raw path 
for i in raw_path_csv:
  paths = file_system_client.get_paths(path=i)
  for path in paths:
    if file_format_csv in path.name:
      file_paths.append(path.name)                #extract file path
      file_source.append(path.name.split('/')[2]) #extract file source name
      file_names.append(path.name.split('/')[-1]) #extract file name
      file_year.append(path.name.split('/')[-4])  #extract year
      file_month.append(path.name.split('/')[-3]) #extract month
      file_day.append(path.name.split('/')[-2])   #extract day
      if 'pinda' in path.name:
        file_plant_name.append('pinda')           #extract plant name
        if 'mes' in path.name:
          file_machine_center.append(mes)         #extract machine center details
          coil_processed_number.append('0')       #define a default value to coil processed number
          file_coilid.append('0')                 #define a default value to coil id
        elif 'pcs' in path.name:
          file_machine_center.append('pcs')
          coil_processed_number.append('0')
          file_coilid.append('0')
      elif 'oswego' in path.name:
        file_plant_name.append('oswego')
        if 'mes' in path.name:
          file_machine_center.append(mes)
          coil_processed_number.append('0')
          file_coilid.append('0')
        elif 'pcs' in path.name:
          file_machine_center.append('pcs')
          coil_processed_number.append('0')
          file_coilid.append('0')
      elif 'sierre' in path.name:
        file_plant_name.append('sierre')
        if 'mes' in path.name:
          file_machine_center.append(mes)
          coil_processed_number.append('0')
          file_coilid.append('0')
        elif 'pcs' in path.name:
          file_machine_center.append('pcs')
          coil_processed_number.append('0')
          file_coilid.append('0')
      elif 'yeongju' in path.name:
        file_plant_name.append('yeongju')
        if 'mes' in path.name:
          file_machine_center.append(mes)
          coil_processed_number.append('0')
          file_coilid.append('0')
        elif 'pcs' in path.name:
          file_machine_center.append('pcs')
          coil_processed_number.append('0')
          file_coilid.append('0')
      else:
        pass

# COMMAND ----------

#creating a dataframe from two lists and then adding a new column to the dataframe
from pyspark.sql.functions import lit
df = spark.createDataFrame(zip(file_names, file_paths,file_plant_name,file_source,file_machine_center,file_coilid,coil_processed_number, file_year, file_month, file_day), schema=[raw_file_name, raw_file_path,raw_plant_name,raw_file_source,raw_machine_center,coil_id,processed_number,raw_file_year,raw_file_month,raw_file_day]).withColumn(is_read, lit(0))

# COMMAND ----------

#create machine center seq column based on coil id, machine center and year of given file
from pyspark.sql import functions as F
df = df.select('*',row_number().over(Window.partitionBy("coil_id","machine_center","year").orderBy(asc(processed_number))).alias('machine_center_seq'))
df = df.withColumn('machine_center_seq',F.when((col('coil_id') =='0'),lit(1)).otherwise(col('machine_center_seq')))

# COMMAND ----------

#convert year, month and day to integer type
df = df.withColumn(raw_file_year,col(raw_file_year).cast('integer'))\
       .withColumn(raw_file_month,col(raw_file_month).cast('integer'))\
       .withColumn(raw_file_day,col(raw_file_day).cast('integer'))\
       .withColumn(error_code,lit(None).cast('string'))

# COMMAND ----------

try:
  #get file_names and plant name from the file_log delta table
  df_read = spark.sql("SELECT {},{} FROM {}.{} WHERE {} in {} ".format(raw_file_name,raw_plant_name,trn_db, table_name,raw_file_source,('iba','mes')))
    
except:
  pass
else:   
  #filter dataframe to keep files from the append_files_names_list
  df_insert = df.join(df_read,on=[df.file_name == df_read.file_name,df.plant_name==df_read.plant_name], how="left_anti")  

#check for new records
if df_insert.count() ==0:
  dbutils.notebook.exit('No new files')
  
#write the data into a delta table
df_insert.write.format('delta').mode('append').partitionBy('plant_name','machine_center','file_name').option('path',destination_path).saveAsTable((trn_db+'.'+table_name))

# COMMAND ----------

"""#insert the new records into file log delta table
try:
  file_log_tbl = DeltaTable.forPath(spark, destination_path)
except:
  print('file log table not found')
  #store file log data in transform folder as a delta table
  df.write.format('delta').mode('append').partitionBy('plant_name','machine_center','file_name').option('path',destination_path).saveAsTable((trn_db+'.'+table_name))
else:
  #insert new records
  file_log_tbl.alias("file_log").merge( df.alias("fl_df"), "file_log.machine_center = fl_df.machine_center and fl_df.plant_name = file_log.plant_name and file_log.file_name=fl_df.file_name")\
                                .whenNotMatchedInsertAll()\
                                .execute()
  """