# Databricks notebook source
# MAGIC %md # Overview of this notebook
# MAGIC 
# MAGIC This notebook is to scan recent files in the designated paths of the REMELT
# MAGIC 
# MAGIC • Read the newly uploaded files from landing zone and store it in rml_file_log delta table in enterprise zone.
# MAGIC 
# MAGIC • Extract file name, file path, plant name, machine center name, coil id etc. information from file path
# MAGIC 
# MAGIC • Store this data into a delta table in enterprise zone
# MAGIC 
# MAGIC Input to this notebook: Details of latest uploaded files in landing zone
# MAGIC 
# MAGIC Output of this notebook: Append to the existing delta table(rml_file_log) with new data in enterprise zone

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

def scan_remelt_gen2(scan_path , fsc, days_upto:int=6,fpattern:str='.'):
  """"Gets and prints the spreadsheet's header columns
  
  Parameters
  ----------
  
  scan_path:str
    The ADLS location to scan for new files
    
  fsc: ADLS File System Client
  
  days_upto: int
    Upto how many days back we should scan the directories. This will build the sub-directories <scan_path>/<current year>/<current month>/<current date> thru <scan_path>/<current year>/<current month>/<current date-daysupto> 
    
  fpattter: str
    To search for file names for the string specified. 
    
  Returns:
  dataframe with names of the files with 
  
  """
  raw_file_path = 'file_path'
  raw_file_source = 'file_source'
  raw_file_name = 'file_name'
  raw_file_year = 'year'
  raw_file_month= 'month'
  raw_file_day  = 'day'
  raw_machine_center = 'machine_center'
  raw_plant_name ='plant_name'
  is_read = 'is_read'
  error_code = 'error_code'
  processed_number = 'processed_number'
  coil_id ='coil_id'
  machine_center_seq = 'machine_center_seq'
  scan_dirs = []
  today = date.today()
  #Generate list with entries in the form of  <scan_path>/yyyy/mm/dd/*filep*
  for x  in range(0,days_upto):
    tdelta = timedelta(days=x)
    scan_dir = scan_path + (today - tdelta).strftime('%Y') + '/' +(today - tdelta).strftime('%m') + \
    '/' + (today - tdelta).strftime('%d') 
    scan_dirs.append(scan_dir)
  filelist = []
  #scan directores specfied
  for dir in scan_dirs:
    try:
      files = fsc.get_paths(path=dir,recursive=False)
      for file in files:
        if (file.is_directory == False) and (fpattern in file.name):
#         if (file.is_directory == False): 
          filelist.append(file.name)
    except:
      pass
    
  #Remove files with no pattern match 
#   filelist = [file for file in files if fpattern in file.name] 
  expr_str = "CASE WHEN file_path like '%/pi/%' THEN 'pi'  WHEN file_path like '%/ods/%' THEN 'ods' ELSE 'OTHERS' END"
  expr_str_plt = "CASE WHEN plant_name = 'oswego' THEN 'oswego'  WHEN plant_name = 'yeongju' THEN 'yej' WHEN plant_name = 'pinda' then 'pba' ELSE plant_name END"
  
  #Create dataframe
  if len(filelist) > 0 :
    df_files = spark.createDataFrame(zip(filelist) , schema=[raw_file_path])
    df_files = df_files.withColumn(raw_file_name, f.split(df_files[raw_file_path], '/'))
    df_files = df_files.withColumn(raw_plant_name, f.split(df_files[raw_file_path], '/').getItem(1))
    df_files = df_files.withColumn(raw_file_source,expr(expr_str))
    df_files = df_files.withColumn(raw_machine_center, lit('RML'))
    df_files = df_files.withColumn(raw_file_year, \
                                   col(raw_file_name).getItem(expr("size({0})-4".format(raw_file_name))).cast(IntegerType()))
    df_files = df_files.withColumn(raw_file_month,\
                               col(raw_file_name).getItem(expr("size({0})-3".format(raw_file_name))).cast(IntegerType()))
    df_files = df_files.withColumn(raw_file_day,\
                                 col(raw_file_name).getItem(expr("size({0})-2".format(raw_file_name))).cast(IntegerType()))                             
#   df_files = df_files.withColumn(coil_id, lit(0).cast(StringType()))
#   df_files = df_files.withColumn(processed_number, lit(0).cast(StringType()))
    df_files = df_files.withColumn(is_read, lit(0))
    df_files = df_files.withColumn(error_code, lit(None).cast(StringType()))
#   df_files = df_files.withColumn(machine_center_seq, lit(0).cast(IntegerType()))
    df_files = df_files.withColumn(raw_file_name, col(raw_file_name).getItem(expr("size({0})-1".format(raw_file_name))))  
    df_files = df_files.withColumn(raw_plant_name,expr(expr_str_plt))
  #rearrange columns
    df_files = df_files.select(raw_file_path,raw_file_name,raw_plant_name,raw_file_source,raw_machine_center, \
                               raw_file_year,raw_file_month,raw_file_day,is_read,error_code)
  
    return df_files
  else:
    return None

# COMMAND ----------

#Create list with all the paths and respective file names to search for
# paths_list = ['landing/oswego/pi/remelt/assetframe/','landing/oswego/pi/remelt/assetframe/','landing/oswego/pi/remelt/assetframe/','landing/oswego/ods/standard/remeltbatchmoltenbatchinput/','landing/oswego/ods/standard/remeltsamplebatchsample/','landing/oswego/ods/standard/castingingot/','landing/pinda/pi/remelt/assetframe/','landing/pinda/pi/remelt/assetframe/','landing/pinda/pi/remelt/assetframe/','landing/yeongju/pi/remelt/assetframe/','landing/yeongju/pi/remelt/assetframe/','landing/yeongju/pi/remelt/assetframe/','landing/yeongju/pi/remelt/assetframe/','landing/yeongju/pi/remelt/assetframe/']
# files_list = ['Melter','Caster','Holder','.','.','.','Melter','Holder','DC','Decoter','SM','Melter','Holder','DC_Caster']

# COMMAND ----------

#Create list with all the paths and respective file names to search for
paths_list = ['landing/oswego/pi/remelt/assetframe/','landing/oswego/pi/remelt/assetframe/','landing/oswego/pi/remelt/assetframe/','landing/oswego/ods/standard/remeltbatchmoltenbatchinput/','landing/oswego/ods/standard/remeltsamplebatchsample/','landing/oswego/ods/standard/castingingot/','landing/oswego/ods/standard/remeltprocessphase/','landing/oswego/ods/standard/remeltprocesscycle/','landing/pinda/pi/remelt/assetframe/','landing/pinda/pi/remelt/assetframe/','landing/pinda/pi/remelt/assetframe/','landing/yeongju/pi/remelt/assetframe/','landing/yeongju/pi/remelt/assetframe/','landing/yeongju/pi/remelt/assetframe/','landing/yeongju/pi/remelt/assetframe/','landing/yeongju/pi/remelt/assetframe/']
files_list = ['Melter','Caster','Holder','.','.','.','.','.','Melter','Holder','DC','Decoter','SM','Melter','Holder','DC_Caster']

# COMMAND ----------

#print(paths_list),len(paths_list)


# COMMAND ----------

#print(files_list),len(files_list)

# COMMAND ----------

#Loop  through all the files per the paths specified above and call function scan_remelt_gen2
for l in range(len(paths_list)):
  if l == 0:
    files = scan_remelt_gen2(paths_list[l], file_system_client
                         ,2,files_list[l])
  else:
    files_to_app = scan_remelt_gen2(paths_list[l], file_system_client
                         ,2,files_list[l])
    if files is not None and files_to_app is not None:
      files = files.union(files_to_app)
    elif files is None  and files_to_app is not None :
      files = files_to_app
    else:
      None 


# COMMAND ----------

#display(files)

# COMMAND ----------

#Extract unique years,months and days from the files data frame.
files.persist()
l_plant = ["'" + str(i.plant_name) + "'" for i in files.select('plant_name').distinct().collect()]
l_year = [str(i.year) for i in files.select('year').distinct().collect()]
l_month = [str(i.month) for i in files.select('month').distinct().collect()]
l_day = [str(i.day) for i in files.select('day').distinct().collect()]

# COMMAND ----------

#Build SQL Predicates to read the existing entries from table rml_file_log
plant_pred = ",".join(l_plant) 
plant_pred = "(" + plant_pred + ")"
# print(plant_pred)
year_pred = ",".join(l_year)
year_pred = "(" + year_pred + ")"
# print(year_pred)
month_pred = ",".join(l_month) 
month_pred = "(" + month_pred + ")"
# print(month_pred)
day_pred = ",".join(l_day) 
day_pred = "(" + day_pred + ")"
# print(day_pred)

# COMMAND ----------

#select the exisitng records from rml_file_log to identify the new records from dataframe files
v_sql = "select * from opstrnprodg2.rml_file_log  where plant_name in {3} and machine_center = 'RML' and year in {0} and month in {1} and  day in {2} ".format(year_pred,month_pred ,day_pred,plant_pred)
print(v_sql)

df_read = spark.sql(v_sql)


# COMMAND ----------

#Identify the records to be insert by joining df_insert and df_read
df_insert = broadcast(files).join(df_read,on=[files.file_name == df_read.file_name ,files.year == df_read.year,files.month == df_read.month\
                                  , files.day == df_read.day , files.file_source == df_read.file_source ], how="left_anti")

# COMMAND ----------

 #print("Number of records to insert into table rml_file_log: " + str(df_insert.count()) )

# COMMAND ----------

#Insert into rml_file_log
df_insert.write.format('delta').mode('append').partitionBy('plant_name','machine_center','file_name').option('path',destination_path).saveAsTable((trn_db+'.'+table_name))

# COMMAND ----------

files.unpersist()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*) fROm opstrnprodg2.rml_file_log where plant_name = 'oswego' and machine_center = 'RML'

# COMMAND ----------

# %sql
# select count(*) fROm opstrnprodg2.rml_file_log where plant_name = 'oswego' and machine_center = 'RML' and is_read = 0

# COMMAND ----------

# %sql
# -- optimize opstrnprodg2.rml_file_log ZORDER BY (is_read)

# COMMAND ----------

