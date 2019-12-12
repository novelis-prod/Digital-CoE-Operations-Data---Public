# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook is to store the file path of all the files dropped in the landing zone. This will have path and an additional column for read for transform zone. 
# MAGIC 
# MAGIC • Read the newly uploaded files from landing zone and store it in file log delta table in transform zone 
# MAGIC 
# MAGIC • Input to this notebook: Details of latest uploaded files in landing zone 
# MAGIC 
# MAGIC • Output of this notebook: Append to the existing delta table with new data in transform zone

# COMMAND ----------

# MAGIC %run config/secret

# COMMAND ----------

#define variables
env = 'prod'
TABLE_NAME = 'file_log'
DB_NAME = 'opstrnprod'
DL = 'daa'
FILE_FORMAT_PAR = '.parquet'
FILE_FORMAT_CSV = '.csv'

BASE_PATH = "adl://novelisadls.azuredatalakestore.net/"

#input path for machine center files
RAW_PATH_PAR = ['landing/pinda/iba/standard/material_length']
#input path for mes files
RAW_PATH_CSV = ['landing/pinda/mes',
                'landing/yeongju/mes']

DESTINATION_PATH = 'adl://novelisadls.azuredatalakestore.net/transform/' + env + '/file_log'

#column names
raw_file_path = 'file_path'
raw_file_name = 'file_name'
raw_file_year = 'year'
raw_file_month= 'month'
raw_file_day  = 'day'
raw_machine_center = 'machine_center'
raw_plant_name ='plant_name'
is_read = 'is_read'
error_code = 'error_code'
mes = 'mes'
hrm ='hrm'
hfm ='hfm'
cm1 ='cm1'
cm2 ='cm2'
cm3 ='cm3'

# COMMAND ----------

adl = connection(DL)

# COMMAND ----------

#get all the file paths of parquet files from raw path 
file_paths = []
file_names = []
file_year  = []
file_month = []
file_day   = []
file_plant_name=[]
file_machine_center = []
for i in RAW_PATH_PAR:
  for file_path in adl.walk(i):
    if FILE_FORMAT_PAR in file_path:
      file_paths.append(file_path)
      file_names.append(file_path.split('/')[-1])
      file_year.append(file_path.split('/')[-4])
      file_month.append(file_path.split('/')[-3])
      file_day.append(file_path.split('/')[-2])
      if 'pinda' in file_path:
        file_plant_name.append('pinda')
        if hrm in file_path:
          file_machine_center.append(hrm)
        elif hfm in file_path:
          file_machine_center.append(hfm)
        elif cm1 in file_path:
          file_machine_center.append(cm1)
        elif cm2 in file_path:
          file_machine_center.append(cm2)
        elif cm3 in file_path:
          file_machine_center.append(cm3)
        elif 'tcm3' in file_path:
          file_machine_center.append(cm3)
      elif 'oswego' in file_path:
        file_plant_name.append('oswego')
        if hrm in file_path:
          file_machine_center.append(hrm)
        elif hfm in file_path:
          file_machine_center.append(hfm)
        elif cm1 in file_path:
          file_machine_center.append(cm1)
        elif cm2 in file_path:
          file_machine_center.append(cm2)
        elif cm3 in file_path:
          file_machine_center.append(cm3)
        elif 'tcm3' in file_path:
          file_machine_center.append(cm3)
      elif 'sierre' in file_path:
        file_plant_name.append('sierre')
        if hrm in file_path:
          file_machine_center.append(hrm)
        elif hfm in file_path:
          file_machine_center.append(hfm)
        elif cm1 in file_path:
          file_machine_center.append(cm1)
        elif cm2 in file_path:
          file_machine_center.append(cm2)
        elif cm3 in file_path:
          file_machine_center.append(cm3)
        elif 'tcm3' in file_path:
          file_machine_center.append(cm3)
      elif 'yeongju' in file_path:
        file_plant_name.append('yeongju')
        if hrm in file_path:
          file_machine_center.append(hrm)
        elif hfm in file_path:
          file_machine_center.append(hfm)
        elif cm1 in file_path:
          file_machine_center.append(cm1)
        elif cm2 in file_path:
          file_machine_center.append(cm2)
        elif cm3 in file_path:
          file_machine_center.append(cm3)
        elif 'tcm3' in file_path:
          file_machine_center.append(cm3)
      else:
        pass

# COMMAND ----------

#get all the file paths of csv files from raw path 
for i in RAW_PATH_CSV:
  for file_path in adl.walk(i):
    if FILE_FORMAT_CSV in file_path:
      file_paths.append(file_path)
      file_names.append(file_path.split('/')[-1])
      file_year.append(file_path.split('/')[-4])
      file_month.append(file_path.split('/')[-3])
      file_day.append(file_path.split('/')[-2])
      if 'pinda' in file_path:
        file_plant_name.append('pinda')
        if mes in file_path:
          file_machine_center.append(mes)
        elif 'pcs' in file_path:
          file_machine_center.append('pcs')
      elif 'oswego' in file_path:
        file_plant_name.append('oswego')
        if mes in file_path:
          file_machine_center.append(mes)
        elif 'pcs' in file_path:
          file_machine_center.append('pcs')
      elif 'sierre' in file_path:
        file_plant_name.append('sierre')
        if mes in file_path:
          file_machine_center.append(mes)
        elif 'pcs' in file_path:
          file_machine_center.append('pcs')
      elif 'yeongju' in file_path:
        file_plant_name.append('yeongju')
        if mes in file_path:
          file_machine_center.append(mes)
        elif 'pcs' in file_path:
          file_machine_center.append('pcs')
      else:
        pass

# COMMAND ----------

#creating a dataframe from two lists and then adding a new column to the dataframe
from pyspark.sql.functions import lit
df = spark.createDataFrame(zip(file_names, file_paths,file_plant_name,file_machine_center, file_year, file_month, file_day), schema=[raw_file_name, raw_file_path,raw_plant_name,raw_machine_center,raw_file_year,raw_file_month,raw_file_day]).withColumn(is_read, lit(0))

# COMMAND ----------

#convert year, month and day to integer
from pyspark.sql.functions import *
df = df.withColumn(raw_file_year,col(raw_file_year).cast('integer'))\
       .withColumn(raw_file_month,col(raw_file_month).cast('integer'))\
       .withColumn(raw_file_day,col(raw_file_day).cast('integer'))\
       .withColumn(error_code,lit(None).cast('string'))

# COMMAND ----------

#creating a database
#spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(DB_NAME))
spark.sql("USE {}".format(DB_NAME))

# COMMAND ----------

#get file_names as list from the file_log delta table
log_table_data = spark.sql("SELECT {} FROM {}.{}".format(raw_file_name, DB_NAME, TABLE_NAME))
log_table_data_list = [x[0] for x in log_table_data.select(raw_file_name).collect()]

# COMMAND ----------

#get file_names from the dataframe to be appended
file_names_list = [x[0] for x in df.select(raw_file_name).collect()]

# COMMAND ----------

#get files not stored in delta table # all files: file_names_list # delta table files : log_table_data_list
append_file_names_list = []
append_file_names_list = (set(file_names_list).difference(log_table_data_list))

# COMMAND ----------

#filter dataframe to keep files from the append_files_names_list
df = df.where(df[raw_file_name].isin(append_file_names_list))

if df.count() ==0:
  dbutils.notebook.exit('No new files')

# COMMAND ----------

#saving the data and storing the data as delta table
df.repartition(1).write.format('delta').mode('append').partitionBy('plant_name','machine_center').option('path',DESTINATION_PATH).saveAsTable(TABLE_NAME)