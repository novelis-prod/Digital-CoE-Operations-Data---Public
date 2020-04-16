# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview of this notebook
# MAGIC This notebook is to read the files from file log table and create respective delta tables for plants.
# MAGIC 
# MAGIC • Get all the files in the file log delta table which are not yet read(is_read=0)
# MAGIC 
# MAGIC Input: Machine center  and plant details
# MAGIC 
# MAGIC Output: Run a notebook which write's data to delta tables

# COMMAND ----------

# DBTITLE 1,Connect adls to data bricks
# MAGIC %run config/secret_gen2

# COMMAND ----------

# MAGIC %md Define parameters

# COMMAND ----------

#import libraries 
from pyspark.sql.functions import * 
from multiprocessing.pool import ThreadPool
from datetime import *
#define database
env = 'prodg2'
trn_db = 'opstrnprodg2'
table_name = 'file_log'
#define column names
year = 'year'
is_read = 'is_read'
proc_date = 'proc_date'
plant_name='plant_name'
file_path = 'file_path'
error_code='error_code'
machine_center='machine_center'

spark.conf.set('spark.sql.execution.arrow.enabled',False)

#get the machine center value from parameters
mac = dbutils.widgets.get('machine_center')

#get the plant value from parameters
plant = dbutils.widgets.get('plant_name')

#get the number to days  value from parameters
days_upto = int(dbutils.widgets.get('days_upto'))

#define details of notebook to be run
notebook_to_run ='/de_digital_coe/'+env+'/050-Rolling/10-Length/03.p_g2_all_rolling_load_file_3'

#define error code default value
error_code_type = 'null'

#define number of jobs to run parallelly 
pool = ThreadPool(10) 

# COMMAND ----------

# MAGIC %md Get unporcessed files from file log

# COMMAND ----------

#query to read files with is_read as 0 and error_code as null values
query = 'select {},{} from {}.{} where {}={} and {}={} and {}={} and {} is {} and {} > {}'.format(file_path,proc_date,trn_db,table_name,is_read,0,plant_name,'"'+plant+'"',machine_center,'"'+mac+'"',error_code,error_code_type,year,'2018')

#read data into df
file_log_df = spark.sql(query)

# COMMAND ----------

# MAGIC %md Run another notebook  to append new data to delta tables

# COMMAND ----------

#get unprocessed files of based on date order and load it to delta table 
for days in range(0,days_upto):
  date = datetime.date(datetime.today()) - timedelta(days)
  #get the list of unprocessed files
  file_list = file_log_df.where(col('proc_date')== date).toPandas()['file_path']
  
  #check for new files
  if len(file_list) >0:
    print(date)
    print(mac,len(file_list))
    
    #run a particular notebook based on the details of  machine center from filename
    pool.map(lambda path : dbutils.notebook.run(notebook_to_run,0,{'input_file':path}),file_list) 
  else:
    print(mac,date,'No new files')  

# COMMAND ----------

#get unprocessed files of remaining days and load into delta table
file_list = file_log_df.where(col('proc_date')<date).toPandas()['file_path']

#check for new files
if len(file_list) >0:
  print(mac,len(file_list))
    
  #run a particular notebook based on the details of  machine center from filename
  pool.map(lambda path : dbutils.notebook.run(notebook_to_run,0,{'input_file':path}),file_list) 
else:
  print(mac,'No new files')  