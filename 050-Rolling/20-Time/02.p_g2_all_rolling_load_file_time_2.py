# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview of this notebook
# MAGIC This notebook is to read the files from file log time table and create respective delta tables for plants.
# MAGIC 
# MAGIC • Get all the files in the file log delta table which are not yet read(is_read=0)
# MAGIC 
# MAGIC • Read all those files and store it in respective delta table
# MAGIC 
# MAGIC Input: All unread files with specific machine center code
# MAGIC 
# MAGIC Output: Run a notebook which loads data to delta table

# COMMAND ----------

# DBTITLE 1,Connect adls to data bricks
# MAGIC %run config/secret_gen2

# COMMAND ----------

# MAGIC %md Define parameters

# COMMAND ----------

#import libraries 
from pyspark.sql.functions import * 
from multiprocessing.pool import ThreadPool
#define database
env = 'prodg2'
trn_db = 'opstrnprodg2'
table_name = 'file_log_time'
#define column names
year = 'year'
month = 'month'
is_read = 'is_read'
proc_date = 'proc_date'
plant_name='plant_name'
file_path = 'file_path'
error_code='error_code'
machine_center='machine_center'
spark.conf.set('spark.sql.execution.arrow.enabled',False)

# COMMAND ----------

#get the machine center value from parameters
mac = dbutils.widgets.get('machine_center')

#get the plant value from parameters
plant = dbutils.widgets.get('plant_name')

#define details of notebook to be run
notebook_to_run ='/de_digital_coe/prodg2/050-Rolling/20-Time/03.p_g2_all_rolling_load_file_time_3'

#define error code default value
error_code_type = 'null'

#define number of jobs to run parallelly 
pool = ThreadPool(10) 

# COMMAND ----------

# MAGIC %md Get unporcessed files from file log

# COMMAND ----------

def read_file_log():
  #query to get unprocessed files(is_read =0) in file log table
  query = 'select {} from {}.{} where {}={} and {}={} and {}={} and {} is {} and {}={} order by proc_date desc limit 50'.format(file_path,trn_db,table_name,is_read,0,plant_name,'"'+plant+'"',machine_center,'"'+mac+'"',error_code,error_code_type,year,'2020')
  
  #read data into df
  file_log_df = spark.sql(query)
  
  #get the list of unprocessed files from file log df
  file_list = file_log_df.toPandas()['file_path']
  
  if len(file_list)>0:
    load_data(file_list)
  else:
    dbutils.notebook.exit('No new files')  

# COMMAND ----------

# MAGIC %md Run another notebook  to store data in delta tables

# COMMAND ----------

def load_data(file_list):
  print(' load data to delta table : '+mac)
  #run the notebook which loads data into delta table
  pool.map(lambda path : dbutils.notebook.run(notebook_to_run,0,{'input_file':path}),file_list) 
  #get files from file log
  read_file_log()

# COMMAND ----------

if __name__ == "__main__":
  read_file_log()