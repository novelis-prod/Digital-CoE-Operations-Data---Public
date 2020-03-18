# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview of this notebook
# MAGIC This notebook is to read the files from file log table and create respective delta tables for plants.
# MAGIC 
# MAGIC • Get all the files in the file log delta table which are not yet read(is_read=0)
# MAGIC 
# MAGIC • Read all those files and store it in respective delta table
# MAGIC 
# MAGIC Input: All unread files with specific machine center
# MAGIC 
# MAGIC Output: Store data in delta table

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
table_name = 'file_log'
#define column names
year = 'year'
is_read = 'is_read'
plant_name='plant_name'
file_path = 'file_path'
error_code='error_code'
machine_center='machine_center'

# COMMAND ----------

#machine center list 
mac = dbutils.widgets.get('machine_center')

#plant list
plant =  dbutils.widgets.get('plant_name')

#year
yr = dbutils.widgets.get('year')

#define details of notebook to be run
notebook_to_run ='/de_digital_coe/'+env+'/050-Rolling/03.p_g2_all_rolling_load_file_3'

#define error code default value
error_code_type = 'null'

#define number of jobs to run parallelly 
pool = ThreadPool(10) 

# COMMAND ----------

# MAGIC %md Read and load unprocessed files into delta table

# COMMAND ----------

#query to read files with is_read as 0 and error_code as null values
query = 'select {} from {}.{} where {}={} and {}={} and {}={} and {} is {} and {} in {}'.format(file_path,trn_db,table_name,is_read,0,plant_name,'"'+plant+'"',machine_center,'"'+mac+'"',error_code,error_code_type,year,yr)

#read data into pandas df
file_log_df = spark.sql(query).toPandas()
  
#exit the notebook if there are no files to read
if file_log_df['file_path'].count()==0:
  dbutils.notebook.exit(mac+' No new files')
  #print(mac,'No new files')
else:
  #define a list of filenames 
  file_list = file_log_df['file_path']
  print(mac,len(file_list))
  #run a particular notebook based on the details of  machine center from filename
  pool.map(lambda path : dbutils.notebook.run(notebook_to_run,0,{'input_file':path}),file_list)  