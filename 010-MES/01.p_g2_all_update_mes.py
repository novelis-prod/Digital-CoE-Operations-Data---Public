# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview of this notebook
# MAGIC This notebook is to read the files from file log table and create mes delta table for plants(pinda,yeongju,sierre,oswego).
# MAGIC 
# MAGIC • Get all the files from file log delta table which are not yet read (is read=0) and machine center code as MES
# MAGIC 
# MAGIC • Read all those files and store it in mes delta table
# MAGIC 
# MAGIC • Run the notebook for every X minutes to keep mes table updated by appending with new data
# MAGIC 
# MAGIC Input: All unread files with specific machine center code (MES)
# MAGIC 
# MAGIC Output: Store data in transform zone as delta table

# COMMAND ----------

# DBTITLE 1,Connect adls to databricks
# MAGIC %run /config/secret_gen2

# COMMAND ----------

# MAGIC %md Define parameters

# COMMAND ----------

#details of database
trn_db= 'opstrnprodg2'
table_name= 'file_log'
env = 'prod'
#details of machine center and notebook
machine_center = '"MES"'
notebook_to_run ='/de_digital_coe/prodg2/010-MES/02.p_g2_all_mes'
error_code_type = 'null'

# COMMAND ----------

# MAGIC %md Get unprocessed files from file log 

# COMMAND ----------

#query to read files with is_read as 0
query = 'select * from '+ trn_db + '.' + table_name+' where is_read = 0 and machine_center = '+ machine_center + ' and error_code is ' + error_code_type +' order by year,month,day'
file_log_df = spark.sql(query).toPandas()

if file_log_df['file_path'].count()==0:
  dbutils.notebook.exit('No new files')
  
# define a list of filenames 
file_list = file_log_df['file_path']

# COMMAND ----------

# MAGIC %md Read and load unprocessed files into delta table

# COMMAND ----------

from multiprocessing.pool import ThreadPool
#run a particular notebook based on the details of  machine center from filename
pool = ThreadPool(10) #define number of threads to run parallelly 
pool.map(lambda path : dbutils.notebook.run(notebook_to_run,0,{'input_file':path}),file_list)