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

# MAGIC %run config/secret

# COMMAND ----------

#details of database
DB= 'opstrnprod'
TABLENAME= 'file_log'
env = 'prod'
#details of machine center and notebook
machine_center = '"mes"'
notebook_to_run ='/de_digital_coe/'+ env +'/2.p_all_mes'
error_code_type = 'null'

# COMMAND ----------

#query to read files with is_read as 0
query = 'select * from '+ DB + '.' + TABLENAME+' where is_read = 0 and machine_center = '+ machine_center + ' and error_code is ' + error_code_type 
file_log_df = spark.sql(query).toPandas()

if file_log_df['file_path'].count()==0:
  dbutils.notebook.exit('No new files')
  
# define a list of filenames 
file_list = file_log_df['file_path']

# COMMAND ----------

# run a particular notebook based on the details of  machine center from filename
for i in file_list:
  dbutils.notebook.run(notebook_to_run,0,{'input_file':i})