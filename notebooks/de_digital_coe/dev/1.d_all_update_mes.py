# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview of this notebook
# MAGIC This notebook is to read the files from file log table and create respective delta tables for plants (pinda,oswego,yeongju,sierre).
# MAGIC 
# MAGIC • Get all the files in the file log delta table which are not yet read(is_read=0)
# MAGIC 
# MAGIC • Read all those files and store it in respective delta table (mes)
# MAGIC 
# MAGIC Input: All unread files with specific machine center (mes)
# MAGIC 
# MAGIC Output: Store data in delta table

# COMMAND ----------

# MAGIC %run config/secret

# COMMAND ----------

#details of database
DB= 'opstrndev'
TABLENAME= 'file_log'

#details of machine center and notebook
machine_center = '"pcs"'
notebook_to_run ='/de_digital_coe/dev/2.d_all_mes'
error_code_type = 'null'

# COMMAND ----------

#query to read files with is_read as 0
query = 'select * from '+ DB + '.' + TABLENAME+' where is_read = 0 and machine_center = '+ machine_center + 'and error_code is ' + error_code_type
file_log_df = spark.sql(query).toPandas()

if file_log_df['file_path'].count()==0:
  dbutils.notebook.exit('No new files')
  
# define a list of filenames 
file_list = file_log_df['file_path']

# COMMAND ----------

# run a particular notebook based on the details of  machine center from filename
for i in file_list:
  dbutils.notebook.run(notebook_to_run,0,{'input_file':i})