# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview of this notebook
# MAGIC This notebook is to read the files from file log table and create CM3 delta tables for plant pinda.
# MAGIC 
# MAGIC • Get all the files in the file log delta table which are not yet read(is_read=0) and machine center value as CM3
# MAGIC 
# MAGIC • Read all those files and store it in CM3 delta table
# MAGIC 
# MAGIC • Run the notebook for every X minutes to update the delta table with new data
# MAGIC 
# MAGIC Input: All unread files of machine center CM3
# MAGIC 
# MAGIC Output: Store the new data in CM3 delta table

# COMMAND ----------

# MAGIC %run config/secret

# COMMAND ----------

#details of database
env = 'prod'
DB= 'opstrnprod'
TABLENAME= 'file_log'

#details of machine center and notebook
machine_center = '"cm3"'
notebook_to_run ='/de_digital_coe/' + env +'/13.p_pba_cm'
error_code_type = 'null'
plant_name = '"pinda"'

# COMMAND ----------

#query to read files in file_log table with is_read as 0
query = 'select * from '+ DB + '.' + TABLENAME+' where is_read = 0 and plant_name = '+plant_name+' and machine_center = '+ machine_center + 'and error_code is ' + error_code_type 
file_log_df = spark.sql(query).toPandas()

if file_log_df['file_path'].count()==0:
  dbutils.notebook.exit('No new files')
  
# define a list of filenames 
file_list = file_log_df['file_path']

# COMMAND ----------

#run a particular notebook based on the details of  machine center from filename
for i in file_list:
  dbutils.notebook.run(notebook_to_run,0,{'input_file':i})