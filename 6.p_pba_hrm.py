# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview of this notebook
# MAGIC This notebook is to read the files from file log table and create HRM delta tables for plant pinda.
# MAGIC 
# MAGIC • Get all the files in the file log delta table which are not yet read(is_read=0) and machine center value as HRM
# MAGIC 
# MAGIC • Read all those files and store it in HRM delta table
# MAGIC 
# MAGIC • Run the notebook for every X minutes to update the delta table with new data
# MAGIC 
# MAGIC Input: All unread files of machine center HRM
# MAGIC 
# MAGIC Output: Store the new data in HRM delta table

# COMMAND ----------

# MAGIC %run config/secret

# COMMAND ----------

#details of database
env = 'prod'
DB= 'opstrnprod'
TABLENAME= 'file_log'

#details of machine center and notebook
plant_name = '"pinda"'
machine_center = '"hrm"'
notebook_to_run ='/de_digital_coe/' + env + '/8.p_pba_hm'
error_code_type = 'null'
year = '2019'

# COMMAND ----------

#query to read files in file_log table with is_read as 0
query = 'select * from '+ DB + '.' + TABLENAME+' where is_read = 0 and plant_name = ' + plant_name + ' and  machine_center = '+ machine_center + ' and year='+year+' and error_code is ' + error_code_type 
file_log_df = spark.sql(query).toPandas()

if file_log_df['file_path'].count()==0:
  dbutils.notebook.exit('No new files')
  
#define a list of filenames 
file_list = file_log_df['file_path']

# COMMAND ----------

#run a particular notebook based on the details of  machine center from filename
for i in file_list:
  dbutils.notebook.run(notebook_to_run,0,{'input_file':i})