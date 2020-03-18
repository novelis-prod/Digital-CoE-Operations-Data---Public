# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview of this notebook
# MAGIC This notebook is to get the details of machine center and plant and run another notebook to load unprocessed files.
# MAGIC 
# MAGIC Input: Details of machine center and plant 
# MAGIC 
# MAGIC Output: Run other notebook to get unprocessed files data 

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

#define details of notebook to be run
notebook_to_run ='/de_digital_coe/'+env+'/050-Rolling/02.p_g2_all_rolling_load_file_2'

#define error code default value
error_code_type = 'null'

#define number of jobs to run parallelly 
pool = ThreadPool(10) 

# COMMAND ----------

# MAGIC %md Get details of machine center and plant

# COMMAND ----------

#machine center list 
m_list = dbutils.widgets.get('machine_center')
machine_list = list(m_list.split(','))

#plant list
plant = dbutils.widgets.get('plant_name')

#year
year = dbutils.widgets.get('year')

# COMMAND ----------

# MAGIC %md Get unprocessed files

# COMMAND ----------

#run a particular notebook based on the details of  machine center
pool.map(lambda machine_center : dbutils.notebook.run(notebook_to_run,0,{'machine_center':machine_center,'plant_name':plant,'year':year}),machine_list)  