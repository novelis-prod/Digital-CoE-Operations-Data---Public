# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview of this notebook
# MAGIC This notebook is to get the details of machine center and plant and run another notebook to load unprocessed files parallelly.
# MAGIC 
# MAGIC Input: Details of machine center and plant 
# MAGIC 
# MAGIC Output: Run other notebook to get unprocessed files data from file log 

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
notebook_to_run ='/de_digital_coe/prodg2/050-Rolling/10-Length/02.p_g2_all_rolling_load_file_2'

#define error code default value
error_code_type = 'null'

#define number of jobs to run parallelly 
pool = ThreadPool(10) 

# COMMAND ----------

# MAGIC %md Get details of machine center and plant

# COMMAND ----------

#get the machine center value from parameters
m_list = dbutils.widgets.get('machine_center')
machine_list = list(m_list.split(','))

#get the plant value from parameters
plant =  dbutils.widgets.get('plant_name')

#get the number to days  value from parameters
days = dbutils.widgets.get('days_upto')

# COMMAND ----------

# MAGIC %md Run another notebook to get unprocessed files from file log

# COMMAND ----------

#run a particular notebook based on the details of  machine center
pool.map(lambda machine_center : dbutils.notebook.run(notebook_to_run,0,{'machine_center':machine_center,'plant_name':plant,'days_upto':days}),machine_list)  