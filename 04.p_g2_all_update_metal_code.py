# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Overview of this notebook
# MAGIC 
# MAGIC This notebook is to read data from rolling delta tables and update metal code from coil index table.
# MAGIC 
# MAGIC Input: All the records from cm delta tables where metal code column has default value.
# MAGIC 
# MAGIC Output: Store the updated data in respective delta table.

# COMMAND ----------

# DBTITLE 1,Connect adls to data bricks 
# MAGIC %run config/secret_gen2

# COMMAND ----------

# MAGIC %md Define parameters

# COMMAND ----------

#import libraries
from pyspark.sql.functions import *
from multiprocessing.pool import ThreadPool
from delta.tables import *
#define database details
env='prod'
ent_db = 'opsentprodg2'

#define folder details
store_name = "novelisadlsg2"
container = "plantdata"
transform = 'transform'
enterprise ='enterprise'
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net" 

#define column names
coil_id  = 'cg_Gen_Coilid'
metal_code_default = 1000000000000000

#query to get coil id and metal code data from coil index table
query = 'select coil_id, metal_code,plant_code,year from '+ent_db+'.coil_index'

#create a new dataframe with coil id and metal code values
coil_index_df = spark.sql(query)

#define details of plant
plant_name = ['pinda']

#details of machine centers
machine_center = ['hrm','hfm','cm1','cm2','cm3']

# COMMAND ----------

#define a function to return the delta table details of each machine center
def table_details(plant_name,machine_center):
  destination_path   = base_path +'/'+ enterprise+ '/' + env + '/' + plant_name + '/' + machine_center
  if plant_name == 'pinda':
    if machine_center == 'hrm':
      machine_center_tbl = 'pba_hrm'
    elif machine_center == 'hfm':
      machine_center_tbl = 'pba_hfm'
    elif machine_center == 'cm1':
      machine_center_tbl = 'pba_cm1'
    elif machine_center == 'cm2':
      machine_center_tbl = 'pba_cm2'
    elif machine_center == 'cm3':
      machine_center_tbl = 'pba_cm3'
  elif plant_name == 'yeongju':
    if machine_center == 'hrm':
       machine_center_tbl = 'yj_hrm'  
    elif machine_center == 'hfm':
       machine_center_tbl = 'yj_hfm'
    elif machine_center == 'cm1':
      machine_center_tbl = 'pba_cm1'
    elif machine_center == 'cm2':
      machine_center_tbl = 'pba_cm2'
    elif machine_center == 'cm3':
      machine_center_tbl = 'pba_cm3'
    
  return destination_path,machine_center_tbl

# COMMAND ----------

#define a function to update the metal code for each plant
def plant(plant):
  def metal_code_update(mc):
    # This function takes machine center as input and perform update operations on respective machine center delta table

    #get destination path and table details form reference notebook
    destination_path,table_name = table_details(plant,mc)  
    print(plant,mc)
    #define delta table path
    tbl=DeltaTable.forPath(spark, destination_path)

    #update metal code value in machine center 
    tbl.alias("cm").merge(
      coil_index_df.alias("coil_index"),
      "cm."+coil_id +" = coil_index.coil_id and cm.plant_code = coil_index.plant_code and cm.year = coil_index.year  and cm.year > 2018  and cm.month <11 and cm.metal_code = "+ str(metal_code_default))\
    .whenMatchedUpdate(set = { "cm.metal_code" : "coil_index.metal_code"})\
    .execute()      
  #update metal code for each machine cente
  pool.map(lambda mc : metal_code_update(mc),machine_center) 

# COMMAND ----------

#update metal code for each plant
pool = ThreadPool(5) #define number of threads to run parallelly
pool.map(lambda p : plant(p),plant_name)