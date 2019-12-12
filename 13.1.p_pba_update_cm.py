# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Overview of this notebook
# MAGIC 
# MAGIC This notebook is to read data from CM1/CM2/CM3 delta tables and update metal code from coil index table.
# MAGIC 
# MAGIC • Read the data from CM1/CM2/CM3 delta tables which has default metal code values(1000000000000000)
# MAGIC 
# MAGIC • Update the metal code value based on plant code and coil id from coil index table 
# MAGIC 
# MAGIC Input: All the records from CM1/CM2/CM3 delta tables with default metal code value.
# MAGIC 
# MAGIC Output: Store the updated data in CM1/CM2/CM3 delta table.

# COMMAND ----------

# MAGIC %run config/secret

# COMMAND ----------

#import libraries to perform functional operations
from pyspark.sql.functions import *
from delta.tables import * 

#details of database
DB='opsentprod'
env = 'prod'
#details of delta table
plant_name = 'pinda'
cm1_tbl='pba_cm1'
cm2_tbl='pba_cm2'
cm3_tbl='pba_cm3'

#details of coil index table
coil_index_tbl='coil_index'

#details of machine centers
cm1 = 'cm1'
cm2 = 'cm2'
cm3 = 'cm3'
#details of machine centers
machine_center = ['cm2','cm3']

#metal code default value
metal_code_default = '1000000000000000'

# details of destination folder
cm1_dest = 'adl://novelisadls.azuredatalakestore.net/enterprise/' + env + '/' + plant_name +'/cm1'
cm2_dest = 'adl://novelisadls.azuredatalakestore.net/enterprise/' + env + '/' + plant_name +'/cm2'
cm3_dest = 'adl://novelisadls.azuredatalakestore.net/enterprise/' + env + '/' + plant_name +'/cm3'

#query to get coil id and metal code data from coil index table
query = 'select coil_id, metal_code,plant_code from '+DB+'.'+coil_index_tbl

#create a new dataframe with coil id and metal code values
coil_index_df = spark.sql(query)

# COMMAND ----------

# define a function to update metal code in delta table
def metal_code_update(machine_center):
  """ This function takes machine center as input and perform update operations on respective machine center delta table"""
   # database details of each  CM machine center
  if machine_center== cm1:
    destination_path= cm1_dest
    machine_center_tbl = cm1_tbl
    coil_id ="cg_Gen_CoilID"
  elif machine_center== cm2:
    destination_path= cm2_dest
    machine_center_tbl = cm2_tbl
    coil_id = "cg_Gen_CoilID"
  elif machine_center== cm3:
    destination_path= cm3_dest
    machine_center_tbl = cm3_tbl
    coil_id = "cg_Gen_CoilID"
  
  
  #query to get data from delta table with default metal code values
  query='select distinct ' + coil_id+' from '+DB+'.'+ machine_center_tbl+' where metal_code = ' + metal_code_default
  df=spark.sql(query)
  
  #define list of coils with default metal code values
  #df_coilid_list = [str(x[0]).split('.')[0] for x in df.select(coil_id).distinct().collect()]
  df_coilid_list = df.toPandas()[coil_id]
  #define list of coils in coil index table
  coilindex_coil_list = list(coil_index_df.toPandas()['coil_id'])
  #coilindex_coil_list = [x[0] for x in coil_index_df.select('coil_id').collect()]
  
  #count of machine center coils with default mc that were in coil index delta table 
  count = 0
  for i in df_coilid_list:
    if str(i).split('.')[0] in coilindex_coil_list:
      count = count+1
  
  #count of coilid's with default metal code values
  cnt_coilid = len(df_coilid_list)   
  print('Number of coil ids with default metal code in machine center',machine_center,'=',cnt_coilid)
    
  #run the update procedure only if count is greater than 0
  if count>0:
    #define delta table path
    tbl=DeltaTable.forPath(spark, destination_path)
 
    #update metal code value in machine center 
    tbl.alias("cm").merge(
      coil_index_df.alias("coil_index"),
      "cm."+coil_id +" = coil_index.coil_id and cm.plant_code = coil_index.plant_code and cm.metal_code = "+metal_code_default)\
    .whenMatchedUpdate(set = { "cm.metal_code" : "coil_index.metal_code"})\
    .execute()
  else:
    pass
  print('Number of coils that were updated = ',count)

# COMMAND ----------

#update metal code for each machine center
for i in machine_center:
  metal_code_update(i)