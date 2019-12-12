# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Overview of this notebook
# MAGIC 
# MAGIC This notebook is to read data from HRM/HFM delta tables and update metal code from coil index table.
# MAGIC 
# MAGIC • Read the data from HRM/HFM delta tables which has default metal code values(1000000000000000)
# MAGIC 
# MAGIC • Update the metal code value based on plant code and coil id from coil index table 
# MAGIC 
# MAGIC Input: All the records from HRM/HFM delta tables with default metal code value.
# MAGIC 
# MAGIC Output: Store the updated data in HRM/HFM delta table.

# COMMAND ----------

# MAGIC %run /config/secret

# COMMAND ----------

#import libraries to perform functional operations
from pyspark.sql.functions import *
from delta.tables import * 

#details of database
env = 'prod'
DB='opsentprod'

#details of coil index table
coil_index_tbl='coil_index'

#details of machine centers
plant_name = 'pinda'
hrm ='hrm'
hfm ='hfm'
machine_center = [hrm,hfm]

#details of delta tables
hrm_tbl='pba_hrm'
hfm_tbl='pba_hfm'

#details of destination folder
hrm_dest = 'adl://novelisadls.azuredatalakestore.net/enterprise/' + env + '/' + plant_name + '/hrm'
hfm_dest = 'adl://novelisadls.azuredatalakestore.net/enterprise/' + env + '/' + plant_name + '/hfm'

#metal code default value
metal_code_default = '1000000000000000'

#query to get coil id and metal code data from coil index table
query = 'select coil_id, metal_code,plant_code from '+DB+'.'+coil_index_tbl

#create a new dataframe with coil id and metal code values
coil_index_df = spark.sql(query)

# COMMAND ----------

#define a function to update metal code in delta table
def metal_code_update(machine_center):
  """ This function takes machine center as input and perform update operations on respective machine center delta table"""   
  
  #database details of each  HM machine center
  if machine_center== hrm:
    destination_path=hrm_dest
    machine_center_tbl = hrm_tbl
    coil_id ="cg_Gen_Coilid"
  elif machine_center== hfm:
    destination_path= hfm_dest
    machine_center_tbl = hfm_tbl
    coil_id = 'cg_Gen_Coilid'
  
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
    tbl.alias("hm").merge(
      coil_index_df.alias("coil_index"),
      "hm."+coil_id +" = coil_index.coil_id and hm.plant_code = coil_index.plant_code and hm.metal_code = "+metal_code_default)\
    .whenMatchedUpdate(set = { "hm.metal_code" : "coil_index.metal_code"})\
    .execute()
  else:
    pass
  print('Number of coils that were updated = ',count)

# COMMAND ----------

#update metal code for each machine center
for i in machine_center:
  metal_code_update(i)