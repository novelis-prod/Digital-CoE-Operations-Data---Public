# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook will populate the values of sync length in the respective machine center data file.

# COMMAND ----------

# MAGIC %run /config/secret_gen2

# COMMAND ----------

#details of folder locations
store_name = "novelisadlsg2"
container = "plantdata"
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/" 

# update parameters                                                          
ent_db = 'opsentprodg2'                                                         # change this if DB is changed
publish_path = 'enterprise/'                                            # change this if published path is changed                                           
env = 'prod/'                                                             # change this if environment is changed
pba_hrm_folder = 'pinda/hrm'
pba_hfm_folder = 'pinda/hfm'
pba_cm1_folder = 'pinda/cm1'
pba_cm2_folder = 'pinda/cm2'
pba_cm3_folder = 'pinda/cm3'
coil_lineage_folder = 'coil_lineage'
coil_lineage_tbl = 'coil_lineage'
pba_hrm_tbl = 'pba_hrm'
pba_hfm_tbl = 'pba_hfm'
pba_cm1_tbl = 'pba_cm1'
pba_cm2_tbl = 'pba_cm2'
pba_cm3_tbl = 'pba_cm3'
pba_hrm = 'HRM'
pba_hfm = 'HFM'
pba_cm1 = 'CM1'
pba_cm2 = 'CM2'
pba_cm3 = 'CM3'
pba_pc = 663
osw_pc = 1050
yj_pc = 542

machine_center = ['HRM']
metal_code = 'metal_code'
machine_center_desc = 'machine_center_desc'
ops_seq = 'ops_seq'
machine_center_seq = 'machine_center_seq'
table_name = 'table_name'
plant_code = 'plant_code'
last_process='last_process'
eRg_EntryHeadDiscard = 'eRg_EntryHeadDiscard'
eRg_EntryTailDiscard = 'eRg_EntryTailDiscard'
eRg_ExitHeadDiscard = 'eRg_ExitHeadDiscard'
eRg_ExitTailDiscard = 'eRg_ExitTailDiscard'
eRg_EntryRolledLength = 'eRg_EntryRolledLength'
eRg_ExitRolledLength = 'eRg_ExitRolledLength'
eRg_Ratio = 'eRg_Ratio'
eRg_FlipPriorLength = 'eRg_FlipPriorLength'
flip_length='flip_length'
ex_head_pos='ex_head_pos'
ex_tail_pos='ex_tail_pos'
use_index = 'use_index'
eg_LengthIndex='eg_LengthIndex'
eg_ReverseLengthIndex='eg_ReverseLengthIndex'
sync_length='sync_length'
sync_default_value = 0.0
hrm_variants = ['HRM', 'HRM1', 'HRM2']
hfm_variants = ['HFM', 'HFM1', 'HFM2']
cm_list = ['CM1','CM2','CM3']
hfm_list = ['HFM']
hotmill_variants = ['HRM', 'HRM1', 'HRM2', 'HFM', 'HFM1', 'HFM2']
coldmill_variants = ['CM1','CM2','CM3', 'TCM3', 'CM72', 'CM88']
min_value_for_tail_position = 2.0
exit_mcc = ['SL1','SL2','SL3','SL4','SCALPER','REMELT','HMFURNACE','CMFURNACE' ,'CMFURNACE2','CMFURNACE3','CMFURNACE4','CMFURNACE5','CMFURNACE6','CMFURNACE7','CMFURNACE8','CTG&PKG', 'HEAVYSLITTER', 'LIGHTSLITTER', 'DEGRESSING', 'TENSION']

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, DoubleType
from delta.tables import *
import pandas as pd
import numpy as np
from multiprocessing.pool import ThreadPool

# COMMAND ----------

# reading data from coil_lineage table - df124 is 
df124 = spark.sql("SELECT {},{},{},{},{},{},{},{},{},{} FROM {}.{} WHERE {} NOT LIKE '0.0' ORDER BY {},{}".format(metal_code, machine_center_desc, ops_seq, machine_center_seq, table_name, use_index, eRg_ExitRolledLength, ex_head_pos, ex_tail_pos, eRg_Ratio, ent_db, coil_lineage_tbl, table_name, metal_code, ops_seq))

# COMMAND ----------

#define hrm/hfm/cm1/cm2/cm3 data frames and delta tables
df_hrm = df124.filter(col(table_name)==ent_db+'.'+pba_hrm_tbl)
df_hfm = df124.filter(col(table_name)==ent_db+'.'+pba_hfm_tbl)
df_cm1 = df124.filter(col(table_name)==ent_db+'.'+pba_cm1_tbl)
df_cm2 = df124.filter(col(table_name)==ent_db+'.'+pba_cm2_tbl)
df_cm3 = df124.filter(col(table_name)==ent_db+'.'+pba_cm3_tbl)
 
hrm_tbl = DeltaTable.forPath(spark, base_path+publish_path+env+pba_hrm_folder)
hfm_tbl = DeltaTable.forPath(spark, base_path+publish_path+env+pba_hfm_folder)
cm1_tbl = DeltaTable.forPath(spark, base_path+publish_path+env+pba_cm1_folder)
cm2_tbl = DeltaTable.forPath(spark, base_path+publish_path+env+pba_cm2_folder)
cm3_tbl = DeltaTable.forPath(spark, base_path+publish_path+env+pba_cm3_folder)

# COMMAND ----------

#update sync_length
def update_sync(month):
  print('HRM',month)
  hrm_tbl.alias("hrm").merge(df_hrm.alias("df_hrm"), "hrm.metal_code = df_hrm.metal_code and hrm.machine_center_seq = df_hrm.machine_center_seq and df_hrm.use_index = 'eg_LengthIndex' and hrm.month ="+str(month) )\
                 .whenMatchedUpdate(set = {"hrm.sync_length": 'round( ((df_hrm.ex_tail_pos - df_hrm.ex_head_pos)/df_hrm.eRg_ExitRolledLength * hrm.eg_LengthIndex + df_hrm.ex_head_pos)*4,0)/4',
                                           "hrm.data_updated": lit(1)})\
                 .execute()
  
  hrm_tbl.alias("hrm").merge(df_hrm.alias("df_hrm"), "hrm.metal_code = df_hrm.metal_code and hrm.machine_center_seq = df_hrm.machine_center_seq and df_hrm.use_index = 'eg_ReverseLengthIndex' and hrm.month ="+str(month)  )\
               .whenMatchedUpdate(set = {"hrm.sync_length": 'round( ((df_hrm.ex_tail_pos - df_hrm.ex_head_pos)/df_hrm.eRg_ExitRolledLength * hrm.eg_ReverseLengthIndex + df_hrm.ex_head_pos)*4,0)/4',
                                         "hrm.data_updated": lit(1)})\
               .execute()
  print('HFM',month)
  # Update HFM and hfm.sync_length = 0.0
  hfm_tbl.alias("hfm").merge(df_hfm.alias("df_hfm"), "hfm.metal_code = df_hfm.metal_code and hfm.machine_center_seq = df_hfm.machine_center_seq and df_hfm.use_index = 'eg_LengthIndex' and hfm.month ="+str(month) )\
               .whenMatchedUpdate(set = {"hfm.sync_length": 'round( ((df_hfm.ex_tail_pos - df_hfm.ex_head_pos)/df_hfm.eRg_ExitRolledLength * hfm.eg_LengthIndex + df_hfm.ex_head_pos)*4,0)/4',
                                         "hfm.data_updated": lit(1)})\
               .execute()
  
  hfm_tbl.alias("hfm").merge(df_hfm.alias("df_hfm"), "hfm.metal_code = df_hfm.metal_code and hfm.machine_center_seq = df_hfm.machine_center_seq and df_hfm.use_index = 'eg_ReverseLengthIndex' and hfm.month ="+str(month)  )\
               .whenMatchedUpdate(set = {"hfm.sync_length": 'round( ((df_hfm.ex_tail_pos - df_hfm.ex_head_pos)/df_hfm.eRg_ExitRolledLength * hfm.eg_ReverseLengthIndex + df_hfm.ex_head_pos)*4,0)/4',
                                         "hfm.data_updated": lit(1)})\
               .execute()
  print('CM1',month)
  # Update CM1
  cm1_tbl.alias("cm1").merge(df_cm1.alias("df_cm1"), "cm1.metal_code = df_cm1.metal_code and cm1.machine_center_seq = df_cm1.machine_center_seq and df_cm1.use_index = 'eg_LengthIndex' and cm1.month ="+str(month) )\
               .whenMatchedUpdate(set = {"cm1.sync_length": 'round( ((df_cm1.ex_tail_pos - df_cm1.ex_head_pos)/df_cm1.eRg_ExitRolledLength * cm1.eg_LengthIndex + df_cm1.ex_head_pos)*4,0)/4',
                                          "cm1.data_updated": lit(1)})\
               .execute()
  
  cm1_tbl.alias("cm1").merge(df_cm1.alias("df_cm1"), "cm1.metal_code = df_cm1.metal_code and cm1.machine_center_seq = df_cm1.machine_center_seq and df_cm1.use_index = 'eg_ReverseLengthIndex' and cm1.month ="+str(month) )\
               .whenMatchedUpdate(set = {"cm1.sync_length": 'round( ((df_cm1.ex_tail_pos - df_cm1.ex_head_pos)/df_cm1.eRg_ExitRolledLength * cm1.eg_ReverseLengthIndex + df_cm1.ex_head_pos)*4,0)/4',
                                         "cm1.data_updated": lit(1)})\
               .execute()
  print('CM2',month)
  # Update CM2 and cm2.sync_length = 0.0 
  cm2_tbl.alias("cm2").merge(df_cm2.alias("df_cm2"), "cm2.metal_code = df_cm2.metal_code and cm2.machine_center_seq = df_cm2.machine_center_seq and df_cm2.use_index = 'eg_LengthIndex' and cm2.month ="+str(month) )\
               .whenMatchedUpdate(set = {"cm2.sync_length": 'round( ((df_cm2.ex_tail_pos - df_cm2.ex_head_pos)/df_cm2.eRg_ExitRolledLength * cm2.eg_LengthIndex + df_cm2.ex_head_pos)*4,0)/4',
                                         "cm2.data_updated": lit(1)})\
               .execute()
  
  cm2_tbl.alias("cm2").merge(df_cm2.alias("df_cm2"), "cm2.metal_code = df_cm2.metal_code and cm2.machine_center_seq = df_cm2.machine_center_seq and df_cm2.use_index = 'eg_ReverseLengthIndex' and cm2.month ="+str(month) )\
               .whenMatchedUpdate(set = {"cm2.sync_length": 'round( ((df_cm2.ex_tail_pos - df_cm2.ex_head_pos)/df_cm2.eRg_ExitRolledLength * cm2.eg_ReverseLengthIndex + df_cm2.ex_head_pos)*4,0)/4',
                                         "cm2.data_updated": lit(1)})\
               .execute()
  print('CM3',month)
  # Update CM3 and cm3.sync_length = 0.0 and day < 15
  cm3_tbl.alias("cm3").merge(df_cm3.alias("df_cm3"), "cm3.metal_code = df_cm3.metal_code and cm3.machine_center_seq = df_cm3.machine_center_seq and df_cm3.use_index = 'eg_LengthIndex' and cm3.month ="+str(month) + ' and cm3.day < 15' )\
               .whenMatchedUpdate(set = {"cm3.sync_length": 'round( ((df_cm3.ex_tail_pos - df_cm3.ex_head_pos)/df_cm3.eRg_ExitRolledLength * cm3.eg_LengthIndex + df_cm3.ex_head_pos)*4,0)/4',
                                         "cm3.data_updated": lit(1)})\
               .execute()
  
  cm3_tbl.alias("cm3").merge(df_cm3.alias("df_cm3"), "cm3.metal_code = df_cm3.metal_code and cm3.machine_center_seq = df_cm3.machine_center_seq and df_cm3.use_index = 'eg_ReverseLengthIndex' and cm3.month ="+str(month) + ' and cm3.day < 15' )\
               .whenMatchedUpdate(set = {"cm3.sync_length": 'round( ((df_cm3.ex_tail_pos - df_cm3.ex_head_pos)/df_cm3.eRg_ExitRolledLength * cm3.eg_ReverseLengthIndex + df_cm3.ex_head_pos)*4,0)/4',
                                         "cm3.data_updated": lit(1)})\
               .execute()
  # Update CM3 and cm3.sync_length = 0.0 and day >= 15
  cm3_tbl.alias("cm3").merge(df_cm3.alias("df_cm3"), "cm3.metal_code = df_cm3.metal_code and cm3.machine_center_seq = df_cm3.machine_center_seq and df_cm3.use_index = 'eg_LengthIndex' and cm3.month ="+str(month) + ' and cm3.day >= 15' )\
               .whenMatchedUpdate(set = {"cm3.sync_length": 'round( ((df_cm3.ex_tail_pos - df_cm3.ex_head_pos)/df_cm3.eRg_ExitRolledLength * cm3.eg_LengthIndex + df_cm3.ex_head_pos)*4,0)/4',
                                         "cm3.data_updated": lit(1)})\
               .execute()
  
  cm3_tbl.alias("cm3").merge(df_cm3.alias("df_cm3"), "cm3.metal_code = df_cm3.metal_code and cm3.machine_center_seq = df_cm3.machine_center_seq and df_cm3.use_index = 'eg_ReverseLengthIndex' and cm3.month ="+str(month) + ' and cm3.day >= 15' )\
               .whenMatchedUpdate(set = {"cm3.sync_length": 'round( ((df_cm3.ex_tail_pos - df_cm3.ex_head_pos)/df_cm3.eRg_ExitRolledLength * cm3.eg_ReverseLengthIndex + df_cm3.ex_head_pos)*4,0)/4',
                                         "cm3.data_updated": lit(1)})\
               .execute()

# COMMAND ----------

#update sync length
pool = ThreadPool(5) #define number of threads to run parallelly 
for month in range (1,13):
  #pool.map(lambda mc : update_sync(mc,month),machine_center)
  update_sync(month)