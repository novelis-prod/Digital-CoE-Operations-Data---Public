# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook will populate the values of sync length in the respective machine center data file.

# COMMAND ----------

# MAGIC %run /config/secret

# COMMAND ----------

#details of database connection
DL = 'daa'
adl = connection(DL)

# COMMAND ----------

# update parameters
base_path = 'adl://novelisadls.azuredatalakestore.net/'                                                             
DB = 'opsentprod'                                                         # change this if DB is changed
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


metal_code = 'metal_code'
machine_center_desc = 'machine_center_desc'
ops_seq = 'ops_seq'
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

# COMMAND ----------

df124 = spark.sql("SELECT {},{},{},{},{},{},{},{},{} FROM {}.{} WHERE {} NOT LIKE '0.0' ORDER BY {},{}".format(metal_code, machine_center_desc, ops_seq, table_name, use_index, eRg_ExitRolledLength, ex_head_pos, ex_tail_pos, eRg_Ratio, DB, coil_lineage_tbl, table_name, metal_code, ops_seq))

# COMMAND ----------

df_hrm = df124.filter(col(table_name)==DB+'.'+pba_hrm_tbl)
df_hfm = df124.filter(col(table_name)==DB+'.'+pba_hfm_tbl)
#df_cm1 = df124.filter(col(table_name)==DB+'.'+pba_cm1_tbl)
#df_cm2 = df124.filter(col(table_name)==DB+'.'+pba_cm2_tbl)
df_cm3 = df124.filter(col(table_name)==DB+'.'+pba_cm3_tbl)
 
hrm_tbl = DeltaTable.forPath(spark, base_path+publish_path+env+pba_hrm_folder)
hfm_tbl = DeltaTable.forPath(spark, base_path+publish_path+env+pba_hfm_folder)
#cm1_tbl = DeltaTable.forPath(spark, base_path+publish_path+env+cm1_folder)
#cm2_tbl = DeltaTable.forPath(spark, base_path+publish_path+env+cm2_folder)
cm3_tbl = DeltaTable.forPath(spark, base_path+publish_path+env+pba_cm3_folder)

# COMMAND ----------

# Update HRM and hrm.sync_length = 0.0
hrm_tbl.alias("hrm").merge(df_hrm.alias("df_hrm"), "hrm.metal_code = df_hrm.metal_code and df_hrm.use_index = 'eg_LengthIndex'" )\
               .whenMatchedUpdate(set = {"hrm.sync_length": 'round( ((df_hrm.ex_tail_pos - df_hrm.ex_head_pos)/df_hrm.eRg_ExitRolledLength * hrm.eg_LengthIndex + df_hrm.ex_head_pos)*4,0)/4'})\
               .execute()
hrm_tbl.alias("hrm").merge(df_hrm.alias("df_hrm"), "hrm.metal_code = df_hrm.metal_code and df_hrm.use_index = 'eg_ReverseLengthIndex'" )\
               .whenMatchedUpdate(set = {"hrm.sync_length": 'round( ((df_hrm.ex_tail_pos - df_hrm.ex_head_pos)/df_hrm.eRg_ExitRolledLength * hrm.eg_ReverseLengthIndex + df_hrm.ex_head_pos)*4,0)/4'})\
               .execute()
# Update HFM and hfm.sync_length = 0.0
hfm_tbl.alias("hfm").merge(df_hfm.alias("df_hfm"), "hfm.metal_code = df_hfm.metal_code and df_hfm.use_index = 'eg_LengthIndex'" )\
               .whenMatchedUpdate(set = {"hfm.sync_length": 'round( ((df_hfm.ex_tail_pos - df_hfm.ex_head_pos)/df_hfm.eRg_ExitRolledLength * hfm.eg_LengthIndex + df_hfm.ex_head_pos)*4,0)/4'})\
               .execute()

hfm_tbl.alias("hfm").merge(df_hfm.alias("df_hfm"), "hfm.metal_code = df_hfm.metal_code and df_hfm.use_index = 'eg_ReverseLengthIndex'" )\
               .whenMatchedUpdate(set = {"hfm.sync_length": 'round( ((df_hfm.ex_tail_pos - df_hfm.ex_head_pos)/df_hfm.eRg_ExitRolledLength * hfm.eg_ReverseLengthIndex + df_hfm.ex_head_pos)*4,0)/4'})\
               .execute()
# Update CM1
"""
cm1_tbl.alias("cm1").merge(df_cm1.alias("df_cm1"), "cm1.metal_code = df_cm1.metal_code and cm1.sync_length = 0.0 and df_cm1.use_index = 'eg_LengthIndex'")\
               .whenMatchedUpdate(set = {"cm1.sync_length": 'round( ((df_cm1.ex_tail_pos - df_cm1.ex_head_pos)/df_cm1.eRg_ExitRolledLength * cm1.eg_LengthIndex + df_cm1.ex_head_pos)*4,0)/4'})\
               .execute()
cm1_tbl.alias("cm1").merge(df_cm1.alias("df_cm1"), "cm1.metal_code = df_cm1.metal_code and df_cm1.use_index = 'eg_ReverseLengthIndex'")\
               .whenMatchedUpdate(set = {"cm1.sync_length": 'round( ((df_cm1.ex_tail_pos - df_cm1.ex_head_pos)/df_cm1.eRg_ExitRolledLength * cm1.eg_ReverseLengthIndex + df_cm1.ex_head_pos)*4,0)/4'})\
               .execute()

# Update CM2 and cm2.sync_length = 0.0 
cm2_tbl.alias("cm2").merge(df_cm2.alias("df_cm2"), "cm2.metal_code = df_cm2.metal_code and df_cm2.use_index = 'eg_LengthIndex'")\
               .whenMatchedUpdate(set = {"cm2.sync_length": 'round( ((df_cm2.ex_tail_pos - df_cm2.ex_head_pos)/df_cm2.eRg_ExitRolledLength * cm2.eg_LengthIndex + df_cm2.ex_head_pos)*4,0)/4'})\
               .execute()
cm2_tbl.alias("cm2").merge(df_cm2.alias("df_cm2"), "cm2.metal_code = df_cm2.metal_code and df_cm2.use_index = 'eg_ReverseLengthIndex'")\
               .whenMatchedUpdate(set = {"cm2.sync_length": 'round( ((df_cm2.ex_tail_pos - df_cm2.ex_head_pos)/df_cm2.eRg_ExitRolledLength * cm2.eg_ReverseLengthIndex + df_cm2.ex_head_pos)*4,0)/4'})\
               .execute()
"""
# Update CM3 and cm3.sync_length = 0.0
cm3_tbl.alias("cm3").merge(df_cm3.alias("df_cm3"), "cm3.metal_code = df_cm3.metal_code and df_cm3.use_index = 'eg_LengthIndex'")\
               .whenMatchedUpdate(set = {"cm3.sync_length": 'round( ((df_cm3.ex_tail_pos - df_cm3.ex_head_pos)/df_cm3.eRg_ExitRolledLength * cm3.eg_LengthIndex + df_cm3.ex_head_pos)*4,0)/4'})\
               .execute()
cm3_tbl.alias("cm3").merge(df_cm3.alias("df_cm3"), "cm3.metal_code = df_cm3.metal_code and df_cm3.use_index = 'eg_ReverseLengthIndex'")\
               .whenMatchedUpdate(set = {"cm3.sync_length": 'round( ((df_cm3.ex_tail_pos - df_cm3.ex_head_pos)/df_cm3.eRg_ExitRolledLength * cm3.eg_ReverseLengthIndex + df_cm3.ex_head_pos)*4,0)/4'})\
               .execute()

# COMMAND ----------

"""
for mc in df124[metal_code].unique():
  df124_mc=(df124[df124[metal_code]==mc]).sort_values(by=[ops_seq], inplace=False, ascending=True) # getting a dataframe for a metal code and then sorting it on ops_seq
  if df124_mc.shape[0]>1: # if the length of dataframe is grater than 0, then only go further else pass    
    # update sync_length column in the machine center data.
    #print(df124_mc, df124_mc.shape[0])
    for row in range(df124_mc.shape[0]):
      end_pos = df124_mc.iloc[row][5]
      process_tbl = df124_mc.iloc[row][3]
      if df124_mc.iloc[row][4]==eg_LengthIndex:
        spark.sql("UPDATE {} SET {}=round(({} * eg_LengthIndex + {})*4, 0)/4 WHERE {}={}".format(process_tbl, sync_length, (df124_mc.iloc[row][7]-df124_mc.iloc[row][6])/(end_pos-0.0), df124_mc.iloc[row][6] ,metal_code, mc))
      elif df124_mc.iloc[row][4]==eg_ReverseLengthIndex:
        spark.sql("UPDATE {} SET {}=round(({} * eg_ReverseLengthIndex + {})*4, 0)/4 WHERE {}={}".format(process_tbl, sync_length, (df124_mc.iloc[row][7]-df124_mc.iloc[row][6])/(end_pos-0.0), df124_mc.iloc[row][6] ,metal_code, mc))
      else:
        pass
"""