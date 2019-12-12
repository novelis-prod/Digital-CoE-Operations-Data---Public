# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook will update ex_head_pos and ex_tail_pos in coil lineage table for each of row that has machine center data available. 

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

df123 = spark.sql("SELECT {},{},{},{},{},{},{},{},{},{},{},{},{} FROM {}.{} WHERE {} NOT LIKE '0.0' ORDER BY {},{}".format(metal_code, machine_center_desc, ops_seq, use_index, eRg_EntryHeadDiscard, eRg_EntryTailDiscard, eRg_ExitHeadDiscard, eRg_ExitTailDiscard, eRg_EntryRolledLength, eRg_ExitRolledLength, ex_head_pos, ex_tail_pos, eRg_Ratio, DB, coil_lineage_tbl, table_name, metal_code, ops_seq))
df123 = df123.toPandas()

# COMMAND ----------

df123_mc_new=pd.DataFrame({})
for i in df123[metal_code].unique():
  #i=1000000000000003
  df123_mc=(df123[df123[metal_code]==i]).sort_values(by=[ops_seq], inplace=False, ascending=True) # getting a dataframe for a metal code and then sorting it on ops_seq
  if df123_mc.shape[0]>1: # if the length of dataframe is greater than 1, then only go further else pass    
    # updating ex_head_pos for hrm and hfm
    df123_mc[ex_head_pos] = np.where(df123_mc[machine_center_desc].isin(hrm_variants), 0.0 , df123_mc[ex_head_pos])
    
    df123_mc[ex_head_pos] = np.where(df123_mc[machine_center_desc].isin(hfm_variants), df123_mc[eRg_EntryHeadDiscard].shift() * np.prod(df123_mc[eRg_Ratio]) +  df123_mc[eRg_ExitHeadDiscard].shift() * np.prod(df123_mc[eRg_Ratio][1:]) + df123_mc[eRg_EntryHeadDiscard] * np.prod(df123_mc[eRg_Ratio][1:]) + df123_mc[eRg_ExitHeadDiscard] * np.prod(df123_mc[eRg_Ratio][2:]), df123_mc[ex_head_pos])
    
    # updating ex_tail_pos for hrm and hfm
    df123_mc[ex_tail_pos] = np.where(df123_mc[machine_center_desc].isin(hrm_variants), df123_mc[eRg_EntryRolledLength] * np.prod(df123_mc[eRg_Ratio]) , df123_mc[ex_tail_pos])
    df123_mc[ex_tail_pos] = np.where(df123_mc[machine_center_desc].isin(hfm_variants), df123_mc[ex_head_pos] + df123_mc[eRg_ExitRolledLength] * np.prod(df123_mc[eRg_Ratio][2:]), df123_mc[ex_tail_pos])
       
    # updating ex_head_pos for cold mills
    df123_mc[ex_head_pos] = np.where( ((df123_mc[machine_center_desc].isin(coldmill_variants)) & (df123_mc[use_index]==eg_ReverseLengthIndex)), df123_mc[ex_head_pos].shift() + df123_mc[eRg_EntryTailDiscard] * df123_mc[eRg_Ratio] + df123_mc[eRg_ExitTailDiscard], df123_mc[ex_head_pos])
    
    df123_mc[ex_head_pos] = np.where( ((df123_mc[machine_center_desc].isin(coldmill_variants)) & (df123_mc[use_index]==eg_LengthIndex)), df123_mc[ex_head_pos].shift() + df123_mc[eRg_EntryHeadDiscard] * df123_mc[eRg_Ratio] + df123_mc[eRg_ExitHeadDiscard], df123_mc[ex_head_pos])
    
    # updating ex_tail_pos for cold mills
    #df123_mc[ex_tail_pos] = np.where(  ((df123_mc[machine_center_desc].isin(coldmill_variants)) & (df123_mc[use_index]==eg_ReverseLengthIndex)), df123_mc[ex_head_pos] + df123_mc[eRg_ExitRolledLength],  df123_mc[ex_tail_pos])
    df123_mc[ex_tail_pos] = np.where(df123_mc[machine_center_desc].isin(coldmill_variants), df123_mc[ex_head_pos] + df123_mc[eRg_ExitRolledLength],  df123_mc[ex_tail_pos])


    # appending dataframe for each metal code into a bigger dataframe    
    df123_mc_new = df123_mc_new.append(df123_mc)
  else:
    pass
 

 
# replace nan in pandas dataframe
# df122_mc_new[eRg_EntryTailDiscard] = df122_mc_new[eRg_EntryTailDiscard].fillna(0)
# convert metal_code to integer
# df122_mc_new[metal_code]=df122_mc_new[metal_code].astype(int)
# get float columns
float_col_list = [x for x in df123_mc_new.columns if df123_mc_new[x].dtype == 'float64']
# round float columns
df123_mc_new[float_col_list] = df123_mc_new[float_col_list].round(2)
#converting pandas dataframe into spark dataframe
df123_mc_new = spark.createDataFrame(df123_mc_new)
#df123_mc_new.show()

# COMMAND ----------

# path for coil lineage table
coil_lineage_tbl_path = base_path + publish_path + env + coil_lineage_folder
# table name for coil lineage
coil_lineage_dbntbl = DB + '.' + coil_lineage_tbl
# define coil lineage delta table
cl = DeltaTable.forPath(spark, coil_lineage_tbl_path)
# update new data frame to coil lineage table
cl.alias("cli").merge(df123_mc_new.alias("df123_mn"), "cli.metal_code = df123_mn.metal_code AND cli.ops_seq = df123_mn.ops_seq")\
               .whenMatchedUpdate(set = {"cli.ex_head_pos": "df123_mn.ex_head_pos", 
                                         "cli.ex_tail_pos": "df123_mn.ex_tail_pos"
                                        })\
               .execute()