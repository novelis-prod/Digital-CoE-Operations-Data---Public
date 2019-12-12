# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook will update the flip_length, use_index, last_process, eRg_EntryHeadDiscard and eRg_EntryTailDiscard columns present in coil lineage table from the existing data in coil lineage table. This notebook doesn't require any data other than present in coil lineage table. 

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
cm_list = ['CM1','CM2','CM3']
hfm_list = ['HFM']
hotmill_variants = ['HRM', 'HRM1', 'HRM2', 'HFM', 'HFM1', 'HFM2']
coldmill_variants = ['CM1','CM2','CM3', 'TCM3', 'CM72', 'CM88']
min_value_for_tail_position = 2.0
exit_mcc = ['SL1','SL2','SL3','SL4','SCALPER','REMELT','HMFURNACE','CMFURNACE' ,'CMFURNACE2','CMFURNACE3','CMFURNACE4','CMFURNACE5','CMFURNACE6','CMFURNACE7','CMFURNACE8','CTG&PKG', 'HEAVYSLITTER', 'LIGHTSLITTER', 'DEGRESSING', 'TENSION']

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, lag
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, StringType, DoubleType
from delta.tables import *
import pandas as pd
import numpy as np

# COMMAND ----------

# getting the fields that are required and needs to be updated from coil lineage
df122_s = spark.sql("SELECT {},{},{},{},{},{},{},{},{},{},{} FROM {}.{} WHERE {} NOT LIKE '0.0' ORDER BY {},{}".format(metal_code, machine_center_desc, ops_seq, eRg_FlipPriorLength, flip_length, use_index, eRg_EntryHeadDiscard, eRg_EntryTailDiscard, eRg_EntryRolledLength, eRg_ExitRolledLength, last_process, DB, coil_lineage_tbl, table_name, metal_code, ops_seq))

# COMMAND ----------

# updating the last process column value to 1
w1 = Window.partitionBy(df122_s[metal_code])
df122_s = df122_s.withColumn(last_process, F.when(col(ops_seq)==F.max(ops_seq).over(w1), lit(1)).otherwise(col(last_process))) # update last process column value to 1

# COMMAND ----------

#df2 = df1.withColumn('use_index', f.when(col('last_process')==1, lit('eg_LengthIndex')).otherwise(lit('0.0'))).orderBy(['metal_code','ops_seq'], ascending=False) # update use_index value of the last process column
w2 = Window.partitionBy(df122_s[metal_code]).orderBy(df122_s[ops_seq].desc())
# updating entry head discards for cold mills
#df122_s = df122_s.withColumn(eRg_EntryHeadDiscard, F.when(df122_s[machine_center_desc].isin(coldmill_variants),  lag(df122_s[eRg_ExitRolledLength], default=0).over(w2) - df122_s[eRg_EntryRolledLength]-df122_s[eRg_EntryTailDiscard]).otherwise(df122_s[eRg_EntryHeadDiscard])
                             
#w3 = Window.partitionBy(df122_s[metal_code]).orderBy(df122_s[ops_seq].desc())
df122_s = df122_s.withColumn("prev_value_fpl", lag(df122_s[eRg_FlipPriorLength], default=False).over(w2)) # creating a new column prev_value_fpl with lag with eRg_FlipPriorLength
df122_s = df122_s.withColumn('prev_value_fpl', F.when(col('prev_value_fpl')=='false', lit(0)).otherwise(lit(1))) # change new column prev_value_fpl values to 0 for false and 1 for true
df122_s.show()

# COMMAND ----------

df122_p = df122_s.toPandas()
df122_mc_new_p=pd.DataFrame({})
df122_p[flip_length]=0

# COMMAND ----------

df122_mc_new_p = pd.DataFrame({})
for mc in df122_p[metal_code].unique():
  df122_mc_p = df122_p[df122_p[metal_code]==mc].sort_values(by=[ops_seq], inplace=False, ascending=False).reset_index(drop=True)
  for row in range(df122_mc_p.shape[0]):
    if row==0:
      pass
    else:
      if df122_mc_p.iloc[row][11]^df122_mc_p.iloc[row-1][4]==1:
        df122_mc_p.loc[row, flip_length] = 1
        #df_p_mc.iloc[row][6] = 1 
      else:
        df122_mc_p.loc[row, flip_length] = 0
  
  # dropping column prev_value_fpl
  df122_mc_p = df122_mc_p.drop(['prev_value_fpl'], axis=1)
  
  # now updating entry head discards for CM and entry tail discards for HM
  df122_mc_p = df122_mc_p.sort_values(by=[ops_seq], inplace=False, ascending=True).reset_index(drop=True)
  df122_mc_p[eRg_EntryHeadDiscard] = np.where(df122_mc_p[machine_center_desc].isin(coldmill_variants), df122_mc_p[eRg_ExitRolledLength].shift()-df122_mc_p[eRg_EntryRolledLength]-df122_mc_p[eRg_EntryTailDiscard], df122_mc_p[eRg_EntryHeadDiscard])

  df122_mc_p[eRg_EntryTailDiscard] = np.where(df122_mc_p[machine_center_desc].isin(hotmill_variants), df122_mc_p[eRg_ExitRolledLength].shift()-df122_mc_p[eRg_EntryRolledLength]-df122_mc_p[eRg_EntryHeadDiscard], df122_mc_p[eRg_EntryTailDiscard])
  
  # replace nan in pandas dataframe
  df122_mc_p[eRg_EntryTailDiscard] = df122_mc_p[eRg_EntryTailDiscard].fillna(0)
  
  # append the dataframes
  df122_mc_new_p=df122_mc_new_p.append(df122_mc_p)

# convert metal_code to integer
df122_mc_new_p[metal_code]=df122_mc_new_p[metal_code].astype(int)
# get float columns
float_col_list = [x for x in df122_mc_new_p.columns if df122_mc_new_p[x].dtype == 'float64']
# round float columns
df122_mc_new_p[float_col_list] = df122_mc_new_p[float_col_list].round(2)

df122_mc_new_s = spark.createDataFrame(df122_mc_new_p)
df122_mc_new_s = df122_mc_new_s.withColumn(use_index, F.when(col(flip_length)==0, eg_LengthIndex).otherwise(eg_ReverseLengthIndex))
#df122_mc_new_s.show()

# COMMAND ----------

# path for coil lineage table
coil_lineage_tbl_path = base_path + publish_path + env + coil_lineage_folder
# table name for coil lineage
coil_lineage_dbntbl = DB + '.' + coil_lineage_tbl
# define coil lineage delta table
cl = DeltaTable.forPath(spark, coil_lineage_tbl_path)
# update new data frame to coil lineage table
cl.alias("cli").merge(df122_mc_new_s.alias("df122_mn"), "cli.metal_code = df122_mn.metal_code AND cli.ops_seq = df122_mn.ops_seq")\
               .whenMatchedUpdate(set = {"cli.flip_length": "df122_mn.flip_length", 
                                         "cli.use_index": "df122_mn.use_index",
                                         "cli.eRg_EntryHeadDiscard": "df122_mn.eRg_EntryHeadDiscard",
                                         "cli.eRg_EntryTailDiscard": "df122_mn.eRg_EntryTailDiscard",
                                         "cli.last_process": "df122_mn.last_process"
                                        })\
               .execute()