# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook will read the process data from respective plant code and machine centers, and then store that in coil lineage table. The process data that we will store in coil lineage table are related to entry and exit discards, width, rolled length and flips.
# MAGIC 
# MAGIC •••input : table names, plant codes, and coil lineage path ••• output: updated coil lineage table with discard lengths, rolled lengths, width etc

# COMMAND ----------

# MAGIC %run /config/secret_gen2

# COMMAND ----------

#details of folder locations
store_name = "novelisadlsg2"
container = "plantdata"
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/" 

# update parameters               
ent_db = 'opsentprodg2'                                                         # change this if DB is changed
publish_path = 'enterprise/'                                             # change this if published path is changed                                           
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
machine_center_seq = 'machine_center_seq'
machine_center_desc = 'machine_center_desc'
ops_seq = 'ops_seq'
table_name = 'table_name'
plant_code = 'plant_code'
last_process='last_process'
eRg_EntryThickness = 'eRg_EntryThickness'
eRg_ExitThickness = 'eRg_ExitThickness'
eRg_EntryHeadDiscard = 'eRg_EntryHeadDiscard'
eRg_EntryTailDiscard = 'eRg_EntryTailDiscard'
eRg_ExitHeadDiscard = 'eRg_ExitHeadDiscard'
eRg_ExitTailDiscard = 'eRg_ExitTailDiscard'
eRg_EntryRolledLength = 'eRg_EntryRolledLength'
eRg_ExitRolledLength = 'eRg_ExitRolledLength'
eRg_Ratio = 'eRg_Ratio'
eRg_FlipPriorLength = 'eRg_FlipPriorLength'
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
exit_mcc = ['SLITTER','SCALPER','REMELT','HM_AUX','CM_AUX','CTG&PKG','TENSION']

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit

from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# passing the right table name to the to coil_lineage table, input is plant code and machine center code and the output is the table name and machine center code  in adl for a machine center
def get_process_table(plant_code, machine_center_code):
  if ((plant_code==pba_pc) & (machine_center_code==pba_hrm)):
    process_tbl = ent_db + '.' + pba_hrm_tbl
    mcc = pba_hrm
  
  elif ((plant_code==pba_pc) & (machine_center_code==pba_hfm)):
    process_tbl = ent_db + '.' + pba_hfm_tbl
    mcc = pba_hfm
    
  elif ((plant_code==pba_pc) & (machine_center_code==pba_cm1)):
    process_tbl = ent_db + '.' + pba_cm1_tbl
    mcc = pba_cm1
    
  elif ((plant_code==pba_pc) & (machine_center_code==pba_cm2)):
    process_tbl = ent_db + '.' + pba_cm2_tbl
    mcc = pba_cm2
    
  elif ((plant_code==pba_pc) & (machine_center_code==pba_cm3)):
    process_tbl = ent_db + '.' + pba_cm3_tbl#, base_path + publish_path + env + cm3_folder    
    mcc = pba_cm3
    
  else:
    process_tbl = None
  return process_tbl


# getting the process data to be added to coil lineage table; input is plant code and machine center code and the output is machine center specific process data
def get_process_data(plant_code, machine_center_code):
  process_tbl = get_process_table(plant_code, machine_center_code)
  mcc = machine_center_code 
  if process_tbl==None:
    return None
  else:
    
    process_df = spark.sql(""" SELECT tbl_a.metal_code,tbl_a.eRg_EntryThickness, tbl_a.eRg_ExitThickness, tbl_a.eRg_EntryHeadDiscard, tbl_a.eRg_EntryTailDiscard, tbl_a.eRg_ExitHeadDiscard, tbl_a.eRg_ExitTailDiscard, tbl_a.eRg_EntryRolledLength, tbl_a.eRg_ExitRolledLength, tbl_a.eRg_Ratio, tbl_a.eRg_FlipPriorLength, tbl_a.machine_center_seq
FROM
(
SELECT metal_code,eRg_EntryThickness, eRg_ExitThickness, eRg_EntryHeadDiscard, eRg_EntryTailDiscard, eRg_ExitHeadDiscard, eRg_ExitTailDiscard, eRg_EntryRolledLength, eRg_ExitRolledLength, eRg_Ratio, eRg_FlipPriorLength,machine_center_seq,count(*) AS ct 
FROM {} 
WHERE metal_code > 1000000000000000 
GROUP BY metal_code,eRg_EntryThickness, eRg_ExitThickness, eRg_EntryHeadDiscard, eRg_EntryTailDiscard, eRg_ExitHeadDiscard, eRg_ExitTailDiscard, eRg_EntryRolledLength, eRg_ExitRolledLength, eRg_Ratio, eRg_FlipPriorLength,machine_center_seq
) tbl_a
JOIN
(
SELECT metal_code,machine_center_seq, MAX(ct) AS max_ct
FROM
(
SELECT metal_code,eRg_EntryThickness, eRg_ExitThickness, eRg_EntryHeadDiscard, eRg_EntryTailDiscard, eRg_ExitHeadDiscard, eRg_ExitTailDiscard, eRg_EntryRolledLength, eRg_ExitRolledLength, eRg_Ratio, eRg_FlipPriorLength,machine_center_seq,count(*) AS ct 
FROM {} 
WHERE metal_code > 1000000000000000 
GROUP BY metal_code, eRg_EntryThickness, eRg_ExitThickness, eRg_EntryHeadDiscard, eRg_EntryTailDiscard, eRg_ExitHeadDiscard, eRg_ExitTailDiscard, eRg_EntryRolledLength, eRg_ExitRolledLength, eRg_Ratio, eRg_FlipPriorLength,machine_center_seq
)
GROUP BY metal_code,machine_center_seq
) tbl_b
ON
tbl_a.metal_code=tbl_b.metal_code AND tbl_a.machine_center_seq =tbl_b.machine_center_seq AND tbl_a.ct=tbl_b.max_ct
""".format(process_tbl, process_tbl))
     
    if process_df.count()==0:
      return None
    else:
      process_df = process_df.na.fill(0)
      process_df = process_df.withColumn('machine_center_desc', lit(mcc))
      process_df = process_df.withColumn('process_table_name', lit(process_tbl))
      return process_df

# COMMAND ----------

# get unique combination of plant_code and machine_center from coil_lineage table 
df121 = spark.sql("SELECT {},{} FROM {}.{} GROUP BY {},{}".format( plant_code, machine_center_desc, ent_db, coil_lineage_tbl, plant_code, machine_center_desc))
# filter dataframe to the machine centers not present in exit_mcc list
df121 = df121.filter(~col(machine_center_desc).isin(exit_mcc)) # getting machine centers that are not present in exit_mcc list

# COMMAND ----------

# define an empty dataframe process_df
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, FloatType, BooleanType, StringType

schema121 = StructType([StructField(metal_code,LongType(),True), 
                     StructField(eRg_EntryThickness,FloatType(),False), 
                     StructField(eRg_ExitThickness,FloatType(),False), 
                     StructField(eRg_EntryHeadDiscard,FloatType(),False), 
                     StructField(eRg_EntryTailDiscard,FloatType(),False), 
                     StructField(eRg_ExitHeadDiscard,FloatType(),False), 
                     StructField(eRg_ExitTailDiscard,FloatType(),False), 
                     StructField(eRg_EntryRolledLength,FloatType(),False), 
                     StructField(eRg_ExitRolledLength,FloatType(),False), 
                     StructField(eRg_Ratio,FloatType(),False), 
                     StructField(eRg_FlipPriorLength,BooleanType(),True),
                     StructField(machine_center_seq,StringType(),False),
                     StructField(machine_center_desc,StringType(),False),
                     StructField('process_table_name',StringType(),False)])

process_df = spark.createDataFrame([], schema121)

# COMMAND ----------

# remove this for loop by combining all the process_df from different combination of plant code and metal code and then in one shot update coil lineage table instead of updating in every loop
for i in range(df121.count()):
  pc=df121.collect()[i][0]
  mcc=df121.collect()[i][1]
  process_df0 = get_process_data(pc, mcc) # get the process data for a specific plant code and machine center code
  process_tbl = get_process_table(pc, mcc) # get the process table name for a specific plant code and machine center code
  #process_df.show()
  print(pc, mcc, process_tbl) # print plant code, machine center code and process table name to verify that the combination is correct.
  if process_tbl==None:
    pass
  else:
    if process_df0.count()==0:
      pass
    else:
      process_df = process_df.union(process_df0)
      #print(process_df0.schema.fields)

# COMMAND ----------

# path for coil lineage table
coil_lineage_tbl_path = base_path + publish_path + env + coil_lineage_folder # get the coil lineage table path so that it can be updated with the process data
# define coil lineage delta table
cl = DeltaTable.forPath(spark, coil_lineage_tbl_path) # defining the delta table name as coil lineage for the coil lineage path
# update coil lineage delta table with process data
cl.alias("cli").merge(process_df.alias("pr"), "cli.metal_code = pr.metal_code AND cli.machine_center_desc = pr.machine_center_desc AND cli.machine_center_seq = pr.machine_center_seq and cli.data_updated = 0 and cli.files_mismatch = 0")\
           .whenMatchedUpdate(set = {"cli.table_name": "pr.process_table_name", 
                                     "cli.eRg_EntryThickness": "pr.eRg_EntryThickness", 
                                     "cli.eRg_ExitThickness" : "pr.eRg_ExitThickness", 
                                     "cli.eRg_EntryHeadDiscard": "pr.eRg_EntryHeadDiscard", 
                                     "cli.eRg_EntryTailDiscard": "pr.eRg_EntryTailDiscard", 
                                     "cli.eRg_ExitHeadDiscard": "pr.eRg_ExitHeadDiscard", 
                                     "cli.eRg_ExitTailDiscard": "pr.eRg_ExitTailDiscard", 
                                     "cli.eRg_EntryRolledLength": "pr.eRg_EntryRolledLength", 
                                     "cli.eRg_ExitRolledLength": "pr.eRg_ExitRolledLength", 
                                     "cli.eRg_Ratio": "pr.eRg_Ratio", 
                                     "cli.eRg_FlipPriorLength": "pr.eRg_FlipPriorLength",
                                     "cli.data_updated":lit(1)})\
           .execute()

# COMMAND ----------

cols_to_replace_null_with_0 = [eRg_EntryThickness, eRg_ExitThickness, eRg_EntryHeadDiscard, eRg_EntryTailDiscard, eRg_ExitHeadDiscard, eRg_ExitTailDiscard, eRg_EntryRolledLength, eRg_ExitRolledLength, eRg_Ratio]
for i in cols_to_replace_null_with_0:
  spark.sql("""UPDATE {}.{} SET {} = 0 WHERE {} IS NULL""".format(ent_db, coil_lineage_tbl, i, i))