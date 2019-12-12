# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook will store the values of sync_length in the process data after calculation from coil lineage table. 
# MAGIC 
# MAGIC • If table names are already present in the coil lineage table for a metal code for CASH, CM and HM, then it will exit the loop, otherwise, it will continue the loop.

# COMMAND ----------

# MAGIC %run /config/secret

# COMMAND ----------

#details of database connection
DL = 'daa'
adl = connection(DL)

# COMMAND ----------

# update parameters
DB = 'opsentprod'
tbl_coil_lineage = 'coil_lineage'
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
min_value_for_tail_position = 5.0
exit_mcc = ['SL1','SL2','SL3','SL4','SCALPER','REMELT','HMFURNACE','CMFURNACE' ,'CMFURNACE2','CMFURNACE3','CMFURNACE4','CMFURNACE5','CMFURNACE6','CMFURNACE7','CMFURNACE8','CTG&PKG', 'HEAVYSLITTER', 'LIGHTSLITTER', 'DEGRESSING' 'TENSION']

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql.functions import *

# COMMAND ----------

# passing the right table name to the to coil_lineage table, input is plant code and machine center code and the output is the table name for a machine center

def return_table(plant_code, machine_center_code):
  
  pba_pc = 663
  osw_pc = 1050
  yj_pc = 773

  pba_hrm = 'HRM'
  pba_hrm_tbl = 'opsentprod.pba_hrm'
  pba_hfm = 'HFM'
  pba_hfm_tbl = 'opsentprod.pba_hfm'
  pba_cm1 = 'CM1'
  pba_cm1_tbl = 'opsentprod.pba_cm1'
  pba_cm2 = 'CM2'
  pba_cm2_tbl = 'opsentprod.pba_cm2'
  pba_cm3 = 'CM3'
  pba_cm3_tbl = 'opsentprod.pba_cm3'


  osw_hrm = 'HRM'
  osw_hrm_tbl = 'osw_hrm'
  osw_hfm = 'HFM'
  osw_hfm_tbl = 'osw_hfm'
  osw_cm1 = 'CM72'
  osw_cm1_tbl = 'osw_cm1'
  osw_cm2 = 'CM88'
  osw_cm2_tbl = 'osw_cm2'
  
  
  if plant_code==pba_pc:
    if machine_center_code==pba_hrm:
      return pba_hrm_tbl
    elif machine_center_code==pba_hfm:
      return pba_hfm_tbl      
    elif machine_center_code==pba_cm1:
      #return pba_cm1_tbl
      pass
    elif machine_center_code==pba_cm2:
      #return pba_cm2_tbl
      pass
    elif machine_center_code==pba_cm3:
      return pba_cm3_tbl    
    else:
      pass
  elif plant_code==osw_pc:
    if machine_center_code==osw_hrm:
      return osw_hrm_tbl
    elif machine_center_code==osw_hfm:
      return osw_hfm_tbl
    elif machine_center_code==osw_cm1:
      return osw_cm1_tbl
    elif machine_center_code==osw_cm2:
      return osw_cm2_tbl
    else:
      pass
  else:
    pass 

# COMMAND ----------

# this code will get the process center data from machines and add it to coil lineage table
for mc in [x[0] for x in spark.sql('SELECT {} FROM {}.{} GROUP BY {}'.format(metal_code, DB, tbl_coil_lineage, metal_code)).collect()]:
  # Getting dataframe where table_name column is filled for a mc
  exit_df = spark.sql("SELECT * FROM {}.{} WHERE {}={} AND {} NOT LIKE '0.0' ORDER BY {}".format(DB, tbl_coil_lineage, metal_code, mc, table_name, ops_seq)) # build upon this
  # Getting full dataframe for a mc
  full_df_for_mc = spark.sql('SELECT * FROM {}.{} WHERE {} = {} ORDER BY {}'.format(DB, tbl_coil_lineage, metal_code, mc, ops_seq))
  for mcc in [x[0] for x in full_df_for_mc.select(machine_center_desc).collect()]:
    if mcc not in exit_mcc:
      print(mc, mcc)
      # condition to continue the loop - either the mcc is not in the list of mcc where table_name is filled, or there is no mcc where table_name is filled
      #if ((mcc not in [x[0] for x in exit_df.select(machine_center_desc).collect()]) | (exit_df.count()==0)):
      # get the plant code from full dataframe
      pc = full_df_for_mc.collect()[0][1] 
      # logic to check if the table is returned or not
      if return_table(pc, mcc):
        process_table_name = return_table(pc, mcc)
        # get the process data from the respective machine center to be updated in coil lineage
        partial_df = spark.sql('SELECT eRg_EntryThickness, eRg_ExitThickness, eRg_EntryHeadDiscard, eRg_EntryTailDiscard, eRg_ExitHeadDiscard, eRg_ExitTailDiscard, eRg_EntryRolledLength, eRg_ExitRolledLength, eRg_Ratio, eRg_FlipPriorLength FROM {} WHERE {}={} LIMIT 1'.format(process_table_name, metal_code, mc))
        # Check if partial_df is not empty
        if partial_df.count()>0:
          # filling all the blanks with 0 in partial dataframe
          partial_df = partial_df.na.fill(0)
          partial_df.show()
          # update the coil lineage table with values from mchine center data
          spark.sql("UPDATE {}.{} SET {}='{}', {}={}, {}={}, {}={}, {}={}, {}={}, {}={}, {}={}, {}={}, {}={}, {}={} WHERE {}={} AND {}='{}'".format(DB, tbl_coil_lineage, 'table_name', process_table_name, 'eRg_EntryThickness', partial_df.collect()[0][0], 'eRg_ExitThickness', partial_df.collect()[0][1], 'eRg_EntryHeadDiscard', partial_df.collect()[0][2], 'eRg_EntryTailDiscard', partial_df.collect()[0][3], 'eRg_ExitHeadDiscard', partial_df.collect()[0][4], 'eRg_ExitTailDiscard', partial_df.collect()[0][5], 'eRg_EntryRolledLength', partial_df.collect()[0][6], 'eRg_ExitRolledLength', partial_df.collect()[0][7], 'eRg_Ratio', partial_df.collect()[0][8], 'eRg_FlipPriorLength', partial_df.collect()[0][9], metal_code, mc, machine_center_desc, mcc))
          
          
  
  # Code to update the last_process column with value 1
  df_with_tables = spark.sql("SELECT * FROM {}.{} WHERE {} = {} AND {} NOT LIKE '0.0' ORDER BY {}".format(DB, tbl_coil_lineage, metal_code, mc, table_name, ops_seq))
  if df_with_tables.count()>1:
    max_os = df_with_tables.agg(max(col(ops_seq))).collect()[0][0] # finding the max operation sequence from ops seq column    
    spark.sql("UPDATE {}.{} SET {}=0 WHERE {}={}".format(DB, tbl_coil_lineage, last_process, metal_code, mc)) # setting all the values of last_process to 0
    spark.sql("UPDATE {}.{} SET {}=1 WHERE {}={} AND {}={}".format(DB, tbl_coil_lineage, last_process, metal_code, mc, ops_seq, max_os)) # updating the last_process column value to 1 for max ops_seq
    
    
    display(df_with_tables)
    # update use_index in coil lineage  
    spark.sql("UPDATE {}.{} SET {}='LengthIndex' WHERE {}={} AND {} NOT LIKE '0.0' AND {}='false'".format(DB, tbl_coil_lineage, use_index, metal_code, mc, table_name, eRg_FlipPriorLength))
    spark.sql("UPDATE {}.{} SET {}='ReverseLengthIndex' WHERE {}={} AND {} NOT LIKE '0.0' AND {}='true'".format(DB, tbl_coil_lineage, use_index, metal_code, mc, table_name, eRg_FlipPriorLength))
    
    # update the entry head discard for CMs and entry tail deiscard for HFM
    for i in [x[0] for x in df_with_tables.select(ops_seq).orderBy(ops_seq, ascending=False).collect()]:
      mcc_for_las_pass = df_with_tables.filter(col(ops_seq) == i).select(machine_center_desc).collect()[0][0]
      if mcc_for_las_pass not in hrm_variants:
        if df_with_tables.agg(min(col(ops_seq))).collect()[0][0] !=i:
          print (mc, mcc, i)
          entry_rolled_length_current_process = df_with_tables.filter(col(ops_seq) == i).select(eRg_EntryRolledLength).collect()[0][0]
          entry_tail_discard_current_process  = df_with_tables.filter(col(ops_seq) == i).select(eRg_EntryTailDiscard).collect()[0][0]
          entry_head_discard_current_process  = df_with_tables.filter(col(ops_seq) == i).select(eRg_EntryHeadDiscard).collect()[0][0]
          try:
            exit_rolled_length_current_process_minus_one = df_with_tables.filter(col(ops_seq) == i-1).select(eRg_ExitRolledLength).collect()[0][0]
          except:
            exit_rolled_length_current_process_minus_one = df_with_tables.filter(col(ops_seq) == i-2).select(eRg_ExitRolledLength).collect()[0][0]

          # checking if this is a cold mill
          if mcc_for_las_pass in (cm_list):
            spark.sql("UPDATE {}.{} SET {}={}-{}-{} WHERE {}={} AND {}={}".format(DB, tbl_coil_lineage, eRg_EntryHeadDiscard, exit_rolled_length_current_process_minus_one, entry_rolled_length_current_process, entry_tail_discard_current_process, metal_code, mc, ops_seq, i))
          # checking if this is a hot mill
          elif mcc_for_las_pass in (hfm_list): 
            spark.sql("UPDATE {}.{} SET {}={}-{}-{} WHERE {}={} AND {}={}".format(DB, tbl_coil_lineage, eRg_EntryTailDiscard, exit_rolled_length_current_process_minus_one, entry_rolled_length_current_process, entry_head_discard_current_process, metal_code, mc, ops_seq, i))  
      
    
    
  # # update exit start position, exit end position, and add sync_length to machine centers
  df_with_discards_updated = spark.sql(" SELECT * FROM {}.{} WHERE {}={} AND {} NOT LIKE '0.0' ORDER BY {}".format(DB, tbl_coil_lineage, metal_code, mc, table_name, ops_seq))
  if df_with_discards_updated.count()>1:
    min_ex_tail_pos = df_with_discards_updated.agg(min(col(ex_tail_pos))).collect()[0][0] # finding the min value from ex_tail_pos column
    if min_ex_tail_pos<min_value_for_tail_position:
      rows = df_with_discards_updated.count()
      columns = len(df_with_discards_updated.columns)
      hd = {} # dict for head discard
      rl = {} # dict for rolled length
      td = {} # dict for tail discard
      ratio = {} # dict for ratio
      coil_length = 0.0 # initiating coil_length to 0.0

      # number of hot mills
      hotmill=0
      coldmill=0
      for mcc in [x[0] for x in df_with_discards_updated.select(machine_center_desc).collect()]:
        if mcc in hotmill_variants:
          hotmill = hotmill + 1
        elif mcc in coldmill_variants:
          coldmill = coldmill + 1

      # when hotmill are two - hrm and hfm and no cold mill
      if ((hotmill==2)&(coldmill==0)):
        for row in range(rows):
          hd['hd'+str(row)] = (df_with_discards_updated.collect()[row][11] * df_with_discards_updated.collect()[row][17]) + df_with_discards_updated.collect()[row][13]
          rl['rl'+str(row)] = df_with_discards_updated.collect()[row][16]
          ratio['r'+str(row)] = df_with_discards_updated.collect()[row][17]
          td['td'+str(row)] = (df_with_discards_updated.collect()[row][12] * df_with_discards_updated.collect()[row][17]) + df_with_discards_updated.collect()[row][14]

        hd['hd1'] = hd['hd1'] + hd['hd0']*ratio['r1'] # updating the hd1 values    
        td['td1'] = td['td1'] + td['td0']*ratio['r1'] # updating the td1 values
        coil_length = rl['rl1'] + hd['hd1'] + td['td1']

        # update ex_head_pos and ex_tail_pos for HRM and HFM
        spark.sql("UPDATE {}.{} SET {}={},{}={} WHERE {}={} AND {}='{}'".format(DB, tbl_coil_lineage, ex_head_pos, 0.0, ex_tail_pos, coil_length, metal_code, mc, machine_center_desc, [x[0] for x in df_with_discards_updated.select(machine_center_desc).collect()][0]))
        spark.sql("UPDATE {}.{} SET {}={},{}={} WHERE {}={} AND {}='{}'".format(DB, tbl_coil_lineage, ex_head_pos, hd['hd1'], ex_tail_pos, hd['hd1']+rl['rl1'], metal_code, mc, machine_center_desc, [x[0] for x in df_with_discards_updated.select(machine_center_desc).collect()][1]))

        # update sync_length column in HRM and HFM data.
        df_updated = spark.sql(" SELECT * FROM {}.{} WHERE {}={} AND {} NOT LIKE '0.0' ORDER BY {}".format(DB, tbl_coil_lineage, metal_code, mc, table_name, ops_seq))
        for row in range(rows):        
          tbl_process = df_updated.collect()[row][8]
          #start_pos = spark.sql("SELECT MIN({}) FROM {} WHERE {}={}".format(eg_LengthIndex, tbl_process, metal_code, mc)).collect()[0][0]
          end_pos = spark.sql("SELECT MAX({}) FROM {} WHERE {}={}".format(eg_LengthIndex, tbl_process, metal_code, mc)).collect()[0][0]
          spark.sql("UPDATE {} SET {}=round(({} * eg_LengthIndex + {})*4 ,0)/4 WHERE {}={}".format(tbl_process, sync_length, (df_updated.collect()[row][21]-df_updated.collect()[row][20])/(end_pos-0.0), df_updated.collect()[row][20] ,metal_code, mc))


      # when hotmills are two - hrm and hfm and cold mill is one
      if ((hotmill==2)&(coldmill==1)):
        for row in range(rows):
          if [x[0] for x in df_with_discards_updated.select(machine_center_desc).collect()][row] in hotmill_variants:
            hd['hd'+str(row)] = (df_with_discards_updated.collect()[row][11] * df_with_discards_updated.collect()[row][17]) + df_with_discards_updated.collect()[row][13]
            rl['rl'+str(row)] = df_with_discards_updated.collect()[row][16]
            ratio['r'+str(row)] = df_with_discards_updated.collect()[row][17]
            td['td'+str(row)] = (df_with_discards_updated.collect()[row][12] * df_with_discards_updated.collect()[row][17]) + df_with_discards_updated.collect()[row][14]
          elif [x[0] for x in df_with_discards_updated.select(machine_center_desc).collect()][row] in coldmill_variants:
            hd['hd'+str(row)] = (df_with_discards_updated.collect()[row][12] * df_with_discards_updated.collect()[row][17]) + df_with_discards_updated.collect()[row][14]
            rl['rl'+str(row)] = df_with_discards_updated.collect()[row][16]
            ratio['r'+str(row)] = df_with_discards_updated.collect()[row][17]
            td['td'+str(row)] = (df_with_discards_updated.collect()[row][11] * df_with_discards_updated.collect()[row][17]) + df_with_discards_updated.collect()[row][13]

        hd['hd1'] = hd['hd1'] + hd['hd0']*ratio['r1'] # updating the hd1 and hd2 values
        hd['hd2'] = hd['hd2'] + hd['hd1']*ratio['r2']

        td['td1'] = td['td1'] + td['td0']*ratio['r1'] # updating the td1 and td2 values
        td['td2'] = td['td2'] + td['td1']*ratio['r2']
        coil_length = rl['rl2'] + hd['hd2'] + td['td2']

        # update min and max for processes
        spark.sql("UPDATE {}.{} SET {}={},{}={} WHERE {}={} AND {}='{}'".format(DB, tbl_coil_lineage, ex_head_pos, 0.0, ex_tail_pos, coil_length, metal_code, mc, machine_center_desc, [x[0] for x in df_with_discards_updated.select(machine_center_desc).collect()][0]))
        spark.sql("UPDATE {}.{} SET {}={},{}={} WHERE {}={} AND {}='{}'".format(DB, tbl_coil_lineage, ex_head_pos, hd['hd1']*ratio['r2'], ex_tail_pos, (hd['hd1']+rl['rl1'])*ratio['r2'], metal_code, mc, machine_center_desc, [x[0] for x in df_with_discards_updated.select(machine_center_desc).collect()][1]))
        spark.sql("UPDATE {}.{} SET {}={},{}={} WHERE {}={} AND {}='{}'".format(DB, tbl_coil_lineage, ex_head_pos, hd['hd2'], ex_tail_pos, hd['hd2']+rl['rl2'], metal_code, mc, machine_center_desc, [x[0] for x in df_with_discards_updated.select(machine_center_desc).collect()][2]))

        # update sync_length column in the machine center data.
        df_updated = spark.sql(" SELECT * FROM {}.{} WHERE {}={} AND {} NOT LIKE '0.0' ".format(DB, tbl_coil_lineage, metal_code, mc, table_name))

        for row in range(rows):        
          tbl_process = df_updated.collect()[row][8]
          #start_pos = spark.sql("SELECT MIN{} FROM {} WHERE {}={}".format(eg_LengthIndex, tbl_process, metal_code, mc)).collect()[0][0]
          if df_updated.collect()[row][22]=='LengthIndex':
            end_pos = spark.sql("SELECT MAX({}) FROM {} WHERE {}={}".format(eg_LengthIndex, tbl_process, metal_code, mc)).collect()[0][0]
            print(mc)
            spark.sql("UPDATE {} SET {}=round(({} * eg_LengthIndex + {})*4, 0)/4 WHERE {}={}".format(tbl_process, sync_length, (df_updated.collect()[row][21]-df_updated.collect()[row][20])/(end_pos-0.0), df_updated.collect()[row][20] ,metal_code, mc))
          else:
            end_pos = spark.sql("SELECT MAX({}) FROM {} WHERE {}={}".format(eg_ReverseLengthIndex, tbl_process, metal_code, mc)).collect()[0][0]
            spark.sql("UPDATE {} SET {}=round(({} * eg_ReverseLengthIndex + {})*4, 0)/4 WHERE {}={}".format(tbl_process, sync_length, (df_updated.collect()[row][21]-df_updated.collect()[row][20])/(end_pos-0.0), df_updated.collect()[row][20], metal_code, mc))