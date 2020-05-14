# Databricks notebook source
# MAGIC %run /config/secret_gen2

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/000-General/00.rml_config

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/040-Remelt/PBA/00.p_schemata

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/020-Master/03-MM_Lineage/00.p_mm_lineage

# COMMAND ----------

# import  libraries
from pyspark.sql.functions import *
import collections
import datetime
import re

# set the plant code
PLANT ='PBA'
log_plant = 'pba'
PLANT_CODE = '663'

# COMMAND ----------

def read_file_log(pi_dict, other_dict):
  start = datetime.datetime.now()
  #Select 5 files from file log table
  query = "SELECT file_path from {} where plant_name = '{}' and machine_center = 'RML' and is_read = 2 order by month desc, day desc limit 15".format(log_tbl, log_plant)
  df = spark.sql(query)
  filelist = [i.file_path for i in df.select('file_path').collect()]
  print(filelist)
  end = datetime.datetime.now()
  print('proc 1 :' + str(end-start))
  # Lock these files
  for file in filelist:
    file_name = file.split('/')[-1]
    try:
      query = update_is_read(786, file_name)
      spark.sql(query)
    except:
      read_file_log(pi_dict, other_dict)
  
  # Now process these files
  for file in filelist:
    file_name = file.split('/')[-1]
    
    # pi dict 
    for k,v in pi_dict.items():
      if k in file:
        print(v)
        try:
          load_pi(v, base+file,file_name)
        except:
          query = update_is_read(-1, file_name)
          spark.sql(query)

# COMMAND ----------

def update_is_read(status, filename):
  print(status)
  start = datetime.datetime.now()
  query = "UPDATE {} SET is_read = {} WHERE plant_name = '{}' AND machine_center = 'RML' AND file_name = '{}'".format(log_tbl, status, log_plant, filename)
  end = datetime.datetime.now()
  print('proc 4 :' + str(end-start))
  return query

# COMMAND ----------

def melter_lineage(masterid, year, month, day):  
  entity = 'holder'
  out_path,tablename,schema=path_details(PLANT.lower(), entity.lower())
  TBL_HOLDER = schema+'.'+tablename
  entity = 'melter'
  out_path,tablename,schema=path_details(PLANT.lower(), entity.lower())
  TBL_MELTER = schema+'.'+tablename
  
  for i in masterid:
    time= spark.sql('select max(Timestamp) from {} where master_batch_id = {} and Melters_AllTapPlugsClosed_dg = False'.format(TBL_HOLDER,i)).collect()[0][0]
    if time is not None:
      data = spark.sql('select distinct Melter,Batch_ID from {} where Timestamp = {} and System_CurrentMelterPhase_dl like "Transfer%"'.format(TBL_MELTER,'"'+time+'"'))
      batch_list = [x[0] for x in data.select('Batch_ID').collect() if x is not None]
      melters = [x[0] for x in data.select('Melter').collect() if x is not None]
      for j in batch_list:
        spark.sql('UPDATE {} SET {} = {} WHERE {} ={}'.format(TBL_MELTER,'master_batch_id',i,'Batch_ID',j))
      if len(melters)==1:
        entityx = melters[0]
        lineage_pi(entityx,entity.lower(),TBL_MELTER,PLANT,[i],year,month,day)  
      elif len(melters) > 1:
        entityx = melters[0] + '/' + melters[1]
        lineage_pi(entityx,entity.lower(),TBL_MELTER,PLANT,[i],year,month,day)  

# COMMAND ----------

def ingot_lineage(masterid, year, month, day):
  entity = 'caster'
  out_path,tablename,schema=path_details(PLANT.lower(), entity.lower())
  TBL_CASTER = schema+'.'+tablename
  entity = 'ingot'
  out_path,tablename,schema=path_details(PLANT.lower(), entity.lower())
  TBL_INGOT = schema+'.'+tablename
  misc = 'check pba_caster table for process info'
  metal_code = ''
  
  # Our Ingot table will have 10 columns - master_batch_id, batch_id, ingot_id, metal_code, caster, pit_id, year, month, day, misc
  # batchid maybe like 2345/49, 2346/49, 2347/49, 2348/49, 2349/49 --> all belonging to the master_batch_id = 6632349
  for i in masterid:
    master_batch_id = i
    data = spark.sql("SELECT DISTINCT Caster, Batch_ID from {} WHERE year = {} AND month = {} AND day = {} AND master_batch_id = {}".format(TBL_CASTER, year, month, day, i))
    caster = [x[0] for x in data.select('Caster').collect() if x is not None][0]
    batch_list = [x[0] for x in data.select('Batch_ID').collect() if x is not None]
    for b in batch_list:
      batch_id = b
      ingot_id = batch_id.split('/')[0]
      pit_id = '' # for now
 
      spark.sql("INSERT INTO {} VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(TBL_INGOT, master_batch_id, batch_id, ingot_id, metal_code, caster, pit_id, year, month, day, misc ))
  lineage_other(entity, TBL_INGOT, PLANT, masterid, year, month, day)

# COMMAND ----------

def load_pi(entity, filepath,filename):
  start = datetime.datetime.now()
  # entity = DC_A suppose, then we need to set entityx as DC_A and entity as DC. Therefore,
  entityx = entity  # Melter_A1
  if 'Melter' in entity:
    entity = entity[:-3] # Melter
  else:
    entity = entity[:-2]

  path = filepath.split('/') 
  year = path[-4]
  month = path[-3]
  day = path[-2]

  # read the files into a df
  df = spark.read.option("badRecordsPath", bad_path).parquet(filepath)
  # first drop any columns named exactly the same as the entity, cause sometimes we have columns like Holder4_Holder
  df = df.drop(entityx + '_' + entity)

  # collect the columns of the df into a list
  col = df.columns

  # for Caster only, reset entity as Caster
  if entity == 'DC':
    entity = 'Caster'
  # remove EntityX_ and rename EntityX to Entity

  col_1 = [x.replace(entityx + '_','') for x in col]
  col_a = [x.replace(entityx, entity) for x in col_1]
  col_b = [x.replace('General_PitID_pg', 'PitID_pg') for x in col_a]
  col_c = [x.replace('System_BatchID_dg', 'Batch_ID') for x in col_b]
  col_d = [x.replace('System_BatchID_dl', 'Batch_ID') for x in col_c]
  col_2 = [x.replace('System_SmartBatchID', 'Smart_Batch_ID') for x in col_d]

  # rename columns in df before starting to drop columns
  df = df.toDF(*col_2)
  
  # now let's drop 
  # remove all columns not having '_' and all columns starting with 'General_'
  col_3 = [x for x in col_2 if '_' in x if 'General_' not in x]
  # add Entity and Timestamp columns back to the beginning of the list
  col_4 = [entity, 'Timestamp'] + col_3
  # select our latest list of columns from our latest df
  df = df.select(*col_4)
  #if entity == 'Melter':
  df = df.withColumn('master_batch_id', concat(lit(PLANT_CODE), split(df["Batch_ID"],'/')[0]))
  #else:
    #add master batch id column to the df and other columns
    #df = df.withColumn('length', length(split(df["Batch_ID"],'/')[0])-length(split(df["Batch_ID"],'/')[1]) )
    #df = df.withColumn('master_batch_id', concat(lit(PLANT_CODE), split(df["Batch_ID"],'/')[0][0:df.select('length').collect()[0][0]], split(df["Batch_ID"],'/')[1] ) )
    #df = df.withColumn('master_batch_id', regexp_replace(df['master_batch_id'],"[^-0-9]", ""))
    #df = df.drop('length')
    
  # add path information to df
  df = df.withColumn('file_name',input_file_name())\
         .withColumn('year',lit(year))\
         .withColumn('month',lit(month))\
         .withColumn('day',lit(day))\
         .withColumn('lastUpdated',lit(datetime.datetime.now()))\
         .withColumn('updatedBy',lit(notebook_name))
  # collect all batchid into unique list of integers > 6 digits
  #masterid = df.select('master_batch_id').distinct().collect()
  #masterid = [row.master_batch_id for row in masterid if row.master_batch_id is not None if row.master_batch_id.isdigit() if len(row.master_batch_id) > 3]
  #print(masterid)
  
  #if len(masterid) > 0: 
    # finally let's fix the data
  cast_list = [x for x in design[entity] if x in df.columns]
  for c in df.columns:
    if c in cast_list:
      df = df.withColumn(c, regexp_replace(df[c],"[^-0-9.]", ""))
      df = df.withColumn(c, when(df[c] != "", df[c]).otherwise('0'))
      df = df.withColumn(c, df[c].cast('double'))

  # get outpath,schema and tablename from config file
  out_path,tablename,schema=path_details(PLANT.lower(), entity.lower())
  # write to table
  df.write.format('delta').mode('append').partitionBy('year','month','day','master_batch_id').option("mergeSchema", "true").option('path',out_path).saveAsTable(schema + '.' +tablename)
  end = datetime.datetime.now()
  print('proc 2 :' + str(end-start))
    
    # Remember entityx = Melter_A1 or Holder_A or DC_A
    # Melter lineage only gets added when a holder file arrives
    # Ingot table gets populated from Caster file
    #if entity == 'Holder':
      #lineage_pi(entityx, entity.lower(), schema+'.'+tablename,PLANT,masterid,year,month,day)
      #melter_lineage(masterid, year, month, day)
    #elif entity == 'Caster':
      #lineage_pi(entityx, entity.lower(), schema+'.'+tablename,PLANT,masterid,year,month,day)
      #ingot_lineage(masterid, year, month, day)
    #else:
      #pass
    
    # update file log table with 1
  query = update_is_read(1, filename)
  spark.sql(query)
  #else:
    #query = update_is_read(2, filename)
    #spark.sql(query)    

# COMMAND ----------

if __name__ == "__main__":
  # dict_name = {'string in path':'entity_name',...}
  pi_dict = {'Melter_A1':'Melter_A1', 'Melter_A2':'Melter_A2', 'Melter_B1':'Melter_B1', 'Melter_B2':'Melter_B2', 'Melter_C':'Melter_C', 'Melter_D1':'Melter_D1', 'Melter_D2':'Melter_D2','Holder_A':'Holder_A', 'Holder_B':'Holder_B', 'Holder_C':'Holder_C', 'Holder_D':'Holder_D', 'DC_A':'DC_A', 'DC_B':'DC_B', 'DC_C':'DC_C', 'DC_D':'DC_D'}
  other_dict = {'RemeltBatch-MoltenBatchInput':'Charge', 'Casting-Ingot':'Ingot', 'RemeltSample-BatchSample':'Chemistry'} # This is not true for Pinda
  try:
    read_file_log(pi_dict, other_dict)
  except:
    spark.sql("UPDATE {} SET is_read = 0 WHERE is_read = 786".format(log_tbl))