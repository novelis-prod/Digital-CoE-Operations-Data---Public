# Databricks notebook source
# MAGIC %run /config/secret_gen2

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/000-General/00.rml_config

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/040-Remelt/OSW/00.p_schemata

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/020-Master/03-MM_Lineage/00.p_mm_lineage

# COMMAND ----------

# import  libraries
from pyspark.sql.functions import *
import collections
import datetime
import re

# set the plant code
PLANT ='OSW'
log_plant = 'oswego'

# COMMAND ----------

# MAGIC %md ##### Reading file log to find unprocessed files

# COMMAND ----------

def read_file_log(pi_dict, other_dict):
  start = datetime.datetime.now()
  #Select 5 files from file log table
  query = "SELECT file_path from {} where plant_name = '{}' and machine_center = 'RML' and is_read = 0 order by month desc, day desc limit 20".format(log_tbl, log_plant)
  df = spark.sql(query)
  filelist = [i.file_path for i in df.select('file_path').collect()]
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
          load_pi(v, base+file, file_name)
        except:
          query = update_is_read(-1, file_name)
          spark.sql(query)

    # other dict
    for k,v in other_dict.items():
      if k in file:
        print(v)
        try:
          load_other(v, base+file, file_name)
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

# MAGIC %md ##### loading PI files

# COMMAND ----------

def load_pi(entity, filepath, filename):
  start = datetime.datetime.now()
  # entity = Melter4 suppose, then we need to set entityx as Melter4 and entity as Melter. Therefore,
  entityx = entity  # Melter4
  entity = entity[:-1]   # Melter

  path = filepath.split('/') 
  year = path[-4]
  month = path[-3]
  day = path[-2]

  # get outpath,schema and tablename from config file
  out_path,tablename,schema=path_details(PLANT.lower(), entity.lower())
  
  # read the files into a df
  df = spark.read.option("badRecordsPath", bad_path).parquet(filepath)
  # first drop any columns named exactly the same as the entity, cause sometimes we have columns like Holder4_Holder
  df = df.drop(entityx + '_' + entity)

  # collect the columns of the df into a list
  col = df.columns

  # remove EntityX_ and rename EntityX to Entity
  col_1 = [x.replace(entityx + '_','') for x in col]
  col_1a = [x.replace(entityx, entity) for x in col_1]
  # rename General_PitID
  col_2 = [x.replace('General_PitID_pg', 'PitID_pg') for x in col_1a]

  # rename columns in df before starting to drop columns
  df = df.toDF(*col_2)

  # now let's drop 
  # remove all columns not having '_' and all columns starting with 'General_'
  col_3 = [x for x in col_2 if '_' in x if 'General_' not in x]

  # add Entity and Timestamp columns back to the beginning of the list
  col_4 = [entity, 'Timestamp'] + col_3

  # select our latest list of columns from our latest df
  df = df.select(*col_4)

  # rename the column names of BatchID and smartBatchID
  df = df.withColumnRenamed('System_BatchID_dg','Batch_ID')\
         .withColumnRenamed('System_SmartBatchID','Smart_Batch_ID')

  #add master batch id column to the df 
  try:
    if entity == 'Caster':
      df = df.withColumn('master_batch_id', concat(lit('1250'), expr("substring(Batch_ID, 1, length(Batch_ID)-1)")))
    else:
      df = df.withColumn('master_batch_id', concat(lit("1250"), df["Batch_ID"]))
  except:
    df = df.withColumn('master_batch_id', lit(''))

  # add path information to df
  df = df.withColumn('file_name',input_file_name())\
         .withColumn('year',lit(year))\
         .withColumn('month',lit(month))\
         .withColumn('day',lit(day))\
         .withColumn('lastUpdated',lit(datetime.datetime.now()))\
         .withColumn('updatedBy',lit(notebook_name))
  
  # collect all batchid into unique list of integers > 6 digits
  masterid = df.select('master_batch_id').distinct().collect()
  masterid = [row.master_batch_id for row in masterid if row.master_batch_id is not None if row.master_batch_id.isdigit() if len(row.master_batch_id) > 6]
  
  if len(masterid) > 0: 
    # finally let's fix the data
    cast_list = [x for x in design[entity] if x[1] != 'string' if x[0] in df.columns]
    for c in df.columns:
      for k, v in cast_list:
        if c == k:
          df = df.withColumn(c, regexp_replace(df[c],"[^-0-9.]", ""))
          df = df.withColumn(c, when(df[c] != "", df[c]).otherwise('0'))
          df = df.withColumn(c, df[c].cast(v))

    # write to table
    df.write.format('delta').mode('append').partitionBy('year','month','day','master_batch_id').option("mergeSchema", "true").option('path',out_path).saveAsTable(schema + '.' +tablename)
    end = datetime.datetime.now()
    print('proc 2 :' + str(end-start))
    # Remember entityx = Melter4
    lineage_pi(entityx, entity.lower(), schema+'.'+tablename,PLANT,masterid,year,month,day)
    
    query = update_is_read(1, filename)
    spark.sql(query)
  else:
    query = update_is_read(2, filename)
    spark.sql(query)

# COMMAND ----------

# MAGIC %md ##### loading OTHER files
# MAGIC For Oswego these files are coming from ODS

# COMMAND ----------

def load_other(entity, filepath, filename):
  start = datetime.datetime.now()
  path = filepath.split('/') 
  year = path[-4]
  month = path[-3]
  day = path[-2]

  # get outpath,schema and tablename from config file
  out_path,tablename,schema=path_details(PLANT.lower(),entity.lower())
  # read the files into a df
  df = spark.read.option("badRecordsPath", bad_path).parquet(filepath)
  
  # rename the column names and add metal code column to ingot df  
  if entity.lower() == 'ingot':
    df = df.drop('Batch_ID')
    df =df.withColumnRenamed('MAINBATCH','BATCH_ID')\
          .withColumnRenamed('L2INGOTNAME','INGOT_ID')\
          .withColumn('metal_code',lit(metal_code_default))
  elif entity.lower() == 'chemistry':
    df = df.drop('Batch_ID')
    df = df.withColumnRenamed('MOLTENBATCHID','BATCH_ID')
  elif entity.lower() == 'cycle':
    df  = df.withColumnRenamed('BATCHID','BATCH_ID')\
            .withColumnRenamed('BATCHNAME','INGOT_ID')
  elif entity.lower() == 'phase':
    df = df.withColumnRenamed('BATCHNAME','BATCH_ID')

  # add master batch id column to the df 
  # master batch id = plant code(1250) + prefix(91) + batchid + yyyymmdd
  if entity.lower() == 'phase':
    df = df.withColumn('master_batch_id', concat(lit("1250"), df["Batch_ID"])) 
  elif entity.lower() == 'water_chem':
    df = df.withColumn('master_batch_id', concat(lit("125091"), df["Batch_ID"])) 
  else:
    df = df.withColumn('master_batch_id', concat(lit("125091"), df["Batch_ID"])) 
  # add path information to df
  path = filepath.split('/') 
  df = df.withColumn('file_name',input_file_name())\
         .withColumn('year',lit(year))\
         .withColumn('month',lit(month))\
         .withColumn('day',lit(day))\
         .withColumn('lastUpdated',lit(datetime.datetime.now()))\
         .withColumn('updatedBy',lit('script'))

  # collect all batchid into unique list of integers > 6 digits
  masterid = df.select('master_batch_id').distinct().collect()
  masterid = [row.master_batch_id for row in masterid if row.master_batch_id is not None if row.master_batch_id.isdigit() if len(row.master_batch_id) > 6]

  # finally let's fix the data
  cast_list = [x for x in design[entity] if x[1] != 'string' if x[0] in df.columns]
  for c in df.columns:
    for k, v in cast_list:
      if c == k:
        df = df.withColumn(c, regexp_replace(df[c],"[^-0-9.]", ""))
        df = df.withColumn(c, when(df[c] != "", df[c]).otherwise('0'))
        df = df.withColumn(c, df[c].cast(v))
                          
  df.write.format('delta').mode('append').partitionBy('year','month','day','master_batch_id').option('mergeSchema','true').option('path',out_path).saveAsTable(schema+'.' + tablename)
  end = datetime.datetime.now()
  print('proc 3 :' + str(end-start))
  # add to the lineage!
  if entity.lower() in ['charge','ingot','chemistry']:
    lineage_other(entity, schema+'.'+tablename,PLANT,masterid,year,month,day)
  
  query = update_is_read(1, filename)
  spark.sql(query)

# COMMAND ----------

if __name__ == "__main__":
  pi_dict = {'Melter4':'Melter4', 'Melter5':'Melter5', 'Melter6':'Melter6', 'Holder4':'Holder4', 'Holder5':'Holder5', 'Holder6':'Holder6', 'Caster4':'Caster4', 'Caster5':'Caster5', 'Caster6':'Caster6'}
  
  other_dict = {'remeltbatchmoltenbatchinput':'Charge', 'castingingot':'Ingot', 'remeltsamplebatchsample':'Chemistry', 'remeltprocessphase':'Phase', 'remeltprocesscycle':'Cycle', 'plantcoolinglooptesting':'Water_Chem'}
  
  try:
    read_file_log(pi_dict, other_dict)
  except:
    spark.sql("UPDATE {} SET is_read = 0 WHERE is_read = 786".format(log_tbl))