# Databricks notebook source
# MAGIC %run /config/secret_gen2

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/000-General/00.rml_config

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/040-Remelt/YEJ/00.p_schemata

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/020-Master/03-MM_Lineage/00.p_mm_lineage

# COMMAND ----------

# import  libraries
from pyspark.sql.functions import *
import collections
import datetime
import re

# set the plant code
PLANT ='YEJ'
log_plant = 'yej'
PLANT_CODE = '542'

# COMMAND ----------

def read_file_log(pi_dict):
  start = datetime.datetime.now()
  #Select 5 files from file log table
  query = "SELECT file_path from {} where plant_name = '{}' and machine_center = 'RML' and is_read = 0 limit 5".format(log_tbl, log_plant)
  df = spark.sql(query)
  filelist = [i.file_path for i in df.select('file_path').collect()]
  end = datetime.datetime.now()
  print('proc 1 :' + str(end-start))
  print(filelist)
  # Lock these files
  for file in filelist:
    file_name = file.split('/')[-1]
    try:
      query = update_is_read(786, file_name)
      spark.sql(query)
    except:
#       read_file_log(pi_dict, other_dict)
      read_file_log(pi_dict)
  
  # Now process these files
  for file in filelist:
    file_name = file.split('/')[-1]    
    # pi dict 
    for k,v in pi_dict.items():
      if k in file:
        print(v)
        try:
#           load_pi(v, base+file, file_name)
          load_pi(v, base+file)
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

# DBTITLE 1,Load PI data for Decoter & Side Melter
def load_pi(entity, filepath):
  start = datetime.datetime.now()
  entityx = entity  # Decoter_1/Decoter_2/SM_1/SM_2/SM_3/SM_4
  entity = entity[:-2] #Decoter/SM
  
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
  if entity == 'DC_Caster':
    entity = 'Caster'
    
  # remove EntityX_ and rename EntityX to Entity
  col_1 = [x.replace(entityx + '_','') for x in col]
  col_1a = [x.replace(entityx, entity) for x in col_1]
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

  # add path information to df
  df = df.withColumn('file_name',input_file_name())\
         .withColumn('year',lit(year))\
         .withColumn('month',lit(month))\
         .withColumn('day',lit(day))\
         .withColumn('lastUpdated',lit(datetime.datetime.now()))\
         .withColumn('updatedBy',lit(notebook_name))
  
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
  df.write.format('delta').mode('append').partitionBy('year','month','day').option("mergeSchema", "true").option('path',out_path).saveAsTable(schema + '.' +tablename)
  end = datetime.datetime.now()
  print('proc 2 :' + str(end-start))

  query = update_is_read(1, filename)
  spark.sql(query)

# COMMAND ----------

if __name__ == "__main__":
  pi_dict = {'Decoter_1':'Decoter_1', 'Decoter_2':'Decoter_2', 'SM_1':'SM_1', 'SM_2':'SM_2', 'SM_3':'SM_3', 'SM_4':'SM_4'}
  try:
    read_file_log(pi_dict)
    spark.sql("UPDATE {} SET is_read = 0 WHERE is_read = 786".format(log_tbl))
  except:
    pass