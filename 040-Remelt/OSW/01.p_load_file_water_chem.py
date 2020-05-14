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
  query = "SELECT file_path from {} where plant_name = '{}' and machine_center = 'RML_water_chem' and is_read = 0 order by month desc, day desc limit 20".format(log_tbl, log_plant)
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
    # other dict
    for k,v in other_dict.items():
      if k in file:
        print(v)
        try:
          load_other(v, base+file)
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

# MAGIC %md ##### loading OTHER files
# MAGIC For Oswego these files are coming from ODS

# COMMAND ----------

def load_other(entity, filepath):
  start = datetime.datetime.now()
  path = filepath.split('/') 
  year = path[-4]
  month = path[-3]
  day = path[-2]

  # get outpath,schema and tablename from config file
  out_path,tablename,schema=path_details(PLANT.lower(),entity.lower())
  # read the files into a df
  df = spark.read.option("badRecordsPath", bad_path).parquet(filepath)
  df = df.withColumn('water_test_id', concat(df['TestDate'], lit('-'), df['TestTime']))
  
  # add path information to df
  path = filepath.split('/') 
  df = df.withColumn('file_name',input_file_name())\
         .withColumn('year',lit(year))\
         .withColumn('month',lit(month))\
         .withColumn('day',lit(day))\
         .withColumn('lastUpdated',lit(datetime.datetime.now()))\
         .withColumn('updatedBy',lit('script'))
    
  cast_list = ['CL_Measurement', 'Cond_Measurement', 'ORP_Measurement', 'PH_Measurement', 'TotalHardness_Measurement', 'TestTime', 'year', 'month', 'day']
  for c in df.columns:
    if c in cast_list:
      df = df.withColumn(c, regexp_replace(df[c],"[^-0-9.]", ""))
      df = df.withColumn(c, when(df[c] != "", df[c]).otherwise('0'))
      df = df.withColumn(c, df[c].cast('double'))
                          
  df.write.format('delta').mode('append').partitionBy('year','month','day').option('mergeSchema','true').option('path',out_path).saveAsTable(schema+'.' + tablename)
  end = datetime.datetime.now()
  print('proc 3 :' + str(end-start))
  
  query = update_is_read(1, filename)
  spark.sql(query)

# COMMAND ----------

def update_caster():
  query = "Select CreatedDate from opsentprodg2.osw_water_chem"
  data = spark.sql(query)
  dates = [x[0] for x in data.select('CreatedDate').collect() if x is not None]
  dates.sort()
  for i in range(len(dates)-1):
    start = dates[i]
    end = dates[i+1]
    query = "update opsentprodg2.osw_caster set water_test_id = (select water_test_id from opsentprodg2.osw_water_chem where CreatedDate = '{}') where timestamp > '{}' and timestamp < '{}'".format(start, start, end)
    spark.sql(query)
    print('start - ' + start)
    print('end - ' + end)

  query = "update opsentprodg2.osw_caster set water_test_id = (select water_test_id from opsentprodg2.osw_water_chem where CreatedDate = '{}') where timestamp > '{}'".format(dates[-1], dates[-1])
  spark.sql(query)
  print('start - ' + start)
  print('end - ' + end)

# COMMAND ----------

if __name__ == "__main__":  
  other_dict = {'plantcoolinglooptesting':'Water_Chem'}
  try:
    read_file_log(pi_dict, other_dict)
    update_caster()
  except:
    spark.sql("UPDATE {} SET is_read = 0 WHERE is_read = 786".format(log_tbl))