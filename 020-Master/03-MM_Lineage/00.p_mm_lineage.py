# Databricks notebook source
# MAGIC %run /config/secret_gen2

# COMMAND ----------

# MAGIC %run /de_digital_coe/prodg2/000-General/00.rml_config

# COMMAND ----------

import datetime

# notebook name
notebook_name = 'lineage'

# COMMAND ----------

def get_smartbatchid(tablename, batchid):
  query = "SELECT DISTINCT smart_batch_id FROM {} WHERE master_batch_id = '{}'".format(tablename, batchid)
  df = spark.sql(query)
  if df.head(1)[0][0] == 0:
    smartbatchid = '-000'
  else:
    smartbatchid = df.select('smart_batch_id').collect()[0][0]
    
  if smartbatchid is None:
    smartbatchid = '-000'
  return smartbatchid
   

# COMMAND ----------

def get_machineid(entity, plant):
  if entity is not None and plant is not None:
    query = "SELECT MACHINE_ID FROM " + index_tbl + " WHERE MACHINE_NAME = '" + entity + "' AND PLANT = '" + plant + "'"
    df = spark.sql(query)
    machineid = df.select('MACHINE_ID').collect()[0][0]
  elif entity is None and plant is not None:
    machineid = '-100'
  elif entity is not None and plant is None:
    machineid = '-200'
  else:
    machineid = '-300'
    
  if machineid is None:
    machineid = '-000'

  return machineid


# COMMAND ----------

def get_time(tablename, y, m, d, batchid):
  times = []
  query = "SELECT MIN(Timestamp) AS STARTTIME FROM {} WHERE year = '{}' AND month = '{}' AND day = '{}' AND master_batch_id = '{}'".format(tablename, y, m, d, batchid)
  df = spark.sql(query)
  starttime = df.select('STARTTIME').collect()[0][0]
  if starttime is None:
    starttime = '-000'
  times.append(starttime)
  
  query = "SELECT MAX(Timestamp) AS ENDTIME FROM {} WHERE year = '{}' AND month = '{}' AND day = '{}' AND master_batch_id = '{}'".format(tablename, y, m, d, batchid)
  df = spark.sql(query)
  endtime = df.select('ENDTIME').collect()[0][0]
  if endtime is None:
    endtime = '-000'
  times.append(endtime)
  
  return times

# COMMAND ----------

def get_query(entity, masterid, smartid, plant, machineid, tablename, starttime, endtime, year, month, day, timestamp, user):
  entity = entity.lower()
  q = {
    "melter" : "INSERT INTO {} VALUES ('{}','{}','{}','','{}','{}','{}','{}','','','','','','','','','','','{}','{}','{}','{}','{}')".format(lineage_tbl,masterid, smartid, plant, machineid, tablename, starttime, endtime, year, month, day, timestamp, user),
    "holder" : "INSERT INTO {} VALUES ('{}','{}','{}','','','','','','{}','{}','{}','{}','','','','','','','{}','{}','{}','{}','{}')".format(lineage_tbl,masterid, smartid, plant, machineid, tablename, starttime, endtime, year, month, day, timestamp, user),
    "caster" : "INSERT INTO {} VALUES ('{}','{}','{}','','','','','','','','','','{}','{}','{}','{}','','','{}','{}','{}','{}','{}')".format(lineage_tbl,masterid, smartid, plant, machineid, tablename, starttime, endtime, year, month, day, timestamp, user),
    "charge" : "INSERT INTO {} VALUES ('{}','','{}','{}','','','','','','','','','','','','','','','{}','{}','{}','{}','{}')".format(lineage_tbl,masterid,plant,tablename,year, month, day, timestamp, user),
    "ingot" : "INSERT INTO {} VALUES ('{}','','{}','','','','','','','','','','','','','','{}','','{}','{}','{}','{}','{}')".format(lineage_tbl,masterid,plant,tablename,year, month, day, timestamp, user),
    "chemistry" : "INSERT INTO {} VALUES ('{}','','{}','','','','','','','','','','','','','','','{}','{}','{}','{}','{}','{}')".format(lineage_tbl,masterid,plant,tablename,year, month, day, timestamp, user)
  }

  query = q[entity]

  return query

# COMMAND ----------

def lineage_pi(entityx, entity, tablename, plant, masterid, year, month, day):
  start = datetime.datetime.now()
  for _id in masterid:
    id = str(_id)
    print(id)
    smartbatchid = get_smartbatchid(tablename, id)
    machineid = get_machineid(entityx, plant)
    times = get_time(tablename, year, month, day, id)
    starttime = times[0]
    endtime = times[1]
    timestamp = str(datetime.datetime.now())
    user = notebook_name

    # column names
    entity_col1 = entity + '_MACHINE_ID'
    entity_col2 = entity + '_TBLNAME'
    entity_col3 = entity + '_STARTTIME'
    entity_col4 = entity + '_ENDTIME'

    # check to see if masterbatchid exists
    query = "SELECT COUNT(*) FROM {} WHERE MASTER_BATCH_ID = '{}'".format(lineage_tbl,id)
    df = spark.sql(query)
    if df.head(1)[0][0] == 0:
      query = get_query(entity, id, smartbatchid, plant, machineid, tablename, starttime, endtime, year, month, day, timestamp, user)
      spark.sql(query)
    else:
      spark.sql(""" UPDATE {} SET {}='{}', {}='{}', {}='{}', {}='{}', year='{}', month='{}', day='{}', lastupdate='{}', updatedby='{}' WHERE plant = '{}' AND master_batch_id = '{}' """.format(lineage_tbl, entity_col1, machineid, entity_col2, tablename, entity_col3, starttime, entity_col4, endtime, year, month, day, timestamp, user, plant, id))
    try:
      spark.sql(""" UPDATE {} SET smart_batch_id = {} WHERE plant = '{}' AND master_batch_id = '{}' AND smart_batch_id = ''""".format(lineage_tbl, smartbatchid, plant, id))
    except:
      pass
  end = datetime.datetime.now()
  print('proc 5 :' + str(end-start))

# COMMAND ----------

def lineage_other(entity, tablename, plant, masterid, year, month, day):
  start = datetime.datetime.now()
  for _id in masterid:
    id = str(_id)
    print(id)
    entity_col = entity + '_TBLNAME'
    timestamp = str(datetime.datetime.now())
    user = notebook_name

    # check to see if batchid exists
    df = spark.sql(""" SELECT COUNT(*) FROM {} where master_batch_id = '{}'""".format(lineage_tbl,id))
    if df.head(1)[0][0] == 0:
      query = get_query(entity, id, '-000', plant, '', tablename, '', '',year, month, day, timestamp, user)
      spark.sql(query)
    else:
      spark.sql(""" UPDATE {} SET {}='{}', year='{}', month='{}', day='{}',lastupdate='{}', updatedby='{}' WHERE plant = '{}' AND master_batch_id = '{}'""".format(lineage_tbl, entity_col, tablename, year, month, day, timestamp, user, plant, id))
  end = datetime.datetime.now()
  print('proc 5 :' + str(end-start))