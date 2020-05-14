# Databricks notebook source
# MAGIC %md # Overview of this notebook
# MAGIC 
# MAGIC This notebook is to archive the records from rml_file_log and save into rml_file_log_archive
# MAGIC 
# MAGIC Input to this notebook: None
# MAGIC 
# MAGIC Output of this notebook: Append to the existing delta table(rml_file_log_archive) with new data in enterprise zone and Deletion of the data from rml_file_log

# COMMAND ----------

# MAGIC %run /config/secret_gen2

# COMMAND ----------

#details of folders 
store_name = "novelisadlsg2"
container = "plantdata"
base = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

#details of databases and tables
env = 'prod'
trn_db = 'opstrnprodg2'
ent_db = 'opsentprodg2'

out_path = base + 'enterprise/'+env+'/remelt/master_tables/file_log_archive'
schema = trn_db
arch_table = trn_db + '.' + 'rml_file_log_archive'
log_table = trn_db + '.' + 'rml_file_log'

# COMMAND ----------

print(arch_table,log_table)

# COMMAND ----------

from datetime import  date, datetime, timedelta
today = date.today()
#archive upto two days ago
today = today - timedelta(days=10)
curr_year = today.strftime('%Y')
curr_month = today.strftime('%m')
curr_day = today.strftime('%d')
print(curr_year)
print(curr_month)
print(curr_day)
del_pred = "is_read = 1 and  ( (year < {5} ) or ( year = {0} and month < {1} ) or  ( year = {2} and month = {3} and day < {4} ))".format(curr_year,curr_month,curr_year,curr_month,curr_day,curr_year)
print(del_pred)

# COMMAND ----------

# sel_sql = "SELECT * from {0} where ".format(log_table) + del_pred
# print(sel_sql)

# COMMAND ----------

#Write to archive file 
sel_sql = "SELECT * from {0} where ".format(log_table) + del_pred
print(sel_sql)
df = spark.sql(sel_sql)
df.write.format('delta').mode('append').partitionBy('year','month','day').option("mergeSchema", "true").option('path',out_path).saveAsTable(arch_table)

# COMMAND ----------

print(log_table)

# COMMAND ----------

#delete from rml_file_log
del_sql = "DELETE from {0} where ".format(log_table) + del_pred
print(del_sql)
spark.sql(del_sql)

# COMMAND ----------

#reset table
spark.sql("UPDATE opstrnprodg2.rml_file_log SET is_read = 0 WHERE is_read = 786")

# COMMAND ----------

#reset table
spark.sql("UPDATE opstrnprodg2.rml_file_log SET is_read = 0 WHERE is_read = -1")

# COMMAND ----------

# %sql DELETE FROM opstrnprodg2.rml_file_log where is_read = 1 and day in (1,2,3,4,5,6,7,8,9,10,11,12,13)