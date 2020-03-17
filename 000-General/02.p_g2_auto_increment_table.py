# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook will create auto increment table.

# COMMAND ----------

# DBTITLE 1,Connect ADLS Gen2 to Databricks
# MAGIC %run /config/secret_gen2

# COMMAND ----------

#define variables
env = 'prod'
table_name = 'auto_increment'
trn_db = 'opstrnprodg2'
file_format = '.csv'

store_name = "novelisadlsg2"
container = "plantdata"

base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

source_path = base_path + 'transform/master_tables/csv_for_auto_increment/auto_increment_number.csv'
destination_path = base_path + 'transform/' + env + '/auto_increment'

# COMMAND ----------

rawDataDF = (spark.read 
  .option("inferSchema", "true") 
  .option("header", "true")
  .csv(source_path) 
)

# COMMAND ----------

#creating a database
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(trn_db))
spark.sql("USE {}".format(trn_db))

# COMMAND ----------

#writing to adl and creating delta table
rawDataDF.repartition(1).write.format('delta').mode('overwrite').option('path',destination_path).saveAsTable(table_name)