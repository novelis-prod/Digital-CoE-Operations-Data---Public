# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook will create auto increment table.

# COMMAND ----------

# MAGIC %run /config/secret

# COMMAND ----------

#define variables
env = 'prod'
TABLE_NAME = 'auto_increment'
DB_NAME = 'opstrnprod'
DL = 'daa'
FILE_FORMAT = '.csv'
BASE_PATH = "adl://novelisadls.azuredatalakestore.net/"
SOURCE_PATH = 'adl://novelisadls.azuredatalakestore.net/transform/'+ env + '/csv_for_auto_increment/auto_increment_number.csv'
DESTINATION_PATH = 'adl://novelisadls.azuredatalakestore.net/transform/' + env + '/auto_increment'

# COMMAND ----------

adl = connection(DL)

# COMMAND ----------

rawDataDF = (spark.read 
  .option("inferSchema", "true") 
  .option("header", "true")
  .csv(SOURCE_PATH) 
)

# COMMAND ----------

#creating a database
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(DB_NAME))
spark.sql("USE {}".format(DB_NAME))

# COMMAND ----------

#writing to adl and creating delta table
rawDataDF.repartition(1).write.format('delta').mode('overwrite').option('path',DESTINATION_PATH).saveAsTable(TABLE_NAME)

# COMMAND ----------

# MAGIC %sql select * from opstrnprod.auto_increment limit 5