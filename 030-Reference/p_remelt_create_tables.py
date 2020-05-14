# Databricks notebook source
# MAGIC %run config/secret

# COMMAND ----------

# machine_index table
path = 'adl://novelisadls.azuredatalakestore.net/transform/master_tables/machine_index_remelt.csv'
df = spark.read.csv(path, header= True)
display(df)

# COMMAND ----------

# MAGIC %run /config/secret_gen2

# COMMAND ----------

store_name = "novelisadlsg2"
container = "plantdata"
base = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"
out_path = base + 'enterprise/prod/remelt/machine_index'

schema = 'opsentprodg2'
tablename = 'machine_index'
df.write.format('delta').mode('append').option('mergeSchema','true').option('path',out_path).saveAsTable(schema + '.' + tablename)

# COMMAND ----------

# MAGIC %run /config/secret

# COMMAND ----------

# mm_lineage table
path = 'adl://novelisadls.azuredatalakestore.net/transform/master_tables/mm_lineage_schema.csv'
df = spark.read.csv(path, header= True)
display(df)

# COMMAND ----------

# MAGIC %run /config/secret_gen2

# COMMAND ----------

store_name = "novelisadlsg2"
container = "plantdata"
base = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"
out_path = base + 'enterprise/prod/remelt/mm_lineage'

schema = 'opsentprodg2'
tablename = 'mm_lineage'
df.write.format('delta').mode('append').option('mergeSchema','true').option('path',out_path).saveAsTable(schema + '.' + tablename)

# COMMAND ----------

# MAGIC %sql select * from opsentprodg2.mm_lineage

# COMMAND ----------

# MAGIC %sql select * from opsentprodg2.machine_index

# COMMAND ----------

