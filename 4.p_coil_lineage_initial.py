# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook is to read data from mes table and create coil lineage table in enterprise zone.
# MAGIC 
# MAGIC • Read data from mes  data where metal code values were updated with newly generated values
# MAGIC 
# MAGIC • Create a coil lineage data frame  with identified columns and machine_center_desc column from machine_index table
# MAGIC 
# MAGIC • Add a new row with machine center desc value as HRM for every HFM value in machine center desc
# MAGIC 
# MAGIC • Create a  new column(ops seq) with values to identify the process flow of every metal coil
# MAGIC 
# MAGIC • Add additional columns to data frame to store the data obtained from coil processing
# MAGIC 
# MAGIC • Store coil lineage table in enterprise  folder as a delta table

# COMMAND ----------

# MAGIC %run config/secret

# COMMAND ----------

#import libraries to perform funcrional operations
from pyspark.sql.functions import *
from datetime import datetime,timedelta
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

#details of coil lineage table
env = 'prod'
DB='opsentprod'
spark.sql('create database if not exists '+DB)
tbl_prod_coil_lineage ='coil_lineage'

#details of mes table
DB_MES='opstrnprod'
mes_tbl = 'mes'
metal_code_default = '1000000000000000'

#destination path to store coil lineage table
destination_path_coil_lineage = 'adl://novelisadls.azuredatalakestore.net/enterprise/' + env + '/coil_lineage'

#define total time for HRM process
eRg_TotalTime = 300

#input path for machine index data
machine_index_input_path = 'adl://novelisadls.azuredatalakestore.net/transform/master_tables/machine_center_index.csv'

#query to get idetified columns from mes whith metal code greater than default value
query = 'select metal_code,plant_code,parent_coil_id,coil_id,operation_start_date,operation_end_date,machine_center_code from ' + DB_MES + '.'+ mes_tbl + ' where metal_code> ' + metal_code_default

#columns to add
columns_to_add =["table_name","eRg_EntryThickness","eRg_ExitThickness","eRg_EntryHeadDiscard","eRg_EntryTailDiscard","eRg_ExitHeadDiscard","eRg_ExitTailDiscard", "eRg_EntryRolledLength","eRg_ExitRolledLength","eRg_Ratio","eRg_FlipPriorLength", "pg_Gen_IsDSLeftSide", "ex_head_pos","ex_tail_pos", "use_index", "last_process","flip_length" ]

# COMMAND ----------

#mes df with metal code values greater than default value
mes_df = spark.sql(query)

#change the column names into a compatible fomart for coil lineage table
mes_df= mes_df.withColumnRenamed('parent_coil_id','en_coil_id')\
                                        .withColumnRenamed('coil_id','ex_coil_id')\
                                        .withColumnRenamed('operation_start_date','en_date')\
                                        .withColumnRenamed('operation_end_date','ex_date')  

#read machine index data into a dataframe
machine_index_df = spark.read.format('csv').options(header=True,inferSchema=True).load(machine_index_input_path)

# COMMAND ----------

#get machine center desc from machine index data   
mes_new_df= mes_df.join(machine_index_df, (mes_df["plant_code"]==machine_index_df["PLANT_CODE"]) & (mes_df["machine_center_code"]==machine_index_df["MACHINE_CENTER_CODE"]), "left_outer").select(mes_df['*'],machine_index_df['MACHINE_CENTER_DESC'])

# COMMAND ----------

#drop machine center code from dataframe
mes_new_df=mes_new_df.drop('machine_center_code')
#convert columns from upper case to lower case
mes_new_df=mes_new_df.toDF(*[x.lower() for x in mes_new_df.columns])

# COMMAND ----------

#add a row with machine center desc as HRM for every HFM in machine center desc
hfm_df = mes_new_df.filter(col('machine_center_desc')=='HFM')
hrm_df = hfm_df.withColumn('machine_center_desc', lit('HRM'))\
               .withColumn('ex_date',expr('en_date'))\
               .withColumn('en_date',from_unixtime(unix_timestamp('ex_date') -eRg_TotalTime).cast('timestamp'))
coil_lineage_df= mes_new_df.union(hrm_df)

# COMMAND ----------

#create a column ops_seq to assign a number based on flow of operation sequence
coil_lineage_df=coil_lineage_df.select('*',row_number().over(Window.partitionBy("metal_code","plant_code").orderBy(asc("ex_date"))).alias('ops_seq'))

# COMMAND ----------

#add extra columns to data frame
for i in columns_to_add:
  coil_lineage_df = coil_lineage_df.withColumn(i,lit(0.0).cast('Double'))   
  
#change columns data type 
from pyspark.sql.types import IntegerType, StringType, BooleanType
coil_lineage_df = coil_lineage_df.withColumn("table_name", coil_lineage_df["table_name"].cast(StringType()))
coil_lineage_df = coil_lineage_df.withColumn("eRg_FlipPriorLength", coil_lineage_df["eRg_FlipPriorLength"].cast(BooleanType()))
coil_lineage_df = coil_lineage_df.withColumn("last_process", coil_lineage_df["last_process"].cast(IntegerType()))
coil_lineage_df = coil_lineage_df.withColumn("use_index", coil_lineage_df["use_index"].cast(StringType()))\
                                 .withColumn("flip_length",coil_lineage_df["flip_length"].cast(IntegerType()))

# COMMAND ----------

#store coil lineage data in enterprise folder as delta table
coil_lineage_df.repartition(1).write.format('delta').option('path',destination_path_coil_lineage).saveAsTable(DB+'.'+tbl_prod_coil_lineage)