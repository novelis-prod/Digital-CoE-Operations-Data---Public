# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of this notebook
# MAGIC This notebook is to read data from mes table and create coil lineage table in enterprise zone.
# MAGIC 
# MAGIC • Read data from mes  data where metal code values are greater than maximum metal code value in coil lineage delta table
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

# DBTITLE 1,Connect adls to data bricks
# MAGIC %run /config/secret_gen2

# COMMAND ----------

# MAGIC %md Define parameters

# COMMAND ----------

#import libraries to perform funcrional operations
from pyspark.sql.types import IntegerType, StringType, BooleanType
from pyspark.sql.functions import *
from datetime import datetime,timedelta
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from delta.tables import *
from multiprocessing.pool import ThreadPool


#details of folder locations
store_name = "novelisadlsg2"
container = "plantdata"

#details of coil lineage database
ent_db='opsentprodg2'
spark.sql('create database if not exists '+ent_db)
cli_tbl ='coil_lineage'

#details of mes database
trn_db='opstrnprodg2'
mes_tbl ='mes'
file_log_tbl = 'file_log'

#define base path of adls gen2
base_path = "abfss://" + container + "@" + store_name + ".dfs.core.windows.net/"

#destination path to store coil lineage table
destination_path = base_path+'/enterprise/prod/coil_lineage'

#input path for machine index data
machine_index_input_path = base_path+'transform/master_tables/machine_center_index.csv'

#define metal code default value
metal_code_default = 1000000000000000

#define total time for HRM process
eRg_TotalTime = 300

#columns to add
columns_to_add =["table_name",
                 "eRg_EntryThickness",
                 "eRg_ExitThickness",
                 "eRg_EntryHeadDiscard",
                 "eRg_EntryTailDiscard",
                 "eRg_ExitHeadDiscard",
                 "eRg_ExitTailDiscard", 
                 "eRg_EntryRolledLength",
                 "eRg_ExitRolledLength",
                 "eRg_Ratio",
                 "eRg_FlipPriorLength",
                 "pg_Gen_IsDSLeftSide", 
                 "ex_head_pos",
                 "ex_tail_pos", 
                 "use_index", 
                 "last_process" ,
                 "flip_length",
                 "data_updated" ]

# COMMAND ----------

# MAGIC %md Read data into dataframe

# COMMAND ----------

#query to get idetified columns from mes whith metal code greater than default value
query = 'select metal_code,plant_code,parent_coil_id,coil_id,operation_start_date,operation_end_date,machine_center_code from ' + trn_db +'.' + mes_tbl +' where metal_code>'+str(metal_code_default)
#mes df with metal code values greater than default value
mes_df = spark.sql(query)

if mes_df.count()==0:
  dbutils.notebook.exit('No new records')

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

#add a row with machine center desc as HRM for every HFM in machine center desc for plant pinda(663)
hfm_df = mes_new_df.filter((col('machine_center_desc')=='HFM') & (col('plant_code') == 663))
hrm_df = hfm_df.withColumn('machine_center_desc', lit('HRM'))\
               .withColumn('ex_date',expr('en_date'))\
               .withColumn('en_date',from_unixtime(unix_timestamp('ex_date') -eRg_TotalTime).cast('timestamp'))
coil_lineage_df= mes_new_df.union(hrm_df)

# COMMAND ----------

#create a column ops_seq to assign incremental values based on process flow
coil_lineage_df=coil_lineage_df.select('*',row_number().over(Window.partitionBy("metal_code","plant_code").orderBy(asc("ex_date"))).alias('ops_seq'))
coil_lineage_df=coil_lineage_df.select('*',row_number().over(Window.partitionBy("metal_code","plant_code","machine_center_desc").orderBy(asc("ops_seq"))).alias('machine_center_seq'))

# COMMAND ----------

#add additional columns to data frame to store process data
for i in columns_to_add:
  coil_lineage_df = coil_lineage_df.withColumn(i,lit(0.0).cast('Double'))

#change columns data type to maintain compatability  
coil_lineage_df = coil_lineage_df.withColumn("table_name", coil_lineage_df["table_name"].cast(StringType()))\
                                 .withColumn("eRg_FlipPriorLength", coil_lineage_df["eRg_FlipPriorLength"].cast(BooleanType()))\
                                 .withColumn("last_process", coil_lineage_df["last_process"].cast(IntegerType()))\
                                 .withColumn("use_index", coil_lineage_df["use_index"].cast(StringType()))\
                                 .withColumn("flip_length",coil_lineage_df["flip_length"].cast(IntegerType()))\
                                 .withColumn("data_updated", coil_lineage_df["data_updated"].cast(IntegerType()))\
                                 .withColumn("files_mismatch", lit(1))

#drop duplicate records
coil_lineage_df = coil_lineage_df.drop_duplicates(['metal_code','plant_code','en_coil_id','ex_coil_id','machine_center_desc','en_date','ex_date'])

# COMMAND ----------

#calculate the max machine center seq for each coil id and machine center 
df = coil_lineage_df.groupBy('en_coil_id','machine_center_desc').max('machine_center_seq')

#add max machine center seq column to coil lineage df
coil_lineage_df1 = coil_lineage_df.join(df,(df.en_coil_id==coil_lineage_df.en_coil_id)&(df.machine_center_desc==coil_lineage_df.machine_center_desc),how='left').select(coil_lineage_df['*'],df['max(machine_center_seq)'].alias('max_machine_center_seq'))

# COMMAND ----------

# MAGIC %md Write data into delta table

# COMMAND ----------

#insert the new records into coil lineage delta table
try:
  #coil lineage delta table 
  coil_lineage_tbl = DeltaTable.forPath(spark, destination_path)
  coil_lineage_tbl.alias("cli").merge( coil_lineage_df1.alias("cli_df"),
                                     "cli.metal_code = cli_df.metal_code and cli.plant_code = cli_df.plant_code and cli.ex_coil_id = cli_df.ex_coil_id and cli.machine_center_desc = cli_df.machine_center_desc and cli.en_date  = cli_df.en_date and cli.ex_date = cli_df.ex_date ")\
                             .whenMatchedUpdate(set = {'cli.ops_seq':'cli_df.ops_seq'})\
                             .whenNotMatchedInsertAll()\
                             .execute()
except:
  print('coil_lineage table not found')
  #store coil lineage data in enterprise folder as a delta table
  coil_lineage_df1.repartition(1).write.format('delta').mode('overwrite').option('path',destination_path).saveAsTable(ent_db+'.'+cli_tbl)

# COMMAND ----------

#verify the number of files for a particular machine center of a coil id  in file log table is equal with the number of records that was recorded in the coil lineage data table for the same coil id and update files mismatch values
#define coil lineage delta table 
coil_lineage_tbl = DeltaTable.forPath(spark, destination_path)

#define file log df with coil id, machine center and max machine center seq values
df_file_log = spark.sql('select coil_id, UPPER(machine_center) as machine_center,max(machine_center_seq) as max_machine_center_seq from opstrnprodg2.file_log  group by coil_id,UPPER(machine_center)')

#update file mismatch column in coil lineage  
coil_lineage_tbl.alias("cli").merge(df_file_log.alias("file_log"),"cli.en_coil_id = file_log.coil_id and cli.max_machine_center_seq = file_log.max_machine_center_seq and cli.machine_center_desc = file_log.machine_center and cli.files_mismatch = 1 and cli.machine_center_desc in ('HFM','HRM','CM1','CM2','CM3')")\
  .whenMatchedUpdate(set = { "cli.files_mismatch" :lit(0) })\
  .execute()