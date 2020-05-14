-- Databricks notebook source
-- MAGIC %run /config/secret_gen2

-- COMMAND ----------

describe opsentprodg2.machine_index

-- COMMAND ----------

select * from opsentprodg2.machine_index where plant = 'PBA'

-- COMMAND ----------

-- machine_id = plant code + machine code + machine series

-- COMMAND ----------

-- Melter_1_General_DesignCapacity_pg	Melter_1_General_FceID_pg	Melter_1_General_Manufacturer_pg	Melter_1_General_MetalVolumeCapacity_pg

-- DC_Caster_2_General_MaxIngotLength_pg	DC_Caster_2_General_PitID_pg
-- 7000	                                    YJDC2


-- COMMAND ----------

insert into opsentprodg2.machine_index VALUES (542501, 'DC_Caster_1', 'Caster', 'YEJ', '0', '', '', '0', 'YJDC1', '7000')

-- COMMAND ----------

insert into opsentprodg2.machine_index VALUES (663504, 'DC_D', 'Caster', 'PBA', '0', '', '', '0', '', '7000')

-- COMMAND ----------

insert into opsentprodg2.machine_index VALUES (663404, 'Holder_D', 'Holder', 'PBA', '0', '', '', '0', '', '')

-- COMMAND ----------

insert into opsentprodg2.machine_index VALUES (663310, 'Melter_D1/Melter_D2', 'Melter', 'PBA', '0', '', '', '0', '', '')

-- COMMAND ----------

select * from opsentprodg2.mm_lineage where plant = 'PBA' and holder_tblname <> '' limit 2

-- COMMAND ----------

