# Databricks notebook source
# MAGIC %run config/secret

# COMMAND ----------

adl = connection('daa')

# COMMAND ----------

# OUT_FILE = '/Oswego/Inventory/alloy_5182.csv'
# ALLOY = '5182'

# cm_list = []
# cm_file_list = adl.walk('Oswego/CM/CM88/Raw')
# for f in cm_file_list:
#   if ALLOY in f:
#     cm_list.append(f)
    
# hm_file_list = adl.walk('Oswego/HM/HFM/Raw')
# hm_list = []
# for f in hm_file_list:
#   if ALLOY in f:
#     hm_list.append(f)


# COMMAND ----------

OUT_FILE = '/Oswego/Inventory/alloy_5182.csv'
ALLOY = '5182'

cm_list = []
cm_file_list = adl.walk('Oswego/CM/CM88/Raw')
for f in cm_file_list:
  if ALLOY in f:
    cm_list.append(f)
    
hm_file_list = adl.walk('Oswego/HM/HFM/Raw')
hm_list = []
for f in hm_file_list:
  if ALLOY in f:
    hm_list.append(f)
    


# COMMAND ----------

cm_str = "Full path of all 5182 coils in CM " + "\n" + "\n".join(cm_list)
hm_str = "Full path of all 5182 coils in HM " + "\n" + "\n".join(hm_list)
final_str = cm_str + "\n" + "\n" + "\n" + "\n" + hm_str
with adl.open(OUT_FILE, 'wb') as f:
    f.write(str.encode(str(final_str)))
    f.close()

# COMMAND ----------

