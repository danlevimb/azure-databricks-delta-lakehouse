# Databricks notebook source
# #%%

storage_account = "stdanadblh4827"

landing_base_path = "abfss://landing@" + storage_account + ".dfs.core.windows.net"
bronze_base_path = "abfss://bronze@" + storage_account + ".dfs.core.windows.net"
silver_base_path = "abfss://silver@" + storage_account + ".dfs.core.windows.net"
gold_base_path = "abfss://gold@" + storage_account + ".dfs.core.windows.net"
metadata_base_path = "abfss://metadata@" + storage_account + ".dfs.core.windows.net"

print("Environment configuration loaded")
print("Landing: " + landing_base_path)
print("Bronze: " + bronze_base_path)
print("Silver: " + silver_base_path)
print("Gold: " + gold_base_path)
print("Metadata: " + metadata_base_path)

# %%
# COMMAND ----------

display(dbutils.fs.ls(landing_base_path + "/source_data"))