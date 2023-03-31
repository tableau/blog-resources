# Databricks notebook source
# MAGIC %md
# MAGIC ### Delete the bike status history from cloud storage
# MAGIC - WARNING: This is used to reset the test environment

# COMMAND ----------

# MAGIC %python
# MAGIC import os
# MAGIC 
# MAGIC # Define the API response JSON storage path:
# MAGIC api_resp_path = '/FileStore/Shared/DivvyBikes/api_response/bike_status'

# COMMAND ----------

# Delete the directory:
dbutils.fs.rm(api_resp_path,True)

# COMMAND ----------

# Remake the dir:
dbutils.fs.mkdirs(api_resp_path)
