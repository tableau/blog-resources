# Databricks notebook source
# MAGIC %md
# MAGIC ## Build static reference data tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve the latest bike station reference data and write the json response to cloud storage:
# MAGIC - **WARNING: Overwrites contents of station_information directory**

# COMMAND ----------

# MAGIC %python
# MAGIC import os, time
# MAGIC import requests, json
# MAGIC from datetime import datetime
# MAGIC 
# MAGIC # get current timestamp
# MAGIC now = datetime.now()
# MAGIC fmt_now = now.strftime("%Y%m%d_%H-%M-%S")
# MAGIC 
# MAGIC # Define the API response JSON storage path:
# MAGIC api_resp_path = '/FileStore/Shared/DivvyBikes/api_response/station_information'
# MAGIC 
# MAGIC # Define Realtime DivvyBike API URLs:
# MAGIC station_information_url = "https://gbfs.divvybikes.com/gbfs/en/station_information.json"

# COMMAND ----------

# delete output directory if exists
dbutils.fs.rm(api_resp_path,True)

# COMMAND ----------

# create output directory
dbutils.fs.mkdirs(api_resp_path)

# COMMAND ----------

# get json response from api
resp = requests.get(station_information_url)
resp_json_str = resp.content.decode("utf-8")
resp_json_str

# COMMAND ----------

# write response to cloud storage
with open(f"/dbfs/{api_resp_path}/station_info_{fmt_now}.json","w") as f:
  f.write(resp_json_str)
  
print("Byte size of JSON Response: ", len(resp_json_str))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create station reference tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS divvy_exploration;
# MAGIC DROP TABLE IF EXISTS divvy_exploration.station_info;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE divvy_exploration.station_info
# MAGIC USING json
# MAGIC OPTIONS (path="/FileStore/Shared/DivvyBikes/api_response/station_information")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM divvy_exploration.station_info

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS divvy_exploration.station_info_flattened;
# MAGIC 
# MAGIC CREATE TABLE divvy_exploration.station_info_flattened AS
# MAGIC SELECT 
# MAGIC   stations.station_id,
# MAGIC   stations.name,
# MAGIC   stations.station_type,
# MAGIC   stations.has_kiosk,
# MAGIC   stations.capacity,
# MAGIC   stations.lat,
# MAGIC   stations.lon,
# MAGIC   last_updated_ts
# MAGIC FROM
# MAGIC (
# MAGIC SELECT 
# MAGIC EXPLODE(data.stations) AS stations,
# MAGIC CAST(last_updated AS timestamp) AS last_updated_ts
# MAGIC FROM divvy_exploration.station_info
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM divvy_exploration.station_info_flattened
