# Databricks notebook source
# MAGIC %md
# MAGIC ###Poll the Divvy Bikes web service to retrieve the latest bike station status and write the json response to cloud storage

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
# MAGIC api_resp_path = '/FileStore/Shared/DivvyBikes/api_response/bike_status'
# MAGIC 
# MAGIC # Define Realtime DivvyBike API URLs:
# MAGIC bike_status_url = "https://gbfs.divvybikes.com/gbfs/en/station_status.json"

# COMMAND ----------

# get json response from api
resp = requests.get(bike_status_url)
resp_json_str = resp.content.decode("utf-8")

# COMMAND ----------

# write response to cloud storage
with open(f"/dbfs/{api_resp_path}/bike_status_{fmt_now}.json","w") as f:
  f.write(resp_json_str)
  
#print("Byte size of JSON Response: ", len(resp_json_str))