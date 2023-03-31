-- Databricks notebook source
-- Create the bronze bike status table containing the raw JSON
CREATE TEMPORARY STREAMING LIVE TABLE bike_status_bronze
COMMENT "The raw bike status data, ingested from /FileStore/DivvyBikes/api_response/station_status."
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * FROM cloud_files("/FileStore/Shared/DivvyBikes/api_response/bike_status", "json", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------


-- Create the silver station status table by exploding on station and picking the desired fields.
CREATE STREAMING LIVE TABLE bike_status_silver (
  CONSTRAINT exclude_dead_stations EXPECT (last_reported_ts > '2023-03-01') ON VIOLATION DROP ROW
  )
COMMENT "The cleaned bike status data with working stations only."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT 
  stations.station_id,
  stations.num_bikes_available,
  stations.num_bikes_disabled,
  stations.num_docks_available,
  stations.num_docks_disabled,
  stations.num_ebikes_available,
  stations.station_status,
  stations.is_renting,
  stations.is_returning,
  last_updated_ts,
  CAST(stations.last_reported AS timestamp) AS last_reported_ts
FROM (
SELECT 
EXPLODE(data.stations) AS stations,
last_updated, 
CAST(last_updated AS timestamp) AS last_updated_ts
  FROM STREAM(LIVE.bike_status_bronze)
  );

-- COMMAND ----------

