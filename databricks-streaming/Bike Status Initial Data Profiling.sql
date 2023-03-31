-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Initial exploration and profiling of bike status data

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS divvy_exploration;
DROP TABLE IF EXISTS divvy_exploration.bike_status;

-- COMMAND ----------

CREATE TABLE divvy_exploration.bike_status
USING json
OPTIONS (path="/FileStore/Shared/DivvyBikes/api_response/bike_status")

-- COMMAND ----------

select * from divvy_exploration.bike_status

-- COMMAND ----------

SELECT 
EXPLODE(data.stations) AS stations
FROM divvy_exploration.bike_status

-- COMMAND ----------

DROP TABLE IF EXISTS divvy_exploration.bike_status_flattened;

CREATE TABLE divvy_exploration.bike_status_flattened AS
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
  CAST(stations.last_reported AS timestamp) AS last_reported_ts
FROM
(
SELECT 
EXPLODE(data.stations) AS stations
FROM divvy_exploration.bike_status
)

-- COMMAND ----------

select * from divvy_exploration.bike_status_flattened
