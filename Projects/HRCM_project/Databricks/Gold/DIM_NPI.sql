-- Databricks notebook source
CREATE TABLE IF NOT EXISTS gold.dim_npi (
  npi_id STRING,
  first_name STRING,
  last_name STRING,
  position STRING,
  organisation_name STRING,
  last_updated STRING,
  refreshed_at TIMESTAMP)

-- COMMAND ----------

truncate table gold.dim_npi

-- COMMAND ----------

insert into
  gold.dim_npi
select
  npi_id,
  first_name,
  last_name,
  position,
  organisation_name,
  last_updated,
  current_timestamp()
from
  silver.npi_extract
  where is_current_flag = true
