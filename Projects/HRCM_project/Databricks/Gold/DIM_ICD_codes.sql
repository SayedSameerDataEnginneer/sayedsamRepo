-- Databricks notebook source
CREATE TABLE IF NOT EXISTS gold.dim_icd (
    icd_code STRING,
    icd_code_type STRING,
    code_description STRING,
    refreshed_at TIMESTAMP
)

-- COMMAND ----------

truncate table gold.dim_icd

-- COMMAND ----------

insert into gold.dim_icd
select distinct
  icd_code,
  icd_code_type,
  code_description,
  current_timestamp() refreshed_at
from
  silver.icd_codes
where
  is_current_flag = true
