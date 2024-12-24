# Databricks notebook source
from pyspark.sql import SparkSession,functions as f
from pyspark.sql import *
from pyspark.sql.functions import lit

#hosa providers dataframe
hosa_df=spark.read.format("parquet").load("/mnt/bronze/hosa/providers.parquet")
hosa_df=hosa_df.withColumn("datasource",lit("hosa"))
#hosb providers dataframe

hosb_df=spark.read.format("parquet").load("/mnt/bronze/hosb/providers.parquet")
hosb_df=hosb_df.withColumn("datasource",lit("hosb"))


#unionBYName both the dataframes

df_merged=hosa_df.unionByName(hosb_df)

df_merged.createOrReplaceTempView("providers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.providers (
# MAGIC ProviderID string,
# MAGIC FirstName string,
# MAGIC LastName string,
# MAGIC Specialization string,
# MAGIC DeptID string,
# MAGIC NPI long,
# MAGIC datasource string,
# MAGIC is_quarantined boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into silver.providers
# MAGIC select 
# MAGIC distinct
# MAGIC ProviderID,
# MAGIC FirstName,
# MAGIC LastName,
# MAGIC Specialization,
# MAGIC DeptID,
# MAGIC cast(NPI as INT) NPI,
# MAGIC datasource,
# MAGIC     CASE 
# MAGIC         WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC from providers
