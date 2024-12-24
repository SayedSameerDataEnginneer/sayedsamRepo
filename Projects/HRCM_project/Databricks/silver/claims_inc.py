# Databricks notebook source
from pyspark.sql import SparkSession,functions as f
from pyspark.sql.types import StructType,StructField,StringType,DateType,DoubleType 

schema = StructType([
    StructField("ClaimID", StringType(), True),
    StructField("SRC_ClaimID", StringType(), True),
    StructField("TransactionID", StringType(), True),
    StructField("PatientID", StringType(), True),
    StructField("EncounterID", StringType(), True),
    StructField("ProviderID", StringType(), True),
    StructField("DeptID", StringType(), True),
    StructField("ServiceDate", DateType(), True),
    StructField("ClaimDate", DateType(), True),
    StructField("PayorID", StringType(), True),
    StructField("ClaimAmount", DoubleType(), True),
    StructField("PaidAmount", DoubleType(), True),
    StructField("ClaimStatus", StringType(), True),
    StructField("PayorType", StringType(), True),
    StructField("Deductible", DoubleType(), True),
    StructField("Coinsurance", DoubleType(), True),
    StructField("Copay", DoubleType(), True),
    StructField("SRC_InsertDate", DateType(), True),
    StructField("SRC_ModifiedDate", DateType(), True)
])



class ClaimsData_landing_to_bronze:
    def __init__(self,src_mnt_point,tgt_mnt_point,schema):
        self.src_path=src_mnt_point
        self.tgt_path=tgt_mnt_point
        self.schema=schema
    def ReadData(self):
        df= spark.read.format("csv").schema(self.schema).load(self.src_path)
        return df
    def transformation1(self,df):
        df = df.withColumn('datasource', 
                        f.when(f.input_file_name().contains('hospital-a'), 'hosa')
                            .when(f.input_file_name().contains('hospital-b'), 'hosab')
                            .otherwise("None"))
        return df
    def ConverToParquet(self,df):
        df..write.mode('overwrite').format("parquet").save(self.tgt_path)
        return True
     

def landing_to_bronze_claims_data(src,tgt,schema):
    df=ClaimsData_landing_to_bronze(src_mnt_point=src,tgt_mnt_point=tgt,schema=schema).ReadData()
    df1=ClaimsData_landing_to_bronze(src_mnt_point=src,tgt_mnt_point=tgt,schema=schema).transformation1(df=df)
    ClaimsData_landing_to_bronze(src_mnt_point=src,tgt_mnt_point=tgt,schema=schema).ConverToParquet(df=df1)
    return True


s='/mnt/landing/claims/*.csv'
t='/mnt/bronze/claims'
df1=landing_to_bronze_claims_data(src=s,tgt=t,schema=schema)


    
