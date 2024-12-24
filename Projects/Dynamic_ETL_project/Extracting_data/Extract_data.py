# Databricks notebook source
from pyspark.sql.functions import *
class DataSource:
    def __init__(self,path,file_type):
        self.path=path
        self.file_type=file_type
    def get_data_frame(self):
        if self.file_type=="csv":
            return (spark.read.format(self.file_type).option("header",True).option("inferSchema",True).load(self.path))
        elif self.file_type=="parquet":
            return (spark.read.format(self.file_type).load(self.path))
        elif self.file_type=="delta":
            return (spark.read.table(self.file_type))
        else:
            return ValueError("File Type Not recognized")
        
def data_frame(f_path,file_type):
    return DataSource(path=f_path,file_type=file_type).get_data_frame()





# COMMAND ----------

class DataFrames:
    def __init__(self,dic):
        self.dic=dic

    def data_frame_creation(self):
        transaction_df=data_frame(f_path=self.dic[0].get("t_path"),file_type=self.dic[0].get("t_f_type"))
        customer_df=data_frame(f_path=self.dic[1].get("c_path"),file_type=self.dic[1].get("t_f_type"))
        
        dfs_dic={
            "transaction_df":transaction_df,
            "customer_df":customer_df
        }
         
        return dfs_dic




