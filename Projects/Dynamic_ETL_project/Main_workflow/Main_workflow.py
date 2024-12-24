# Databricks notebook source
# MAGIC %run "./1.Extract_data"

# COMMAND ----------

# MAGIC %run "./2.Transform"

# COMMAND ----------

# MAGIC %run "./3.Load"

# COMMAND ----------

class MainWorkflow:
    def __init__(self, mode, file_type, path, dic_path):
        self.mode = mode
        self.file_type = file_type
        self.path = path
        self.dic_path = dic_path

    def runner(self):
        # Read the data and create data frames 
        read_dic = DataFrames(dic=self.dic_path).data_frame_creation()

        # Transform the data
        trans_df = Transform(read_dic).transformation1()

        # Load the data 
        loader = w_data(df=trans_df, mode=self.mode, file_type=self.file_type, path=self.path, partitionby=0).write_data()

# COMMAND ----------

path=[
    {
       "t_path":"dbfs:/FileStore/Project2/Source/CSV/transaction_data.csv",
       "t_f_type":"csv" 
    },
    {
        "c_path":"dbfs:/FileStore/Project2/Source/Parquet/konbert_output_a91e25c7.parquet",
        "t_f_type":"parquet"
    }
]

p_mode="overwrite"
p_path="dbfs:/FileStore/Project2"
p_file_type="csv"

def start(mode,file_type,path,dic):
    return MainWorkflow(mode=mode,file_type=file_type,path=path,dic_path=dic).runner()

start(mode=p_mode,file_type=p_file_type,path=p_path,dic=path)