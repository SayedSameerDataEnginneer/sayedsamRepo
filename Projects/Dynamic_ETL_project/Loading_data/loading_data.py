# Databricks notebook source
class Load:
    def __init__(self, df, mode, file_type, path, partitionby=0):
        self.df = df
        self.mode = mode
        self.path = path
        self.partitionby = partitionby
        self.file_type = file_type

    def write_data(self):
        if self.partitionby==0:
            self.df.write.format(self.file_type).mode(self.mode).save(self.path + '/Target/CSV2')
        else:
            self.df.write.partitionBy(self.partitionby).format(self.file_type).mode(self.mode).save(self.path)

def w_data(df, mode, file_type, path, partitionby):
    loader = Load(df, mode, file_type, path, partitionby)
    return loader

