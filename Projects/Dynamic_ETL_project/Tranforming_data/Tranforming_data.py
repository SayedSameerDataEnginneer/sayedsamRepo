# Databricks notebook source
class Transform:
    def __init__(self, dic):
        self.dfs_dic = dic

    def transformation1(self):
        trans_df = self.dfs_dic.get("transaction_df")
        custo_df = self.dfs_dic.get("customer_df")

        # Find the total transaction amount for each customer.
        grouped_df = trans_df.groupBy(col("CustomerID")).agg(sum(col("TransactionAmount")).alias("Total_transaction"))
        order_df = grouped_df.orderBy(col("CustomerID"))

        # Identify customers who have made transactions above 500.00
        filtered_df = order_df.filter(col("Total_transaction") > 500)

        return filtered_df
    
    def transformation2(self):
        
        trans_df = self.dfs_dic.get("transaction_df")
        custo_df = self.dfs_dic.get("customer_df")

        # Determine the average transaction amount per customer.
        grouped_df = trans_df.groupBy(col("CustomerID")).agg(avg(col("TransactionAmount")).alias("Average_transaction"))
        order_df = grouped_df.orderBy(col("CustomerID"))

        return order_df


