# Databricks notebook source
pipeline_name = dbutils.widgets.get("pipeline_name")
file_path = dbutils.widgets.get("file_path")

# COMMAND ----------


print(pipeline_name,file_path)

# COMMAND ----------

print("For_test")