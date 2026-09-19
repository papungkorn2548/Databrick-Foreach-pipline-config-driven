# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

conf = (spark.table("session_life_hamham.session_life.config_table")
        .select(col("pipeline_name")
                ,col("file_path")
                ,col("header")
                ,col("delimiter")
                ,col("table_name")
                ,col("schema_detail")
                ,col("keys")
                ,col("write_mode"))
        .collect())
conf_ = [{"pipeline_name" : i.pipeline_name
        ,"file_path" : i.file_path
        ,"header" : i.header
        ,"delimiter" : i.delimiter
        ,"table_name" : i.table_name
        ,"schema_detail" : i.schema_detail
        ,"keys" : i.keys
        ,"write_mode" : i.write_mode} for i in conf]

# COMMAND ----------

conf_ = [{"pipeline_name" : i.pipeline_name
        ,"file_path" : i.file_path
        ,"header" : i.header
        ,"delimiter" : i.delimiter
        ,"table_name" : i.table_name
        ,"schema_detail" : i.schema_detail
        ,"keys" : i.keys
        ,"write_mode" : i.write_mode} for i in conf]

# COMMAND ----------



# COMMAND ----------

dbutils.jobs.taskValues.set("configtable",conf_)

# COMMAND ----------

spark.table("session_life_hamham.session_life.config_table").display()

# COMMAND ----------


