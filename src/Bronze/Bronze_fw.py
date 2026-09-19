# Databricks notebook source
# MAGIC %run "../no sdp have auto load/framework"

# COMMAND ----------


pipeline_name = dbutils.widgets.get("pipeline_name")
file_path = dbutils.widgets.get("file_path")
header = dbutils.widgets.get("header")
delimiter = dbutils.widgets.get("delimiter")
table_name = dbutils.widgets.get("table_name")
schema_detail = dbutils.widgets.get("schema_detail")
keys = dbutils.widgets.get("keys")
write_mode = dbutils.widgets.get("write_mode")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

Brozne = BronzeLayer(
    pipeline_name = pipeline_name
    ,file_path = file_path
    ,header = header
    ,delimiter = delimiter
    ,table_name = table_name
    ,schema_detail= schema_detail
    ,keys = keys
    ,write_mode = write_mode
)

# COMMAND ----------

Bronze_read_df = Brozne.read_bronze()

if "status" in schema_detail:
    Bronze_read_df = Bronze_read_df.withColumnRenamed(
        "status",
        "status_"
    )
Bronze_write_dt = Brozne.write_bronze(
    Bronze_read_df
    )

# COMMAND ----------

spark.table("s")

# COMMAND ----------



# COMMAND ----------

