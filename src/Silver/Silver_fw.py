# Databricks notebook source
# MAGIC %run "../no sdp have auto load/framework"

# COMMAND ----------


pipeline_name = dbutils.widgets.get("pipeline_name")
file_path = dbutils.widgets.get("file_path")
header = dbutils.widgets.get("header")
delimiter = dbutils.widgets.get("delimiter")
table_name = dbutils.widgets.get("table_name")
schema_detail = json.loads(dbutils.widgets.get("schema_detail"))
keys = json.loads(dbutils.widgets.get("keys"))
write_mode = dbutils.widgets.get("write_mode")

# COMMAND ----------

Silver = SilverLayer(
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



# COMMAND ----------

s_df = Silver.add_sk()

invalid_df = Silver.invalid_check(
    s_df
    )

k_df = Silver.key_null_check(
    s_df
    )

dupe_df = Silver.key_dupe_check(
    s_df,
    k_df
    )

bad_df = Silver.all_bad(
    invalid_df,
    k_df,dupe_df
    )

all_goodsss = Silver.all_good(
    s_df,
    k_df,bad_df
    )

fft_df = Silver.writeas(
    all_goodsss,
    bad_df
    )
