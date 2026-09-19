# Databricks notebook source
# MAGIC %run "../no sdp have auto load/framework"

# COMMAND ----------

# MAGIC %skip  # IF USE MAKE IT ONE TIME ANOTHER USE JUST PUT SOURE_NAME IN CONFIGTABLE
# MAGIC
# MAGIC active_df = spark.table('session_life_hamham.session_life.employee_scd2_silver').filter("is_current = True").drop("_sk")
# MAGIC source_df = spark.table('session_life_hamham.session_life.employee_source_silver').drop("_sk")
# MAGIC
# MAGIC source_df = (
# MAGIC     source_df
# MAGIC     .withColumn("hash_key",xxhash64("employee_id"))
# MAGIC     .withColumn("hash_value",xxhash64("store_id","salary"))
# MAGIC     )
# MAGIC
# MAGIC active_df = (
# MAGIC     active_df
# MAGIC     .withColumn("hash_key",xxhash64("employee_id"))
# MAGIC     .withColumn("hash_value",xxhash64("store_id","salary"))
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC scd2_df = (
# MAGIC     source_df.alias("source")
# MAGIC     .join(active_df.alias("target"), [col("source.hash_key") == col("target.hash_key")], "left")
# MAGIC     .withColumn("record_status",
# MAGIC                 when(col("target.hash_key").isNull(), lit("insert"))
# MAGIC                 .when(col("target.hash_value") == col("source.hash_value"), lit("no_change"))
# MAGIC                 .otherwise(lit("change")))
# MAGIC     .select("source.*", "record_status")
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
# MAGIC (<target>.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table))

# COMMAND ----------

scd2_enabled = dbutils.widgets.get("scd2_enabled")
conf = spark.table("session_life_hamham.session_life.config_table").filter(col("scd2_enabled") == scd2_enabled).first()

# COMMAND ----------

employee = SilverLayer(
    pipeline_name= conf.pipeline_name,
    file_path= conf.file_path,
    header= conf.header,
    delimiter=conf.delimiter,
    table_name=    conf.table_name,
    source_name  = conf.source_name,
    schema_detail= conf.schema_detail,
    keys= conf.keys,
    write_mode= conf.write_mode,
    scd2_enabled= conf.scd2_enabled
)

# COMMAND ----------

hash_df = employee.detect_scd2()

employee.apply_scd2(hash_df)

result_df = spark.table(employee.silver_table)

result_df.display()