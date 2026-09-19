# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name
# MAGIC FROM session_life_hamham.information_schema.tables WHERE table_name LIKE "%bronze";
# MAGIC

# COMMAND ----------

test_df = spark.table("session_life_hamham.session_life.config_table").collect()

# COMMAND ----------

test_df

# COMMAND ----------



# COMMAND ----------

for i in test_df:
    keys = i["keys"]
    current_table = i["table_name"]
    #print("keys =", keys)
    for key in keys:
        #print("กำลังหา:", key)
        for i in test_df:

            other_table = i["table_name"]
            schema = i["schema_detail"]

            if current_table == other_table:                
                continue

            #print("ได้:", schema)
            if key in schema :
                #print(f"{key} กับ,{schema},{i["table_name"]}")
                print("--------------------------------")
                print(f"KEY == {key} | {current_table.split("_")[-1]} && {(i["table_name"].split("_")[-1])}_silver")

                child_df = spark.table(f"{current_table}_silver")
                parent_df = spark.table(f"{i["table_name"]}_silver")

                child_df.join(parent_df, key, "left_anti").select(col(key)).agg(count("*").alias("missing")).withColumn("status", when(col("missing") > 0 , lit("Fail")).otherwise(lit("PASS"))).show()
