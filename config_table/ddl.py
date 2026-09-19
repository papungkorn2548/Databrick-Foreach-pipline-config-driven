# Databricks notebook source
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# COMMAND ----------

spark.sql("create catalog if not exists session_life_hamham")
spark.sql("create schema if not exists session_life_hamham.session_life")
spark.sql("create volume if not exists session_life_hamham.session_life.manual_file_folder")

print("schema and volume session_life_hamham.session_life.manual_file_folder is created successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table session_life_hamham.session_life.config_table (
# MAGIC     pipeline_name string
# MAGIC   , file_path string
# MAGIC   , header string
# MAGIC   , delimiter string
# MAGIC   , table_name string
# MAGIC   , schema_detail map<string,string>
# MAGIC   , keys array<string>
# MAGIC   , write_mode string
# MAGIC   , source_name string
# MAGIC   , scd2_enabled boolean
# MAGIC )

# COMMAND ----------

def upsert_into(df:DataFrame,table_name:str,keys:list) -> DataFrame:
    delta_obj = DeltaTable.forName(spark,table_name)
    return (
        delta_obj.alias("t").merge(
            df.alias("s")," AND ".join([f"t.{key} = s.{key}" for key in keys])
            )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
        )

# COMMAND ----------

l = spark.table("session_life_hamham.session_life.employee_scd2_silver")

l.columns

# COMMAND ----------

raw = (
    spark.read.format('csv')
    .option("header",True)
    .option("delimiter",",")
    .load("/Volumes/session_life_hamham/session_life/manual_file_folder/returns.csv")
    )
raw.columns

# COMMAND ----------

data = [{
    "pipeline_name": "order_items",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/order_items.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.order_items",
    "schema_detail": {"order_item_id": "int", "order_id": "string", "qty": "int", "price": "int","product_id" : "int"},
    "keys": ["order_item_id"],
    "write_mode": "overwrite"
},
###############################################################################################################
        {
    "pipeline_name": "employee",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/employee_scd2.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.employee_scd2",
    "schema_detail": {"employee_id": "int", "store_id": "string", "salary": "int"},
    "keys": ["employee_id"],
    "write_mode": "overwrite",
    "source_name": "session_life_hamham.session_life.employee",
    "scd2_enabled" : True
},
###############################################################################################################
        {
    "pipeline_name": "orders",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/orders.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.orders",
    "schema_detail": {"order_id": "int", "customer_id": "int", "store_id": "int", "order_date": "date", "promotion_id": "int"},
    "keys": ["order_id"],
    "write_mode": "overwrite",
},
 ###############################################################################################################
        {
    "pipeline_name": "payments",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/payments.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.payments",
    "schema_detail": {"payment_id": "int", "order_id": "string", "amount": "int"},
    "keys": ["payment_id"],
    "write_mode": "overwrite",
},
###############################################################################################################
        {
    "pipeline_name": "products",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/products.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.products",
    "schema_detail": {"product_id": "int", "category_id": "string", "supplier_id": "int", "price": "int"},
    "keys": ["product_id"],
    "write_mode": "overwrite",
},
###############################################################################################################
        {
    "pipeline_name": "promotions",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/promotions.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.promotions",
    "schema_detail": {"promotion_id": "int", "discount": "int"},
    "keys": ["promotion_id"],
    "write_mode": "overwrite",
},
###############################################################################################################
        {
    "pipeline_name": "returns",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/returns.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.returns",
    "schema_detail": {"return_id": "int", "order_item_id": "int", "refund": "int"},
    "keys": ["return_id"],
    "write_mode": "overwrite",
},
###############################################################################################################
        {
    "pipeline_name": "shipments",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/shipments.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.shipments",
    "schema_detail": {"shipment_id": "int", "order_id": "int", "status": "string"},
    "keys": ["shipment_id"],
    "write_mode": "overwrite",
},
###############################################################################################################
        {
    "pipeline_name": "stores",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/stores.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.stores",
    "schema_detail": {"store_id": "int", "city": "string"},
    "keys": ["store_id"],
    "write_mode": "overwrite",
},
###############################################################################################################
        {
    "pipeline_name": "suppliers",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/suppliers.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.suppliers",
    "schema_detail": {"supplier_id": "int", "country": "string"},
    "keys": ["supplier_id"],
    "write_mode": "overwrite",
},
###############################################################################################################
        {
    "pipeline_name": "categories",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/categories.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.categories",
    "schema_detail": {"category_id": "int", "category_name": "string"},
    "keys": ["category_id"],
    "write_mode": "overwrite",
},
###############################################################################################################
        {
    "pipeline_name": "customers",
    "file_path": "/Volumes/session_life_hamham/session_life/manual_file_folder/customers.csv",
    "header": "true",
    "delimiter": ",",
    "table_name": "session_life_hamham.session_life.customers",
    "schema_detail": {"customer_id": "int", "city": "string", "signup_date": "date"},
    "keys": ["customer_id"],
    "write_mode": "overwrite",
}
###############################################################################################################
        ]

schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("header", StringType(), True),
    StructField("delimiter", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("schema_detail", MapType(StringType(), StringType()), True),
    StructField("keys", ArrayType(StringType()), True),
    StructField("write_mode", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("scd2_enabled", BooleanType(), True)
])

mock_df = spark.createDataFrame(data, schema)
upsert_into(mock_df,"session_life_hamham.session_life.config_table",["pipeline_name"])

# COMMAND ----------

raw = (
    spark.read.format('csv')
    .option("header",True)
    .option("delimiter",",")
    .load("/Volumes/session_life_hamham/session_life/manual_file_folder/customers.csv")
    )
raw.columns

# COMMAND ----------

raw.display()

# COMMAND ----------

mock_df.display()