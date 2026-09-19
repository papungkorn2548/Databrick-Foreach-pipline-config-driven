# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

orders   = spark.table("session_life_hamham.session_life.orders_silver")
payments = spark.table("session_life_hamham.session_life.payments_silver")
stores   = spark.table("session_life_hamham.session_life.stores_silver")

w = Window.partitionBy("customer_id").orderBy("order_date", "order_id")

orders_with_prev = (
    orders
    .withColumn("previous_order_date", F.lag("order_date").over(w))
    .withColumn("days_since_last_order", F.datediff("order_date", "previous_order_date"))
    .withColumn("is_repeat_30d", F.when(F.col("days_since_last_order") <= 30, 1).otherwise(0))
    .join(payments.select("order_id", F.col("amount").alias("payment_amt")), "order_id", "left")
    .join(stores.select("store_id", F.col("city").alias("store_city")), "store_id", "left")
)

gold_store = (
    orders_with_prev.groupBy("store_id", "store_city")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("is_repeat_30d").alias("repeat_orders_30d"),
        F.round(F.sum("is_repeat_30d") / F.count("*"), 3).alias("repeat_rate"),
        F.sum("payment_amt").alias("total_payment"),
        F.round(F.avg("payment_amt"), 2).alias("avg_payment"),
    )
    .withColumn("gold_load_timestamp", F.current_timestamp())
)
gold_store.write.mode("overwrite").saveAsTable("gold_store_repeat_kpi")


oi   = spark.table("session_life_hamham.session_life.order_items_silver")
prod = spark.table("session_life_hamham.session_life.products_silver").select("product_id", "category_id", "supplier_id")
cat  = spark.table("session_life_hamham.session_life.categories_silver")
sup  = spark.table("session_life_hamham.session_life.suppliers_silver").select("supplier_id", F.col("country").alias("supplier_country"))
ret  = spark.table("session_life_hamham.session_life.returns_silver")

ret_agg = ret.groupBy("order_item_id").agg(F.sum("refund").alias("refund_amt"))

items = (
    oi.join(prod, "product_id", "left")
      .join(cat, "category_id", "left")
      .join(sup, "supplier_id", "left")
      .join(ret_agg, "order_item_id", "left")
      .withColumn("gross_amt", F.col("qty") * F.col("price"))
      .withColumn("is_returned", F.when(F.col("refund_amt").isNotNull(), 1).otherwise(0))
      .withColumn("refund_amt", F.coalesce("refund_amt", F.lit(0)))
      .withColumn("refund_over_line", F.when(F.col("refund_amt") > F.col("gross_amt"), 1).otherwise(0))
)

gold_cat = (
    items.groupBy("category_id", "category_name", "supplier_country")
    .agg(
        F.count("*").alias("total_items"),
        F.sum("is_returned").alias("returned_items"),
        F.round(F.sum("is_returned") / F.count("*"), 3).alias("return_rate"),
        F.sum("gross_amt").alias("gross_sales"),
        F.sum("refund_amt").alias("total_refund"),
        F.sum("refund_over_line").alias("refund_over_line_items"),
    )
    .withColumn("gold_load_timestamp", F.current_timestamp())
)

assert items.count() == oi.count()                  
gold_cat.write.mode("overwrite").saveAsTable("gold_category_return_kpi")

# COMMAND ----------

orders_with_prev.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. store ไหนมี repeat rate สูงสุด
# MAGIC SELECT
# MAGIC   store_id,
# MAGIC   store_city,
# MAGIC   repeat_rate
# MAGIC FROM
# MAGIC   gold_store_repeat_kpi
# MAGIC ORDER BY
# MAGIC   repeat_rate DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. เมืองไหนลูกค้ากลับมาซื้อซ้ำมากสุด
# MAGIC SELECT
# MAGIC   store_city,
# MAGIC   ROUND(SUM(repeat_orders_30d) / SUM(total_orders), 3) AS repeat_rate
# MAGIC FROM
# MAGIC   gold_store_repeat_kpi
# MAGIC GROUP BY
# MAGIC   store_city
# MAGIC ORDER BY
# MAGIC   repeat_rate DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. category ไหนถูกคืนมากสุด
# MAGIC SELECT
# MAGIC   category_name,
# MAGIC   SUM(returned_items) AS returned,
# MAGIC   ROUND(SUM(returned_items) / SUM(total_items), 3) AS return_rate
# MAGIC FROM
# MAGIC   gold_category_return_kpi
# MAGIC GROUP BY
# MAGIC   category_name
# MAGIC ORDER BY
# MAGIC   return_rate DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. supplier country ไหนมี refund สูงเมื่อเทียบกับยอดขาย
# MAGIC SELECT
# MAGIC   supplier_country,
# MAGIC   ROUND(SUM(total_refund) / SUM(gross_sales), 3) AS refund_pct
# MAGIC FROM
# MAGIC   gold_category_return_kpi
# MAGIC GROUP BY
# MAGIC   supplier_country
# MAGIC ORDER BY
# MAGIC   refund_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. store ไหนมี repeat rate 
# MAGIC 2. เมืองไหนลูกค้ากลับมาซื้อซ้ำมากสุด
# MAGIC 3. category ไหนถูกคืนมากสุด
# MAGIC 4. supplier country ไหนมี refund สูงเมื่อเทียบกับยอดขาย