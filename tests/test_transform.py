try:
    from unified_transform_logic.framework_test import BronzeLayer,SilverLayer
except ImportError:
    import os
    import sys
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from logic_packages.src.unified_transform_logic.framework_test import BronzeLayer,SilverLayer

import unittest
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType, BooleanType
from pyspark.sql.functions import col, xxhash64
from datetime import date
from pyspark.sql import SparkSession
import uuid


class TestTierDiscount(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        import sys
        from delta import configure_spark_with_delta_pip
        builder = SparkSession.builder \
            .appName("unit-testing-unittest") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Inject spark into the framework module's global namespace
        # (framework_test.py uses bare `spark` like Databricks notebooks do)
        for mod_name, mod in sys.modules.items():
            if mod_name.endswith("framework_test") and hasattr(mod, "SilverLayer"):
                mod.spark = cls.spark
                break

#   @classmethod
#    def tearDownClass(cls):
 #       cls.spark.stop()

    def test_invalid_transaction_id(self):
        layer = SilverLayer(
            table_name="fact_sales_df",
            pipeline_name="fact_sales_df",
            file_path="asdsad.csv",
            header=True,
            delimiter=",",
            schema_detail={
                "transaction_id": "int",
                "shop_id": "int",
                "sales_qty": "int",
                "sales_amt": "string",
                "sales_date": "date",
                "_sk": "int"
            },
            keys=["shop_id"],
        write_mode="overwrite"
        )

        source_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("shop_id", IntegerType(), True),
            StructField("sales_qty", IntegerType(), True),
            StructField("sales_amt", FloatType(), True),
            StructField("sales_date", StringType(), True),
            StructField("_sk", IntegerType(), True)
        ])

        source = self.spark.createDataFrame(
            [("A", 1, 1, 2.0, "2025-01-01", 1)],
            source_schema
        )

        result = layer.invalid_check(source)

        assert result.select("reason").first()["reason"] == ["_transaction_id_is_invalid"]

    def test_add_sk(self):

        source_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("shop_id", IntegerType(), True),
        StructField("sales_qty", IntegerType(), True),
        StructField("sales_amt", FloatType(), True),
        StructField("sales_date", StringType(), True),
        ])

        source = self.spark.createDataFrame(
        [
            (1, 1, 1, 2.0, "2025-01-01")
        ],
        source_schema
        )

        source.createOrReplaceTempView("fact_sales_bronze")

        layer = SilverLayer(
        table_name="fact_sales",
        pipeline_name="fact_sales",
        file_path="asdsad.csv",
        header=True,
        delimiter=",",
        schema_detail={
            "transaction_id": "int",
            "shop_id": "int",
            "sales_qty": "int",
            "sales_amt": "float",
            "sales_date": "date",
        },
        keys=["shop_id"],
        write_mode="overwrite"
        )

        result = layer.add_sk()
        assert result.filter(col("_sk").isNull()).count() == 0

    def test_key_null_check(self):

        source_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("shop_id", IntegerType(), True),
        StructField("sales_qty", IntegerType(), True),
        StructField("sales_amt", FloatType(), True),
        StructField("sales_date", StringType(), True),
        ])

        source = self.spark.createDataFrame(
        [
            (1, None , 1, 2.0, "2025-01-01")
        ],
        source_schema
        )

        layer = SilverLayer(
        table_name="fact_sales",
        pipeline_name="fact_sales",
        file_path="asdsad.csv",
        header=True,
        delimiter=",",
        schema_detail={
            "transaction_id": "int",
            "shop_id": "int",
            "sales_qty": "int",
            "sales_amt": "float",
            "sales_date": "date",
        },
        keys=["shop_id"],
        write_mode="overwrite"
        )

        result = layer.key_null_check(source)
        assert result.select("reason").first()["reason"] == ["_shop_id_is_null"]

    def test_key_dupe_check(self):

        source_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("shop_id", IntegerType(), True),
        StructField("sales_qty", IntegerType(), True),
        StructField("sales_amt", FloatType(), True),
        StructField("sales_date", StringType(), True),
        StructField("_sk", IntegerType(), True)
        ])

        source = self.spark.createDataFrame(
        [
            (1, 3 , 5, 2.0, "2025-01-01",1),
            (2, 3 , 6, 2.0, "2025-01-01",2),
            (3, 4 , 1, 2.0, "2025-01-01",3),
            (4, 4 , 1, 2.0, "2025-01-01",4)
        ],
        source_schema
        )

        layer = SilverLayer(
        table_name="fact_sales",
        pipeline_name="fact_sales",
        file_path="asdsad.csv",
        header=True,
        delimiter=",",
        schema_detail={
            "transaction_id": "int",
            "shop_id": "int",
            "sales_qty": "int",
            "sales_amt": "float",
            "sales_date": "date",
            "_sk" : "int"
        },
        keys=["shop_id"],
        write_mode="overwrite"
        )

        null_df = layer.key_null_check(source)
        assert null_df.count() == 0

        key__row_dupe = layer.key_dupe_check(source, null_df)
        assert key__row_dupe.select("reason").first()["reason"] == ["row_dupe"] or ["key_dupe"] or ["row_dupe","key_dupe"]
         
    def test_scd2(self):
        schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("shop_id", IntegerType(), True),
            StructField("sales_qty", IntegerType(), True),
            StructField("sales_amt", FloatType(), True),
            StructField("current_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("is_current", BooleanType(), True),
            ])

        table_name = "workspace.default.emp_scd2"        
        silver_table = f"{table_name}_silver"

        target = self.spark.createDataFrame(
            [(1, 1, 1, 2.0, "2025-01-01", "9999-12-31", True)],
            schema,
            )
        
        target = target.withColumns({
            "start_date": col("current_date"),
            "hash_key": xxhash64(col("employee_id")),
            "hash_value": xxhash64(
                col("shop_id"), col("sales_qty"), col("sales_amt"),
                col("current_date"), col("end_date"), col("is_current"))})

        (target.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table))                 

        source = self.spark.createDataFrame([(1, 1, 3, 2.0, "2025-01-01", "9999-12-31", True)],schema,)
        source.createOrReplaceTempView("employee_source_silver")

        employee = SilverLayer(                          
            table_name=table_name,
            pipeline_name="employee",
            file_path="emp_scd2.csv",
            header=True,
            delimiter=",",
            schema_detail={
            "employee_id": "int",
            "shop_id": "int",
            "sales_qty": "int",
            "sales_amt": "float",
            "current_date": "date",
            "end_date": "date",
            "is_current": "boolean",
            },
            keys=["employee_id"],
            write_mode="overwrite",
            scd2_enabled=True,
            source_name="employee",
            )

        hash_df = employee.detect_scd2()

        employee.apply_scd2(hash_df)

        result_df = self.spark.table(employee.silver_table)

        assert result_df.filter(col("employee_id") == 1).count() == 2
        
    def test_cross_check(self):

        source_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("shop_id", IntegerType(), True),
        StructField("sales_qty", IntegerType(), True),
        StructField("sales_amt", FloatType(), True),
        StructField("sales_date", StringType(), True),
        ])

        source = self.spark.createDataFrame([(1, None , 1, 2.0, "2025-01-01")],source_schema)
        target = self.spark.createDataFrame([(1, None , 1, 2.0, "2025-01-01")],source_schema)

        source.createOrReplaceTempView("fact_sales_silver")
        target.createOrReplaceTempView("fact_sales_parent")

        layer = SilverLayer(
            table_name="fact_sales",
            pipeline_name="fact_sales",
            file_path="asdsad.csv",
            header=True,
            delimiter=",",
            schema_detail={
                "transaction_id": "int",
                "shop_id": "int",
                "sales_qty": "int",
                "sales_amt": "float",
                "sales_date": "date",
            },
            keys=["shop_id"],
            write_mode="overwrite"
            )

        result = layer.cross_check("fact_sales_parent","shop_id")

        assert result is None

    def test_writes_check(self):

        source_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("shop_id", IntegerType(), True),
        StructField("sales_qty", IntegerType(), True),
        StructField("sales_amt", FloatType(), True),
        StructField("sales_date", StringType(), True),
        ])

        source = self.spark.createDataFrame([(1, None , 1, 2.0, "2025-01-01")],source_schema)
        target = self.spark.createDataFrame([(1, None , 1, 2.0, "2025-01-01")],source_schema)

        layer = SilverLayer(
            table_name="fact_sales",
            pipeline_name="fact_sales",
            file_path="asdsad.csv",
            header=True,
            delimiter=",",
            schema_detail={
                "transaction_id": "int",
                "shop_id": "int",
                "sales_qty": "int",
                "sales_amt": "float",
                "sales_date": "date",
            },
            keys=["shop_id"],
            write_mode="overwrite"
            )

        result = layer.writeas(source,target)
        
        assert result == print(f"{layer.silver_table},{layer.bad_table} write success")
        assert layer.silver_table == "fact_sales_silver"
        assert layer.bad_table == "fact_sales_bad"

if __name__ == "__main__":
    unittest.main()