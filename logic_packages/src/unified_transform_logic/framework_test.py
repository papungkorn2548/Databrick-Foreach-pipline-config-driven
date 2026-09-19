from dataclasses import dataclass
from pyspark.sql.functions import *
from pyspark.sql.window import *
from delta.tables import DeltaTable

@dataclass
class BronzeLayer:
    pipeline_name : str
    file_path : str
    header : str
    delimiter : str
    table_name : str
    schema_detail : dict
    keys : list
    write_mode : str

    def __post_init__(self):
        self.format = self.file_path.split('.')[-1]
        self.save_path = (f"{self.table_name}_bronze")

    def read_bronze(self) -> DataFrame:
        df = (spark.read
            .format(self.format)
            .option("header", self.header)
            .option("delimiter", self.delimiter)
            .load(self.file_path))
        return ( df
            .withColumn("_file_name",col("_metadata.file_name"))
            .withColumn("_file_path",col("_metadata.file_path"))
            .withColumn("_file_size",col("_metadata.file_size"))
            .withColumn("_file_mod",col("_metadata.file_modification_time"))
            )
        
    def write_bronze(self,df : DataFrame) -> None:
        df.write.mode(self.write_mode).option("mergeSchema", "true").saveAsTable(self.save_path)
        print(f"Bronze layer written : {self.save_path}")

def invalid_data(df:DataFrame) -> DataFrame:

    control_ = [i for i in df.columns if i.startswith("_") and i != "_sk"]
    data_col = [i for i in df.columns if i not in control_]
    or_col = " OR ".join( i for i in control_)

    return (df.filter(or_col).melt(
        ids = [*data_col]
        ,values = control_
        ,variableColumnName='reason'
        ,valueColumnName = 'status'
    ).
    filter(col('status')==True).groupBy(*data_col).agg(collect_list(col('reason')).alias('reason')
))
    
@dataclass
class SilverLayer:
    pipeline_name : str
    file_path : str
    header : str
    delimiter : str
    table_name : str
    schema_detail : dict
    keys : list
    write_mode : str
    source_name : str = None
    scd2_enabled: bool = False
    scd2_columns: list = None
    

    def __post_init__(self):
        self.data_detail = [i for i in self.schema_detail.keys()]
        self.silver_table = f"{self.table_name}_silver"
        self.bronze_table = f"{self.table_name}_bronze"
        self.bad_table = f"{self.table_name}_bad"
        self.source_table = f"{self.source_name}_source_silver"
        if self.source_name is not None:
            self.source_table = spark.table(self.source_table)
        self.invalid_rule = {'int' : "^[0-9]+$" , "date" : "^\\d{4}-\\d{2}-\\d{2}$"}
        if self.scd2_enabled is True : self.scd2_columns = [i for i in self.schema_detail.keys() if i not in self.keys]
    
    def add_sk(self) -> DataFrame:
        return spark.table(self.bronze_table).select(monotonically_increasing_id().alias('_sk'),*self.data_detail)
    
    def invalid_check(self,df:DataFrame) -> DataFrame:
        invalid_col = {f"_{k}_is_invalid" : coalesce(~col(k).rlike(self.invalid_rule[v]),lit(False)) for k,v in self.schema_detail.items() if v not in "string"}
        
        return(df.withColumns(invalid_col).transform(invalid_data))
    
    def key_null_check(self,df:DataFrame) -> DataFrame:
        key_null = {f"_{k}_is_null" : col(k).isNull() for k in self.keys}

        return (
            df.withColumns(key_null).transform(invalid_data)
        )
    
    def key_dupe_check(self,df:DataFrame,key_null_check:DataFrame) -> DataFrame:
        partition_row = Window.partitionBy(*self.data_detail).orderBy('_sk')
        partition_key = Window.partitionBy(*self.keys)

        brononull_df = df.join(key_null_check,"_sk","left_anti")
        row_dupe_df = brononull_df.withColumn('rn',row_number().over(partition_row)).filter('rn > 1').drop('rn').withColumn('reason',array(lit('row_dupe')))

        key_dupe = (
        brononull_df.alias('nonull')
        .join(row_dupe_df.alias('rowd'),[col('nonull._sk') == col('rowd._sk')],'leftanti')
        .withColumn('count',count('*').over(partition_key))
        .filter('count > 1')
        .withColumn('reason',array(lit('key_dupe'))).drop(col('count'))
        )

        return (
            row_dupe_df.unionByName(key_dupe)
        )

    def all_bad(self,invalid_df:DataFrame,keynull_df:DataFrame,dupe_df:DataFrame)-> DataFrame:
        return(
        invalid_df.unionByName(keynull_df).unionByName(dupe_df).groupBy('_sk',*self.data_detail).agg(flatten(collect_list('reason')).alias('reason'))
        #invalid_df.unionByName(keynull_df).unionByName(dupe_df).groupBy("_sk",*self.data_detail).agg(flatten(collect_list(col('reason')).alias('reason')))
        ) 

    def all_good(self,good:DataFrame,key_null_check:DataFrame,bad:DataFrame)-> DataFrame:
        cast_col = [col(k).cast(v) for k,v in self.schema_detail.items() ]
        brononull_df = good.join(key_null_check,["_sk"],"left_anti")
        return(
            brononull_df.join(bad,"_sk","left_anti").select(*cast_col)
        )
    def add_scd2_hash(self,df: DataFrame) -> DataFrame:
        if self.scd2_enabled is True and isinstance(self.source_name, str):
            return df.withColumn("hash_key",xxhash64(*[col(column)for column in self.keys])
            ).withColumn("hash_value",xxhash64(*[col(column)for column in self.scd2_columns]))
        else:
            raise Exception("SCD2 is not enabled. or forgot source_name")

        return df
    
    def detect_scd2(self) -> DataFrame:
        if self.scd2_enabled is True and isinstance(self.source_name, str):
            if self.silver_table.startswith("/"):
                target_df = (spark.read.format("delta").load(self.silver_table).filter(col("is_current") == True))
            else:
                target_df = (spark.table(self.silver_table).filter(col("is_current") == True))
            source_df = (self.add_scd2_hash(self.source_table))
            target_df = (self.add_scd2_hash(target_df))
        else:
           raise Exception("SCD2 is not enabled. or forgot source_name")
        return (
            source_df.alias("source")
            .join(target_df.alias("target"),col("source.hash_key")==col("target.hash_key"),"left")
            .withColumn("record_status",when(col("target.hash_key").isNull(),lit("insert"))
                        .when(col("target.hash_value")==col("source.hash_value"),lit("no_change")).otherwise(lit("change")))
            .select("source.*","record_status"))
        
    def build_scd2_staging(self,scd2_df: DataFrame) -> DataFrame:
        if self.scd2_enabled is True and isinstance(self.source_name, str):
            insert_df = (scd2_df.filter(col("record_status")== "insert").withColumn("merge_key",lit(None).cast("long")))
            change_df = (scd2_df.filter(col("record_status") == "change"))
            change_df_for_insert = (change_df.withColumn("merge_key",lit(None).cast("long")))
            change_df_for_update = (change_df.withColumn("merge_key",col("hash_key")))
            change_staging = (change_df_for_insert.unionByName(change_df_for_update))
        else:
            raise Exception("SCD2 is not enabled.or forgot source_name")
        return (
            change_staging
            .unionByName(insert_df)
        )

    def build_insert_values(self) -> dict:
        values = {column:f"source.{column}"for column in self.data_detail}

        values.update({ "start_date":"current_date()"
                       ,"end_date":"cast('9999-12-31' as date)"
                       ,"is_current":"true"
                       , "hash_key":"source.hash_key"
                       , "hash_value":"source.hash_value",
        })
        return values
    #  CAN USE WHEN U MAKE DELTA TABLE ONE TIME AND ANOTHER USE JUST PUT SOURCE NAME IN CONFIGTABLE
    def apply_scd2(self,scd2_df: DataFrame) -> None:
        if self.scd2_enabled == True:
            staging_df = (self.build_scd2_staging(scd2_df))
            if self.silver_table.startswith("/"):
                target = DeltaTable.forPath(spark, self.silver_table)
            else:
                target = DeltaTable.forName(spark, self.silver_table)
            insert_values = (self.build_insert_values())
            scd2_finished = (target.alias("target").merge(staging_df.alias("source"),
                """
                target.hash_key = source.merge_key
                AND target.is_current = true
                """
            ).whenMatchedUpdate(set={"end_date":"current_date()","is_current":"false"})
            .whenNotMatchedInsert(values=insert_values)
            .execute())
            print("SCD2 finished")
        else:
            raise Exception("SCD2 is not enabled. or forgot source_name")
    
    def writeas(self,df:DataFrame,bad_rec_df:DataFrame) -> None:
        if self.scd2_enabled == False or isinstance(self.source_name, str):
            df.write.mode(self.write_mode).option("mergeSchema", "true").saveAsTable(self.silver_table)
            bad_rec_df.write.mode(self.write_mode).option("mergeSchema", "true").saveAsTable(self.bad_table)
            print(f"{self.silver_table},{self.bad_table} write success")
        else:
            raise Exception("SCD2 is not avaiable for this column")
    
    def cross_check(self, parent_table, key):

        child_df = spark.table(self.silver_table)
        parent_df = spark.table(parent_table)

        return (
        child_df.join(parent_df, key, "left_anti").select(col(key)).agg(count("*").alias("missing")).withColumn("status", when(col("missing") > 0 , lit("Fail")).otherwise(lit("PASS")))).show()
    