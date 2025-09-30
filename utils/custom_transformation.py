from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, filter, concat, row_number, current_timestamp, when, upper, split, regexp_replace, concat_ws
from pyspark.sql.window import Window
from delta.tables import DeltaTable

class Transform():

    # cdc: change data capture
    def deduplicate(self, df: DataFrame, dedup_list: List, cdc: str):
        # concat all the columns you want to dedup
        df = df.withColumn("dedup_key", concat(*dedup_list)) \
               .withColumn("dedup_count", row_number().over(Window.partitionBy("dedup_key").orderBy(col(cdc)))) \
               .filter(col("dedup_count") == 1) \
               .drop(["dedup_key", "dedup_count"])

        return df
    
    def processed_timestamp(self, df: DataFrame):
        return df.withColumn("processed_timestamp", current_timestamp())

    def upsert(self, source_df, key_columns, table_name, cdc):
        merge_conditions = ''.join([f"t.{key} = s.{key} AND " for key in key_columns])

        deltaTable = DeltaTable.forName(spark, f'uberproject.silver.{table_name}')
        deltaTable.alias("t").merge(source_df.alias("s"), merge_conditions) \
                            .whenMatchedUpdateAll(condition=f"s.{cdc} >= t.{cdc}") \
                            .whenNotMatchedInsertAll() \
                            .execute()
        return 1

        # Payment Case and When
    def payment_check(self, df):
        df = df.withColumn('online_payment_status', when(((col('payment_method') == 'Card') & (col('payment_status') == 'Success')), "Online-Success") \
                        .when(((col('payment_method') == 'Card') & (col('payment_status') == 'Failed')), "Online-Failed") \
                        .when(((col('payment_method') == 'Card') & (col('payment_status') == 'Pending')), "Online-Pending") \
                        .otherwise("Offline"))
        return df
    
    def uppercase(self, df: DataFrame, column: str):
        return df.withColumn(column, upper(col(column)))