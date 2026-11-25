"""Reusable PySpark transformations (unit-testable, no I/O)."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def add_ingestion_metadata(df: DataFrame, source: str) -> DataFrame:
    return (df.withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_source", F.lit(source))
              .withColumn("_source_file", F.col("_metadata.file_path")))


def standardize_columns(df: DataFrame) -> DataFrame:
    """snake_case + trim column names."""
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))
    return df


def latest_cdc_per_key(df: DataFrame, keys: list, order_col: str = "cdc_commit_ts") -> DataFrame:
    """Keep the latest CDC event per business key.

    SQL Server CDC __$operation: 1=delete, 2=insert, 3=update-before, 4=update-after.
    Update-before rows (3) are dropped.
    """
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc(), F.col("__$seqval").desc())
    return (df.filter(F.col("__$operation") != 3)
              .withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn"))


def add_delivery_flags(shipments: DataFrame, orders: DataFrame) -> DataFrame:
    """Compute on-time and in-full flags for OTIF."""
    j = shipments.alias("s").join(orders.alias("o"), "order_id")
    return (j.withColumn("on_time", F.col("s.delivery_date") <= F.col("o.promised_date"))
             .withColumn("in_full", F.col("s.qty_shipped") >= F.col("o.qty_ordered"))
             .withColumn("otif", F.col("on_time") & F.col("in_full")))


def days_of_cover(qty_on_hand_col: str, avg_daily_demand_col: str):
    return F.when(F.col(avg_daily_demand_col) > 0,
                  F.col(qty_on_hand_col) / F.col(avg_daily_demand_col)).otherwise(F.lit(None))
