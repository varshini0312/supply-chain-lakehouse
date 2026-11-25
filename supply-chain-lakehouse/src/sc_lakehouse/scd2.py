"""SCD Type 2 helper for slowly changing dimensions (e.g., supplier rating changes)."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def apply_scd2(spark: SparkSession, updates: DataFrame, target: str, key: str, tracked_cols: list):
    change_cond = " OR ".join(f"t.{c} <> s.{c}" for c in tracked_cols)
    updates = (updates.withColumn("effective_from", F.current_timestamp())
                      .withColumn("effective_to", F.lit(None).cast("timestamp"))
                      .withColumn("is_current", F.lit(True)))
    if not spark.catalog.tableExists(target):
        updates.write.format("delta").saveAsTable(target)
        return
    tgt = DeltaTable.forName(spark, target)
    # Step 1: close changed current rows
    (tgt.alias("t").merge(updates.alias("s"), f"t.{key} = s.{key} AND t.is_current = true")
        .whenMatchedUpdate(condition=change_cond,
                           set={"is_current": "false", "effective_to": "s.effective_from"})
        .execute())
    # Step 2: insert new versions for keys with no current row
    current_keys = spark.table(target).filter("is_current").select(key)
    new_rows = updates.join(current_keys, key, "left_anti")
    new_rows.write.format("delta").mode("append").saveAsTable(target)
