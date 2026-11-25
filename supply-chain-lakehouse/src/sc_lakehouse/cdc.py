"""Generic Delta Lake CDC merge (upsert + delete) from bronze CDC feed into silver."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from .transforms import latest_cdc_per_key

CDC_META_COLS = ["__$start_lsn", "__$end_lsn", "__$seqval", "__$operation", "__$update_mask", "cdc_commit_ts"]


def merge_cdc(spark: SparkSession, changes: DataFrame, target_table: str, keys: list) -> None:
    latest = latest_cdc_per_key(changes, keys)
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    data_cols = [c for c in latest.columns if c not in CDC_META_COLS and not c.startswith("_")]
    set_map = {c: f"s.{c}" for c in data_cols}

    if not spark.catalog.tableExists(target_table):
        (latest.filter("__$operation != 1").select(*data_cols)
               .write.format("delta").option("delta.enableChangeDataFeed", "true")
               .saveAsTable(target_table))
        return

    (DeltaTable.forName(spark, target_table).alias("t")
        .merge(latest.alias("s"), cond)
        .whenMatchedDelete(condition="s.`__$operation` = 1")
        .whenMatchedUpdate(condition="s.`__$operation` IN (2,4)", set=set_map)
        .whenNotMatchedInsert(condition="s.`__$operation` IN (2,4)", values=set_map)
        .execute())
