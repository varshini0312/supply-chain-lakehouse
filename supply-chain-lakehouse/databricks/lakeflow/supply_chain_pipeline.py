# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Declarative Pipeline (formerly Delta Live Tables)
# MAGIC Declarative bronze -> silver -> gold with expectations (DQ) and `apply_changes` for CDC.

import dlt
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path", "abfss://landing@<storage-account>.dfs.core.windows.net")

# ---------------- Bronze: Auto Loader streaming tables ----------------
def bronze_cdc(entity):
    @dlt.table(name=f"{entity}_cdc_bronze", comment=f"Raw CDC feed for {entity}",
               table_properties={"quality": "bronze"})
    def _t():
        return (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.inferColumnTypes", "true")
                .load(f"{LANDING}/cdc/{entity}")
                .withColumn("_ingest_ts", F.current_timestamp()))

for e in ["inventory", "purchase_order", "customer_order", "shipment"]:
    bronze_cdc(e)

# ---------------- Silver: CDC applied with apply_changes ----------------
def silver_cdc(entity, key, seq_col="cdc_commit_ts"):
    dlt.create_streaming_table(
        name=f"{entity}_silver",
        comment=f"Current state of {entity}",
        table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
        expect_all_or_drop={"valid_key": f"{key} IS NOT NULL"},
    )
    dlt.apply_changes(
        target=f"{entity}_silver",
        source=f"{entity}_cdc_bronze",
        keys=[key],
        sequence_by=F.struct(F.col(seq_col), F.col("`__$seqval`")),
        apply_as_deletes=F.expr("`__$operation` = 1"),
        apply_as_truncates=None,
        except_column_list=["__$start_lsn", "__$end_lsn", "__$seqval", "__$operation",
                            "__$update_mask", "cdc_commit_ts", "_ingest_ts", "_rescued_data"],
        stored_as_scd_type=1,
    )

silver_cdc("inventory", "inventory_id")
silver_cdc("purchase_order", "po_id")
silver_cdc("customer_order", "order_id")
silver_cdc("shipment", "shipment_id")

# ---------------- Gold ----------------
@dlt.table(name="shipment_otif_gold", comment="OTIF by carrier", table_properties={"quality": "gold"})
@dlt.expect_or_fail("rate_between_0_1", "otif_rate BETWEEN 0 AND 1")
def shipment_otif_gold():
    s = dlt.read("shipment_silver").alias("s")
    o = dlt.read("customer_order_silver").alias("o")
    j = s.join(o, "order_id")
    return (j.withColumn("otif", ((F.col("s.delivery_date") <= F.col("o.promised_date")) &
                                  (F.col("s.qty_shipped") >= F.col("o.qty_ordered"))).cast("int"))
             .groupBy("carrier").agg(F.count("*").alias("shipments"), F.avg("otif").alias("otif_rate")))

@dlt.table(name="open_orders_gold", comment="Orders not yet delivered")
def open_orders_gold():
    return dlt.read("customer_order_silver").filter("status <> 'DELIVERED'")
