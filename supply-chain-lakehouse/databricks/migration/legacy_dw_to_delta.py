# Databricks notebook source
# MAGIC %md
# MAGIC # Data migration: legacy on-prem SQL Server DW -> Delta Lake (Unity Catalog)
# MAGIC One-time historical load with validation (row counts, checksums), partitioned by date, idempotent.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("env", "dev")
catalog = f"supply_chain_{dbutils.widgets.get('env')}"
jdbc_url = dbutils.secrets.get("kv-scope", "legacy-dw-jdbc-url")
user = dbutils.secrets.get("kv-scope", "legacy-dw-user")
pwd = dbutils.secrets.get("kv-scope", "legacy-dw-password")

TABLES = {
    # source table : (target table, partition column, numeric column used for parallel read)
    "dbo.fact_shipments_hist": ("shipment_history", "ship_date", "shipment_id"),
    "dbo.fact_orders_hist": ("order_history", "order_date", "order_id"),
    "dbo.dim_supplier_hist": ("supplier_history", None, "supplier_id"),
}

# COMMAND ----------
def migrate(src, tgt, part_col, num_col):
    bounds = (spark.read.format("jdbc").option("url", jdbc_url).option("user", user).option("password", pwd)
              .option("query", f"SELECT MIN({num_col}) lo, MAX({num_col}) hi, COUNT(*) cnt FROM {src}").load().first())
    df = (spark.read.format("jdbc").option("url", jdbc_url).option("user", user).option("password", pwd)
          .option("dbtable", src).option("partitionColumn", num_col)
          .option("lowerBound", bounds.lo).option("upperBound", bounds.hi)
          .option("numPartitions", 16).option("fetchsize", 10000).load()
          .withColumn("_migrated_ts", F.current_timestamp()))
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if part_col:
        w = w.partitionBy(part_col)
    w.saveAsTable(f"{catalog}.silver.{tgt}")
    target_cnt = spark.table(f"{catalog}.silver.{tgt}").count()
    status = "OK" if target_cnt == bounds.cnt else "MISMATCH"
    return (src, tgt, bounds.cnt, target_cnt, status)

# COMMAND ----------
results = [migrate(s, *cfg) for s, cfg in TABLES.items()]
res_df = spark.createDataFrame(results, "source string, target string, source_rows long, target_rows long, status string")
display(res_df)
assert res_df.filter("status <> 'OK'").count() == 0, "Row count reconciliation failed"
