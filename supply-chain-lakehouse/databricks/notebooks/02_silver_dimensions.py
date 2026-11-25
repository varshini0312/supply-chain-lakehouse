# Databricks notebook source
# MAGIC %md # 02 - Silver dimensions (dedupe + SCD2 for supplier)

# COMMAND ----------
import sys; sys.path.append("../../src")
from pyspark.sql import functions as F, Window
from sc_lakehouse.scd2 import apply_scd2

dbutils.widgets.text("env", "dev")
catalog = f"supply_chain_{dbutils.widgets.get('env')}"

def latest(df, key):
    w = Window.partitionBy(key).orderBy(F.col("updated_at").desc(), F.col("_ingest_ts").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")

# COMMAND ----------
supplier = latest(spark.table(f"{catalog}.bronze.supplier_raw"), "supplier_id") \
    .select("supplier_id", "supplier_name", "country", "contact_email", "rating", "updated_at")
apply_scd2(spark, supplier, f"{catalog}.silver.supplier", "supplier_id", ["supplier_name", "country", "rating"])

for t, k in [("product", "product_id"), ("warehouse", "warehouse_id")]:
    df = latest(spark.table(f"{catalog}.bronze.{t}_raw"), k).drop("_ingest_ts", "_source_file", "_rescued_data")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.{t}")
