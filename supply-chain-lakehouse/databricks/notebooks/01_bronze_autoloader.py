# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion with Auto Loader
# MAGIC Incrementally ingests Parquet landed by ADF (SQL + CDC) and vendor files from ADLS Gen2 into Delta bronze tables.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_date", "")
env = dbutils.widgets.get("env")
catalog = f"supply_chain_{env}"
storage = "<storage-account>"
landing = f"abfss://landing@{storage}.dfs.core.windows.net"
chk_base = f"abfss://lakehouse@{storage}.dfs.core.windows.net/supply_chain/{env}/_checkpoints"

dim_tables = ["supplier", "product", "warehouse"]
cdc_tables = ["inventory", "purchase_order", "customer_order", "shipment"]

# COMMAND ----------
def ingest(source_path, target, fmt="parquet", schema_loc=None, **opts):
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaLocation", schema_loc)
              .option("cloudFiles.inferColumnTypes", "true")
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
    for k, v in opts.items():
        reader = reader.option(k, v)
    df = (reader.load(source_path)
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path")))
    q = (df.writeStream.format("delta")
         .option("checkpointLocation", f"{chk_base}/{target}")
         .option("mergeSchema", "true")
         .trigger(availableNow=True)
         .toTable(f"{catalog}.bronze.{target}"))
    q.awaitTermination()

# COMMAND ----------
# ELT: dimension snapshots and CDC change feeds are loaded RAW (no business transforms)
for t in dim_tables:
    ingest(f"{landing}/scm/{t}", f"{t}_raw", schema_loc=f"{chk_base}/schemas/{t}")

for t in cdc_tables:
    ingest(f"{landing}/cdc/{t}", f"{t}_cdc", schema_loc=f"{chk_base}/schemas/{t}_cdc")

# COMMAND ----------
# ETL-style vendor feed: light validation applied on the way in (rescued data column kept)
ingest(f"{landing}/vendor_files", "vendor_asn_raw", fmt="parquet",
       schema_loc=f"{chk_base}/schemas/vendor_asn")
