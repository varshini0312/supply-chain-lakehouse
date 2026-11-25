# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver CDC merge
# MAGIC Applies SQL Server CDC events (insert/update/delete) from bronze to silver Delta tables with `MERGE`.
# MAGIC Change Data Feed is enabled on silver so gold can read only changes.

# COMMAND ----------
import sys; sys.path.append("../../src")
from sc_lakehouse.cdc import merge_cdc
from sc_lakehouse.config import PRIMARY_KEYS

dbutils.widgets.text("env", "dev")
catalog = f"supply_chain_{dbutils.widgets.get('env')}"

# COMMAND ----------
for entity in ["inventory", "purchase_order", "customer_order", "shipment"]:
    changes = spark.table(f"{catalog}.bronze.{entity}_cdc")
    merge_cdc(spark, changes, f"{catalog}.silver.{entity}", PRIMARY_KEYS[entity])
    spark.sql(f"ALTER TABLE {catalog}.silver.{entity} SET TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true)")
    print(f"merged {entity}")

# COMMAND ----------
# Delta maintenance
for entity in ["inventory", "purchase_order", "customer_order", "shipment"]:
    spark.sql(f"OPTIMIZE {catalog}.silver.{entity} ZORDER BY ({PRIMARY_KEYS[entity][0]})")

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Example: time travel & change data feed
# MAGIC -- SELECT * FROM table_changes('supply_chain_dev.silver.shipment', 1);
# MAGIC -- SELECT * FROM supply_chain_dev.silver.shipment VERSION AS OF 3;
