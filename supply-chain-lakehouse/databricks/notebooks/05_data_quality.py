# Databricks notebook source
# MAGIC %md # 05 - Data quality checks (governance)

# COMMAND ----------
import sys; sys.path.append("../../src")
from sc_lakehouse.dq import run_checks

dbutils.widgets.text("env", "dev")
catalog = f"supply_chain_{dbutils.widgets.get('env')}"

checks = [
    {"table": "silver.inventory", "name": "no_negative_on_hand", "severity": "error",
     "sql_violation_query": f"SELECT 1 FROM {catalog}.silver.inventory WHERE qty_on_hand < 0"},
    {"table": "silver.shipment", "name": "delivery_after_ship", "severity": "warn",
     "sql_violation_query": f"SELECT 1 FROM {catalog}.silver.shipment WHERE delivery_date < ship_date"},
    {"table": "silver.customer_order", "name": "order_id_unique", "severity": "error",
     "sql_violation_query": f"SELECT order_id FROM {catalog}.silver.customer_order GROUP BY order_id HAVING COUNT(*) > 1"},
    {"table": "gold.inventory_position", "name": "gold_not_empty", "severity": "error",
     "sql_violation_query": f"SELECT 1 FROM (SELECT COUNT(*) c FROM {catalog}.gold.inventory_position) WHERE c = 0"},
]
print(run_checks(spark, catalog, checks))
