# Databricks notebook source
# MAGIC %md # 00 - Setup & config

# COMMAND ----------
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
catalog = f"supply_chain_{env}"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE TABLE IF NOT EXISTS governance.dq_results (run_ts TIMESTAMP, table_name STRING, check_name STRING, violations LONG, severity STRING, passed BOOLEAN)")
spark.sql("CREATE TABLE IF NOT EXISTS governance.pipeline_audit (run_ts TIMESTAMP, layer STRING, table_name STRING, rows_written LONG)")
print(f"Using catalog {catalog}")
