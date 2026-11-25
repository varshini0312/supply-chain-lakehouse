# Databricks notebook source
# MAGIC %md # 04 - Gold marts (integration of ERP + WMS + supplier data)

# COMMAND ----------
import sys; sys.path.append("../../src")
from pyspark.sql import functions as F
from sc_lakehouse.transforms import add_delivery_flags, days_of_cover

dbutils.widgets.text("env", "dev")
catalog = f"supply_chain_{dbutils.widgets.get('env')}"
t = lambda n, l="silver": spark.table(f"{catalog}.{l}.{n}")

# COMMAND ----------
# 1) Shipment OTIF
otif = add_delivery_flags(t("shipment"), t("customer_order"))
shipment_otif = (otif.groupBy("carrier")
    .agg(F.count("*").alias("shipments"),
         F.avg(F.col("on_time").cast("int")).alias("on_time_rate"),
         F.avg(F.col("in_full").cast("int")).alias("in_full_rate"),
         F.avg(F.col("otif").cast("int")).alias("otif_rate")))
shipment_otif.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.shipment_otif")

# COMMAND ----------
# 2) Inventory position with days of cover
demand = (t("customer_order").filter("order_ts >= date_sub(current_date(), 30)")
          .groupBy("product_id", "warehouse_id")
          .agg((F.sum("qty_ordered") / 30).alias("avg_daily_demand")))
inv = (t("inventory").join(t("product"), "product_id").join(t("warehouse"), "warehouse_id")
       .join(demand, ["product_id", "warehouse_id"], "left")
       .withColumn("available_qty", F.col("qty_on_hand") - F.col("qty_reserved"))
       .withColumn("days_of_cover", days_of_cover("available_qty", "avg_daily_demand"))
       .select("product_id", "sku", "product_name", "warehouse_id", "warehouse_name",
               "qty_on_hand", "qty_reserved", "available_qty", "avg_daily_demand", "days_of_cover"))
inv.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.inventory_position")

# COMMAND ----------
# 3) Supplier scorecard
sup = t("supplier").filter("is_current")
po = (t("purchase_order").withColumn("lead_days", F.datediff("expected_date", "po_date"))
      .groupBy("supplier_id").agg(F.avg("lead_days").alias("avg_lead_days"),
                                  F.count("*").alias("po_count"),
                                  F.avg((F.col("status") == "RECEIVED").cast("int")).alias("fill_rate")))
sup.join(po, "supplier_id", "left").select("supplier_id", "supplier_name", "country", "rating",
    "avg_lead_days", "po_count", "fill_rate") \
   .write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.supplier_scorecard")

# COMMAND ----------
# 4) Order fulfillment fact
(t("customer_order").alias("o").join(t("shipment").alias("s"), "order_id", "left")
 .withColumn("order_to_delivery_days", F.datediff("s.delivery_date", F.to_date("o.order_ts")))
 .select("order_id", "customer_id", "product_id", "warehouse_id", "qty_ordered", "qty_shipped",
         "carrier", "order_ts", "ship_date", "delivery_date", "order_to_delivery_days")
 .write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.order_fulfillment_fact"))
