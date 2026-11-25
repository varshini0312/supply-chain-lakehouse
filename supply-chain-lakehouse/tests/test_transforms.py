from datetime import date
from sc_lakehouse.transforms import latest_cdc_per_key, add_delivery_flags


def test_latest_cdc_drops_update_before_and_keeps_latest(spark):
    rows = [
        (1, 3, "2026-11-01 10:00:00", 1, "old"),
        (1, 4, "2026-11-01 10:00:00", 2, "new"),
        (2, 2, "2026-11-01 09:00:00", 1, "ins"),
        (2, 1, "2026-11-01 11:00:00", 2, "ins"),
    ]
    df = spark.createDataFrame(rows, ["id", "__$operation", "cdc_commit_ts", "__$seqval", "val"]) \
              .selectExpr("id", "`__$operation`", "to_timestamp(cdc_commit_ts) as cdc_commit_ts",
                          "`__$seqval`", "val")
    out = {r["id"]: r for r in latest_cdc_per_key(df, ["id"]).collect()}
    assert out[1]["val"] == "new"
    assert out[2]["__$operation"] == 1  # delete is the latest event


def test_otif_flags(spark):
    s = spark.createDataFrame([(1, date(2026, 11, 4), 200), (2, date(2026, 11, 8), 40)],
                              ["order_id", "delivery_date", "qty_shipped"])
    o = spark.createDataFrame([(1, date(2026, 11, 5), 200), (2, date(2026, 11, 6), 50)],
                              ["order_id", "promised_date", "qty_ordered"])
    res = {r["order_id"]: r for r in add_delivery_flags(s, o).collect()}
    assert res[1]["otif"] is True
    assert res[2]["on_time"] is False and res[2]["in_full"] is False
