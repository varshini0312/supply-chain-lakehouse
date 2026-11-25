"""Lightweight data-quality framework writing results to governance.dq_results."""
from datetime import datetime
from pyspark.sql import SparkSession


def run_checks(spark: SparkSession, catalog: str, checks: list) -> list:
    """checks: list of dicts {table, name, sql_violation_query, severity}"""
    results = []
    for c in checks:
        violations = spark.sql(c["sql_violation_query"]).count()
        results.append({
            "run_ts": datetime.utcnow(), "table_name": c["table"], "check_name": c["name"],
            "violations": violations, "severity": c.get("severity", "warn"),
            "passed": violations == 0,
        })
    if results:
        (spark.createDataFrame(results).write.format("delta").mode("append")
              .saveAsTable(f"{catalog}.governance.dq_results"))
    failed = [r for r in results if not r["passed"] and r["severity"] == "error"]
    if failed:
        raise RuntimeError(f"Data quality failed: {[f['check_name'] for f in failed]}")
    return results
