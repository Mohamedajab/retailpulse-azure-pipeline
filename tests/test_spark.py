import importlib.util
import os
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from pyspark.errors import PythonException
from pyspark.sql import SparkSession

from retailpulse.pipeline import deduplicate


def test_spark_quarantines_and_reconciles_sample(tmp_path):
    root = Path(__file__).parents[1]
    spec = importlib.util.spec_from_file_location("spark_job", root / "spark_jobs/curate_retail.py")
    job = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job)
    spark = (SparkSession.builder.master("local[1]").appName("retailpulse-test")
             .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    try:
        rows = [
            {"id": "1", "value": "old", "updated_at": "2025-01-01T11:00:00Z"},
            {"id": "1", "value": "a", "updated_at": "2025-01-01T14:00:00+02:00"},
            {"id": "1", "value": "z", "updated_at": "2025-01-01T12:00:00"},
        ]
        expected, _ = deduplicate(rows, "id")
        for reordered in (rows, rows[::-1], [rows[1], rows[2], rows[0]]):
            actual = job.latest(spark.createDataFrame(reordered), "id", "updated_at")
            assert [r.asDict() for r in actual.collect()] == expected
        with pytest.raises(PythonException):
            job.latest(spark.createDataFrame([{"id": "1", "updated_at": "invalid"}]), "id", "updated_at")
        frames = [spark.read.option("header", True).csv(str(root / "data/raw" / (name + ".csv")))
                  for name in ("customers", "products", "orders")]
        sales, rejects = job.transform(*frames)
        assert sales.count() == 5
        assert rejects.count() == 2
        assert sales.filter("region = 'Unknown'").count() == 1
        assert {r.rejection_reason for r in rejects.collect()} == {
            "unknown_customer", "non_positive_quantity_or_negative_price"
        }
        assert float(sales.agg({"revenue": "sum"}).first()[0]) == 376.0
        if os.name != "nt":
            metrics = job.run(str(root / "data/raw"), str(tmp_path / "spark"), "parquet")
            assert metrics["source_orders"] == 8
            assert metrics["accepted_orders"] == 5
            assert metrics["source_orders"] == metrics["accepted_orders"] + metrics["rejected_orders"] + metrics["order_duplicates_removed"]
            for name in ("sales_detail", "rejected_orders", "monthly_region_sales", "product_popularity", "pipeline_metrics"):
                assert list((tmp_path / "spark" / name).glob("*.parquet"))
    finally:
        spark.stop()
