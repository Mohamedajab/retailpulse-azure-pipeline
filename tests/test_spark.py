import importlib.util
import os
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession


def test_spark_quarantines_and_reconciles_sample(tmp_path):
    root = Path(__file__).parents[1]
    spec = importlib.util.spec_from_file_location("spark_job", root / "spark_jobs/curate_retail.py")
    job = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job)
    spark = (SparkSession.builder.master("local[1]").appName("retailpulse-test")
             .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    try:
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
            for name in ("sales_detail", "rejected_orders", "monthly_region_sales", "product_popularity", "pipeline_metrics"):
                assert list((tmp_path / "spark" / name).glob("*.parquet"))
    finally:
        spark.stop()
