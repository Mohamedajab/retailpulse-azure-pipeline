"""Azure Databricks/Synapse PySpark job for the silver and gold layers."""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def latest(df, key: str, timestamp_col: str):
    window = Window.partitionBy(key).orderBy(F.col(timestamp_col).desc())
    return df.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")


def run(raw_uri: str, curated_uri: str) -> None:
    spark = SparkSession.builder.appName("retailpulse-curation").getOrCreate()
    customers = latest(spark.read.option("header", True).csv(f"{raw_uri}/customers"), "customer_id", "updated_at")
    products = latest(spark.read.option("header", True).csv(f"{raw_uri}/products"), "product_id", "updated_at")
    orders = latest(spark.read.option("header", True).csv(f"{raw_uri}/orders"), "order_id", "updated_at")

    typed = (orders
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)")))
    valid = typed.filter(
        F.col("order_date").isNotNull() & (F.col("quantity") > 0) & (F.col("unit_price") >= 0)
    )
    sales = (valid.join(customers, "customer_id", "inner")
        .join(products, "product_id", "inner")
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .withColumn("month", F.date_format("order_date", "yyyy-MM")))

    sales.write.format("delta").mode("overwrite").partitionBy("month").save(f"{curated_uri}/sales_detail")
    (sales.groupBy("month", "region").agg(F.sum("revenue").alias("revenue"))
        .write.format("delta").mode("overwrite").save(f"{curated_uri}/monthly_region_sales"))


if __name__ == "__main__":
    import sys
    run(sys.argv[1], sys.argv[2])

