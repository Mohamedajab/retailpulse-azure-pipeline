"""Retail curation for Spark; Delta on Azure or Parquet for a local demonstration."""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def latest(df, key, timestamp_col):
    window = Window.partitionBy(key).orderBy(F.col(timestamp_col).desc())
    return df.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")


def transform(customers, products, orders):
    customers = latest(customers, "customer_id", "updated_at").select(
        "customer_id", "customer_name", "region"
    )
    products = latest(products, "product_id", "updated_at").select(
        "product_id", "product_name", "category"
    )
    orders = latest(orders, "order_id", "updated_at")
    checked = (orders
        .withColumn("_date", F.expr("try_cast(order_date as date)"))
        .withColumn("_quantity", F.expr("try_cast(quantity as int)"))
        .withColumn("_price", F.expr("try_cast(unit_price as decimal(12,2))"))
        .join(customers, "customer_id", "left")
        .join(products, "product_id", "left"))
    checked = checked.withColumn("rejection_reason", F.concat_ws(";",
        F.when(F.col("_date").isNull() | F.col("_quantity").isNull()
               | F.col("_price").isNull(), "invalid_numeric_or_date_value"),
        F.when((F.col("_quantity") <= 0) | (F.col("_price") < 0),
               "non_positive_quantity_or_negative_price"),
        F.when(F.col("customer_name").isNull(), "unknown_customer"),
        F.when(F.col("product_name").isNull(), "unknown_product")))
    rejected = checked.filter(F.col("rejection_reason") != "").select(
        *orders.columns, "rejection_reason"
    )
    sales = (checked.filter(F.col("rejection_reason") == "")
        .select("order_id", F.col("_date").alias("order_date"),
                F.date_format("_date", "yyyy-MM").alias("month"),
                "customer_id", "customer_name",
                F.when(F.col("region").isNull() | (F.trim("region") == ""), "Unknown")
                 .otherwise(F.col("region")).alias("region"),
                "product_id", "product_name", "category",
                F.col("_quantity").alias("quantity"),
                (F.col("_quantity") * F.col("_price")).alias("revenue")))
    return sales, rejected


def run(raw_uri, curated_uri, output_format="delta"):
    spark = SparkSession.builder.appName("retailpulse-curation").getOrCreate()
    inputs = [spark.read.option("header", True).csv(f"{raw_uri}/{name}.csv")
              for name in ("customers", "products", "orders")]
    sales, rejected = transform(*inputs)
    sales = sales.cache()
    rejected = rejected.cache()
    outputs = {
        "sales_detail": sales,
        "rejected_orders": rejected,
        "monthly_region_sales": sales.groupBy("month", "region").agg(F.sum("revenue").alias("revenue")),
        "product_popularity": sales.groupBy("product_name").agg(F.sum("quantity").alias("units_sold")),
    }
    metrics = {
        "source_orders": inputs[2].count(),
        "accepted_orders": sales.count(),
        "rejected_orders": rejected.count(),
        "duplicate_records_removed": sum(df.count() - latest(df, key, "updated_at").count()
            for df, key in zip(inputs, ("customer_id", "product_id", "order_id"))),
        "repeat_customers": sales.groupBy("customer_id").count().filter("count > 1").count(),
    }
    outputs["pipeline_metrics"] = spark.createDataFrame([metrics])
    for name, frame in outputs.items():
        frame.write.format(output_format).mode("overwrite").save(f"{curated_uri}/{name}")
    sales.unpersist()
    rejected.unpersist()
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_uri")
    parser.add_argument("curated_uri")
    parser.add_argument("--format", choices=("delta", "parquet"), default="delta")
    args = parser.parse_args()
    print(run(args.raw_uri, args.curated_uri, args.format))
