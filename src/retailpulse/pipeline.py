"""Local reference implementation of the RetailPulse transformations.

The production-shaped PySpark job lives in ``spark_jobs/``. This standard-library
version keeps the project quick to run in CI and documents the same business rules.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
CURATED = ROOT / "data" / "curated"
REJECTED = ROOT / "data" / "rejected"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[dict[str, object]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def deduplicate(rows: list[dict[str, str]], key: str) -> tuple[list[dict[str, str]], int]:
    """Keep the last record for a business key, matching the batch overwrite rule."""
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        result[row[key]] = row
    return list(result.values()), len(rows) - len(result)


def curate(raw_dir: Path = RAW, curated_dir: Path = CURATED, rejected_dir: Path = REJECTED) -> dict[str, int]:
    customers, customer_dupes = deduplicate(read_csv(raw_dir / "customers.csv"), "customer_id")
    products, product_dupes = deduplicate(read_csv(raw_dir / "products.csv"), "product_id")
    orders, order_dupes = deduplicate(read_csv(raw_dir / "orders.csv"), "order_id")

    customer_by_id = {r["customer_id"]: r for r in customers}
    product_by_id = {r["product_id"]: r for r in products}
    accepted: list[dict[str, object]] = []
    rejected: list[dict[str, object]] = []

    for row in orders:
        reasons: list[str] = []
        try:
            quantity = int(row["quantity"])
            unit_price = Decimal(row["unit_price"])
            datetime.strptime(row["order_date"], "%Y-%m-%d")
            if quantity <= 0 or unit_price < 0:
                reasons.append("non_positive_quantity_or_negative_price")
        except (ValueError, InvalidOperation):
            reasons.append("invalid_numeric_or_date_value")
            quantity, unit_price = 0, Decimal("0")
        if row["customer_id"] not in customer_by_id:
            reasons.append("unknown_customer")
        if row["product_id"] not in product_by_id:
            reasons.append("unknown_product")
        if reasons:
            rejected.append({**row, "rejection_reason": ";".join(reasons)})
            continue

        customer = customer_by_id[row["customer_id"]]
        product = product_by_id[row["product_id"]]
        revenue = (unit_price * quantity).quantize(Decimal("0.01"))
        accepted.append(
            {
                "order_id": row["order_id"],
                "order_date": row["order_date"],
                "month": row["order_date"][:7],
                "customer_id": row["customer_id"],
                "customer_name": customer["customer_name"],
                "region": customer["region"] or "Unknown",
                "product_id": row["product_id"],
                "product_name": product["product_name"],
                "category": product["category"],
                "quantity": quantity,
                "revenue": f"{revenue:.2f}",
            }
        )

    sales_fields = [
        "order_id", "order_date", "month", "customer_id", "customer_name",
        "region", "product_id", "product_name", "category", "quantity", "revenue",
    ]
    write_csv(curated_dir / "sales_detail.csv", accepted, sales_fields)
    write_csv(rejected_dir / "orders.csv", rejected, list(orders[0]) + ["rejection_reason"])

    monthly: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
    customer_orders: Counter[str] = Counter()
    product_units: Counter[str] = Counter()
    for row in accepted:
        monthly[(str(row["month"]), str(row["region"]))] += Decimal(str(row["revenue"]))
        customer_orders[str(row["customer_id"])] += 1
        product_units[str(row["product_name"])] += int(row["quantity"])

    summary = [
        {"month": month, "region": region, "revenue": f"{value:.2f}"}
        for (month, region), value in sorted(monthly.items())
    ]
    write_csv(curated_dir / "monthly_region_sales.csv", summary, ["month", "region", "revenue"])
    write_csv(
        curated_dir / "product_popularity.csv",
        ({"product_name": key, "units_sold": value} for key, value in product_units.most_common()),
        ["product_name", "units_sold"],
    )

    metrics = {
        "source_orders": len(orders) + order_dupes,
        "accepted_orders": len(accepted),
        "rejected_orders": len(rejected),
        "duplicate_records_removed": customer_dupes + product_dupes + order_dupes,
        "repeat_customers": sum(count > 1 for count in customer_orders.values()),
    }
    write_csv(curated_dir / "pipeline_metrics.csv", [metrics], list(metrics))
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Build RetailPulse curated CSV datasets")
    parser.add_argument("--raw", type=Path, default=RAW)
    parser.add_argument("--output", type=Path, default=CURATED)
    parser.add_argument("--rejected", type=Path, default=REJECTED)
    args = parser.parse_args()
    metrics = curate(args.raw, args.output, args.rejected)
    print("RetailPulse run complete:", ", ".join(f"{k}={v}" for k, v in metrics.items()))


if __name__ == "__main__":
    main()

