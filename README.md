# RetailPulse — Azure retail data pipeline

RetailPulse turns three untidy retail extracts into small, dependable reporting tables.
It includes a lightweight local runner for review and CI, plus a PySpark job shaped for
Azure Databricks or Synapse. Bad orders are quarantined instead of disappearing, and each
run publishes counts that reconcile back to the source.

## What is here

- a bronze → silver → gold design for Azure Data Lake Storage;
- business-key deduplication and customer/product integrity checks;
- detailed sales, monthly regional sales and product-popularity outputs;
- tests for reconciliation and transformation rules;
- DAX measures and practical Power BI build notes.

## Run locally

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
python -m pip install -e ".[dev]"
python -m retailpulse.pipeline
pytest -q
```

The sample deliberately contains stale duplicates, a missing region, an unknown customer
and a negative quantity. Inspect `data/curated/` and `data/rejected/` after the run.

## Production path

Data Factory lands dated files in ADLS bronze and invokes
`spark_jobs/curate_retail.py`. Delta tables in silver retain typed detail; gold contains
the report-shaped aggregations. See [the architecture](docs/architecture.md) and
[dashboard notes](docs/dashboard.md).

## Data contract

| File | Business key | Required fields |
|---|---|---|
| customers | `customer_id` | name, email |
| products | `product_id` | name, category, list price |
| orders | `order_id` | date, customer, product, quantity, unit price |

An order is accepted only when its date and numeric fields parse, quantity is positive,
price is non-negative, and both foreign keys resolve. The last record received for a
business key wins within a batch.

## Deliberate limits

The repository does not pretend a `.pbix` file can be meaningfully reviewed in source
control. It keeps the semantic measures, model guidance and generated datasets visible;
the visual report can be built from those assets without hidden transformation logic.

The Spark runner writes sales detail, quarantined orders with reasons, monthly regional
sales, product popularity and reconciled run metrics. It accepts the same CSV filenames as
the local runner. On Databricks use Delta (the default); for local execution with Spark 4
and Java 17 or 21, use Parquet:

```bash
python spark_jobs/curate_retail.py data/raw output/spark --format parquet
```

## View the report

Run the local pipeline, then `python scripts/build_report.py`. Open
[the standalone report](docs/report.html) in a browser. It contains sample sales,
product popularity and reconciliation figures; regenerate it after changing inputs.
Azure deployment requires your storage locations, identities and a Delta-enabled runtime.

On Windows, Spark file writes additionally require Hadoop Windows binaries; use the
standard-library local runner for the interview demo. CI verifies Spark Parquet writes on Linux.
