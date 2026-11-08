# RetailPulse — Azure retail data pipeline

RetailPulse reads customer, product and order CSV files and builds sales reports.
It has a local Python runner and a PySpark job for Azure Databricks or Synapse.
Rejected orders are saved with a reason, and each run reports the input and output counts.

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

## Azure setup

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
price is non-negative, and both foreign keys resolve. For duplicate business keys, the row with the newest
valid `updated_at` is kept. The tie rules are described below.

## Deliberate limits

Power BI measures, model notes and sample datasets are included. A `.pbix` file is
not included; use the notes and generated tables to build the dashboard.

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
local Python runner instead. CI checks Spark Parquet writes on Linux.

## Duplicate selection and limits

Both runners check every `updated_at` before selecting the newest record
per business key. ISO timestamps must include seconds (up to six fractional
digits); timezone-free values mean UTC and offsets are normalised to UTC.
Missing or invalid timestamps fail the run, even on superseded rows. Equal
timestamps select the lexicographically greatest tuple of remaining source
values, in alphabetical column-name order; null/empty values compare as empty
strings. Both runners use the same parser.

`source_orders = accepted_orders + rejected_orders + order_duplicates_removed`.
The broader `duplicate_records_removed` also includes customer/product duplicates.
Local and Linux Spark tests cover timestamps and tied rows. Prices can still
differ: the local runner rounds Python Decimal values after multiplication, while
Spark casts to `decimal(12,2)` first. Rounding, overflow and output order can differ.
