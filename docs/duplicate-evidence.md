# Duplicate handling

Both runners keep the row with the newest `updated_at` for each business key.
They use the same timestamp parser and compare tied rows using the remaining
columns in alphabetical order.

Tests check reordered input, tied times, UTC offsets, missing timestamps and invalid
dates. All 11 local tests passed. The Linux Spark run passed all 12 tests, including
writing Parquet files.

The sample report was rebuilt from a local run:

**8 source orders = 5 accepted + 2 rejected + 1 duplicate removed.**

Three duplicates were removed across the customer, product and order files.
`order_duplicates_removed` counts orders separately, so the totals use the run's
actual counts rather than a fixed number from the sample.

The runners still differ when handling some numbers. Python rounds Decimal values
after multiplication; Spark converts prices to decimal(12,2) first. Rounding,
overflow, rejection details and output order can therefore differ.
