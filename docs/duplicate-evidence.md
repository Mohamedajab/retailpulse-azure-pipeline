# Duplicate selection evidence

The local and Spark paths now use the same UTC timestamp parser and fixed
alphabetical source-column tie order. Local tests exercise every permutation of
three duplicates, an offset timestamp tied with a naive UTC timestamp, invalid
dates, missing timestamps and malformed offsets. Spark's Linux CI exercises the
same selection with reordered input and fails on an invalid timestamp.

The checked-in sample HTML report was regenerated from a local run:
8 source orders = 5 accepted + 2 rejected + 1 superseded order.
Three duplicates were removed across all input tables. The separate order count
prevents reconciliation from depending on a sample-specific constant.

No claim of complete engine parity is made. Python Decimal quantises after
multiplication; Spark first casts prices to decimal(12,2). Numeric rounding,
overflow, validation reason details and aggregation order can still differ.
