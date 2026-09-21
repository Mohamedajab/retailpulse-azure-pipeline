# Architecture and operating model

```mermaid
flowchart LR
  S[Supplier CSV drops] -->|scheduled copy| A[Data Factory]
  A --> B[(ADLS Gen2 / bronze)]
  B --> C[Databricks PySpark job]
  C -->|invalid + reason| Q[(quarantine)]
  C --> D[(Delta / silver)]
  D --> E[(reporting tables / gold)]
  E --> F[Power BI semantic model]
  A -. run metadata .-> M[Azure Monitor]
  C -. quality metrics .-> M
```

Raw files are immutable and date-partitioned in bronze. The silver job applies types,
business-key deduplication and referential checks. Gold contains only report-shaped
tables; Power BI does not query raw files. Rejected rows retain the input fields and a
reason so an operator can correct and replay them.

For a production deployment, use managed identities between services, Key Vault for
external credentials, private endpoints for storage, and lifecycle rules on bronze.
The batch date should be passed by Data Factory rather than inferred from wall-clock time.

