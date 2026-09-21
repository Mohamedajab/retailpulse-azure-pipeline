# Dashboard build notes

Load `sales_detail.csv` and a contiguous date table. Relate `Date[Date]` one-to-many to
`sales_detail[order_date]`, mark the date table, then add the measures in
`powerbi/measures.dax`.

The suggested first page has four restrained elements: revenue and order cards, a
monthly revenue line, a top-products bar chart, and a regional matrix. A second customer
page holds repeat-customer count and customer order frequency. Use month, region and
category slicers on both pages. Avoid combining revenue and units on one axis; the scales
answer different questions.

Refresh order: land files, complete the curation job, reconcile accepted and rejected
counts, then refresh the semantic model. Do not refresh Power BI directly from bronze.

