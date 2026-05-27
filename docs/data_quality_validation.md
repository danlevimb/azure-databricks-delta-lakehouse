# Data Quality Validation

This document describes the data quality validation approach implemented in the `azure-databricks-delta-lakehouse` project.

The project validates data quality at two levels:

1. During Silver transformations, where invalid records are separated into rejected records.
2. During the final validation notebook, where the complete Lakehouse output is checked for consistency.

### 1. Data Quality Objective

The objective of data quality validation is to prove that the Lakehouse pipeline produces consistent, trusted, and explainable outputs.

The project does not only build Delta tables.

It also validates that the tables meet expected technical and business rules.

The validation process answers questions such as:

- Did the expected number of records arrive?
- Were invalid records rejected correctly?
- Are Gold tables free from duplicate business keys?
- Does each customer have one current SCD2 version?
- Are SCD2 effective dates valid?
- Did Delta MERGE update and insert the expected products?
- Does Delta history contain a MERGE operation?
- Do Gold summaries match fact table totals?
- Was the validation report persisted?

### 2. Data Quality Scope

Data quality validation covers the following layers:

```text
Bronze
Silver
Gold
Metadata
```

The project validates:

- Bronze row counts
- Silver clean table counts
- Silver rejected record counts
- Gold dimension counts
- Gold fact counts
- Gold summary counts
- SCD Type 2 rules
- Duplicate prevention
- Delta MERGE results
- Delta history
- Revenue consistency
- Validation report persistence

### 3. Validation Notebooks

Data quality is implemented across two notebooks.

| Notebook | Purpose |
|---|---|
| `03_silver_transformations` | Applies Silver validation rules and creates rejected records |
| `08_data_quality_validation` | Runs final project-wide validation checks |

#### Silver Validation

The Silver notebook validates incoming Bronze records and separates valid records from invalid records.

#### Final Validation

The final validation notebook checks that the full pipeline output is consistent after Bronze, Silver, Gold, MERGE, and time travel processing.

### 4. Silver Validation Strategy

Silver is the first formal data quality gate.

The Silver layer validates:

- Required fields
- Data types
- Allowed values
- Referential integrity
- Derived amount logic
- Parent-child relationships

The output of Silver is divided into:

```text
Clean records
Rejected records
```

Conceptually:

```text
Bronze records
      ↓
Validation rules
      ↓
Silver clean tables + Silver rejected records
```

### 5. Clean vs Rejected Pattern

The project does not silently drop invalid data.

Instead, invalid records are written to:

```text
silver_rejected_records
```

This design keeps data quality issues visible and traceable.

#### Clean Tables

Clean records are written to:

```text
silver_customers_clean
silver_products_clean
silver_orders_clean
silver_order_items_clean
```

#### Rejected Records

Invalid records are written to:

```text
silver_rejected_records
```

This table stores the invalid record payload and the reason for rejection.

### 6. Rejected Records Table

The rejected records table has a normalized structure.

Table:

```text
silver_rejected_records
```

#### Rejected Record Columns

| Column | Purpose |
|---|---|
| `entity_name` | Entity that failed validation |
| `reject_reason` | First detected validation failure |
| `ingestion_batch_id` | Source batch |
| `source_file_path` | Source file path |
| `raw_record_hash` | Raw record fingerprint from Bronze |
| `rejected_at` | Rejection timestamp |
| `record_json` | Rejected row payload as JSON |

This structure allows records from multiple entities to be reviewed in one place.

### 7. First-Rejection Rule

The MVP records the first applicable rejection reason for each invalid record.

This keeps validation logic readable and simple.

Example:

```text
An order item may have an invalid parent order and invalid quantity.
The current MVP records the first detected reason.
```

A future enhancement could capture multiple rejection reasons per record.

### 8. Customer Quality Rules

Silver customer validation applies rules to:

```text
bronze_customers
```

Output clean table:

```text
silver_customers_clean
```

#### Customer Rules

| Rule | Rejection reason |
|---|---|
| `customer_id` is required | `customer_id_is_required` |
| `customer_name` is required | `customer_name_is_required` |
| `effective_update_ts` must be valid | `effective_update_ts_is_invalid` |

Expected clean records:

```text
9
```

The source customer data is intentionally valid in this MVP, so no customer records are expected to be rejected.

### 9. Product Quality Rules

Silver product validation applies rules to:

```text
bronze_products
```

Output clean table:

```text
silver_products_clean
```

#### Product Rules

| Rule | Rejection reason |
|---|---|
| `product_id` is required | `product_id_is_required` |
| `product_name` is required | `product_name_is_required` |
| `unit_price` must be valid | `unit_price_is_invalid` |
| `unit_price` must be greater than 0 | `unit_price_must_be_positive` |

Expected clean records:

```text
4
```

The source product data is intentionally valid in this MVP, so no product records are expected to be rejected.

### 10. Order Quality Rules

Silver order validation applies rules to:

```text
bronze_orders
```

Output clean table:

```text
silver_orders_clean
```

#### Order Rules

| Rule | Rejection reason |
|---|---|
| `order_id` is required | `order_id_is_required` |
| `customer_id` is required | `customer_id_is_required` |
| `customer_id` must exist in valid customers | `customer_id_not_found` |
| `order_ts` must be valid | `order_ts_is_invalid` |
| `order_status` must be allowed | `order_status_is_not_allowed` |
| `currency_code` must be allowed | `currency_code_is_not_allowed` |

#### Allowed Order Status Values

```text
CREATED
PAID
COMPLETED
CANCELLED
REFUNDED
```

#### Allowed Currency Values

```text
MXN
USD
```

#### Expected Rejected Orders

| Order | Reason |
|---|---|
| `ORD-1006` | `customer_id_not_found` |
| `ORD-1008` | `currency_code_is_not_allowed` |
| `ORD-1009` | `order_status_is_not_allowed` |

Expected clean records:

```text
7
```

Expected rejected order records:

```text
3
```

### 11. Order Item Quality Rules

Silver order item validation applies rules to:

```text
bronze_order_items
```

Output clean table:

```text
silver_order_items_clean
```

#### Order Item Rules

| Rule | Rejection reason |
|---|---|
| `order_id` is required | `order_id_is_required` |
| `order_id` must exist in valid orders | `order_id_not_found_or_parent_order_rejected` |
| `product_id` is required | `product_id_is_required` |
| `product_id` must exist in valid products | `product_id_not_found` |
| `quantity` must be valid | `quantity_is_invalid` |
| `quantity` must be greater than 0 | `quantity_must_be_positive` |
| `unit_price` must be valid | `unit_price_is_invalid` |
| `unit_price` must be greater than 0 | `unit_price_must_be_positive` |
| `discount_amount` must be valid | `discount_amount_is_invalid` |
| `discount_amount` cannot be negative | `discount_amount_cannot_be_negative` |
| `line_total` cannot be negative | `line_total_cannot_be_negative` |

#### Parent Order Validation

Order items must reference valid Silver orders.

If a parent order is rejected, related order items are also rejected.

Expected rejected order item records:

```text
3
```

Expected clean records:

```text
8
```

### 12. Expected Rejected Records

The project expects six rejected records in Silver.

| Entity | Rejection reason | Expected count |
|---|---|---:|
| `orders` | `currency_code_is_not_allowed` | 1 |
| `orders` | `customer_id_not_found` | 1 |
| `orders` | `order_status_is_not_allowed` | 1 |
| `order_items` | `order_id_not_found_or_parent_order_rejected` | 3 |

Total expected rejected records:

```text
6
```

### 13. Final Validation Notebook

Notebook:

```text
08_data_quality_validation
```

This notebook validates the complete Lakehouse output after all previous notebooks have executed.

It creates a validation report and writes it to the `metadata` container.

Output path pattern:

```text
metadata/delta_lakehouse/validation_reports/run_id=<validation_run_id>
```

### 14. Validation Run ID

The validation notebook creates a unique validation run ID.

Conceptually:

```text
validation_run_id = YYYYMMDD_HHMMSS
```

This allows each validation execution to be written to a separate path.

Example:

```text
metadata/delta_lakehouse/validation_reports/run_id=20260525_212928
```

### 15. Validation Report Structure

The final validation report contains one row per validation check.

#### Validation Report Columns

| Column | Purpose |
|---|---|
| `validation_run_id` | Identifies the validation execution |
| `validation_name` | Name of the validation check |
| `layer_name` | Layer being validated |
| `expected_value` | Expected result |
| `actual_value` | Actual result |
| `status` | `PASS` or `FAIL` |
| `details` | Explanation of the validation |
| `validated_at` | Timestamp when validation was recorded |

This structure makes validation results easy to review and document.

### 16. Row Count Validations

The final validation notebook checks expected row counts across Bronze, Silver, and Gold.

#### Expected Bronze Counts

| Table | Expected row count |
|---|---:|
| `bronze_customers` | 9 |
| `bronze_products` | 4 |
| `bronze_orders` | 10 |
| `bronze_order_items` | 11 |

#### Expected Silver Counts

| Table | Expected row count |
|---|---:|
| `silver_customers_clean` | 9 |
| `silver_products_clean` | 4 |
| `silver_orders_clean` | 7 |
| `silver_order_items_clean` | 8 |
| `silver_rejected_records` | 6 |

#### Expected Gold Counts

| Table | Expected row count |
|---|---:|
| `gold_dim_customer_scd2` | 9 |
| `gold_dim_product` | 5 |
| `gold_fact_orders` | 6 |
| `gold_daily_sales_summary` | 4 |
| `gold_customer_sales_summary` | 6 |

### 17. Rejected Record Validations

The final validation notebook validates that expected rejected records were produced.

It checks rejected records grouped by:

```text
entity_name
reject_reason
```

Expected results:

| Entity | Rejection reason | Expected count |
|---|---|---:|
| `orders` | `currency_code_is_not_allowed` | 1 |
| `orders` | `customer_id_not_found` | 1 |
| `orders` | `order_status_is_not_allowed` | 1 |
| `order_items` | `order_id_not_found_or_parent_order_rejected` | 3 |

These validations confirm that Silver quality rules behaved as expected.

### 18. Gold Fact Duplicate Validation

The validation notebook checks that the Gold fact table contains one row per order.

Target table:

```text
gold_fact_orders
```

Validation:

```text
No duplicate order_id values
```

Expected duplicate count:

```text
0
```

Validation name:

```text
gold_fact_orders_no_duplicate_order_id
```

This confirms that latest-order-state logic prevented duplicate fact rows.

### 19. SCD2 Current Record Validation

The validation notebook checks that each customer has exactly one current SCD2 record.

Target table:

```text
gold_dim_customer_scd2
```

Expected condition:

```text
current_versions = 1 for each customer_id
```

Validation name:

```text
gold_dim_customer_scd2_one_current_record_per_customer
```

This confirms that the customer dimension has a valid current-state view.

### 20. SCD2 Effective Date Validation

The validation notebook checks that closed historical versions have valid date ranges.

Expected condition:

```text
effective_end_ts > effective_start_ts
```

Validation name:

```text
gold_dim_customer_scd2_valid_effective_dates
```

Expected invalid count:

```text
0
```

This confirms that historical SCD2 records have coherent effective date windows.

### 21. Product Duplicate Validation

The validation notebook checks that the product dimension has no duplicate product business keys after MERGE.

Target table:

```text
gold_dim_product
```

Validation:

```text
No duplicate product_id values
```

Expected duplicate count:

```text
0
```

Validation name:

```text
gold_dim_product_no_duplicate_product_id
```

This confirms that the MERGE did not create duplicate products.

### 22. Delta MERGE Result Validations

The validation notebook checks the product update and insert caused by Delta MERGE.

#### PROD-002 Update Validation

Expected result:

```text
PROD-002 unit_price = 38.00
```

Validation name:

```text
delta_merge_updated_existing_product
```

#### PROD-005 Insert Validation

Expected result:

```text
PROD-005 exists exactly once
```

Validation name:

```text
delta_merge_inserted_new_product
```

These checks confirm both sides of the upsert pattern.

### 23. Fact-to-Dimension Validation

The validation notebook checks that every fact order resolves to a customer surrogate key.

Target table:

```text
gold_fact_orders
```

Expected condition:

```text
customer_sk is not null for every fact order
```

Validation name:

```text
gold_fact_orders_customer_sk_not_null
```

Expected missing count:

```text
0
```

This confirms that the fact table successfully joined to the customer SCD2 dimension.

### 24. Revenue Consistency Validation

The validation notebook checks that the daily summary revenue matches the fact table revenue.

Target tables:

```text
gold_fact_orders
gold_daily_sales_summary
```

Expected condition:

```text
sum(gold_fact_orders.recognized_revenue_amount)
=
sum(gold_daily_sales_summary.recognized_revenue_amount)
```

Validation name:

```text
gold_daily_summary_revenue_matches_fact_orders
```

Expected result:

```text
1094.00 = 1094.00
```

This confirms that analytical summaries are consistent with the fact table.

### 25. Delta History MERGE Validation

The validation notebook checks that the product dimension Delta history contains a MERGE operation.

Target table:

```text
gold_dim_product
```

Expected condition:

```text
MERGE operation count >= 1
```

Validation name:

```text
gold_dim_product_delta_history_contains_merge
```

This confirms that the product dimension was mutated using Delta MERGE, not only overwritten.

### 26. Validation Status Summary

The final validation notebook groups validation results by status.

Expected output:

| Status | Expected result |
|---|---:|
| `PASS` | All validations |
| `FAIL` | 0 validations |

If any validation returns `FAIL`, the project should not be considered technically complete until the issue is reviewed.

### 27. Validation Persistence

The final validation report is persisted as a Delta output.

Path pattern:

```text
metadata/delta_lakehouse/validation_reports/run_id=<validation_run_id>
```

This creates a durable validation artifact.

The persisted validation report supports:

- Review
- Evidence capture
- Debugging
- Reproducibility
- Portfolio documentation

### 28. Validation Evidence

Recommended validation evidence is defined in:

```text
docs/evidence_index.md
```

Relevant evidence folder:

```text
evidence/09_data_quality_validation/
```

Recommended evidence files:

| Evidence file | Purpose |
|---|---|
| `01_validation_report.png` | Shows validation names, expected values, actual values, and status |
| `02_validation_status_summary.png` | Shows PASS / FAIL summary |
| `03_validation_report_saved_to_metadata.png` | Shows validation report write confirmation |
| `04_metadata_validation_report_folder.png` | Shows validation report folder in metadata container |

### 29. What PASS Means

A `PASS` result means that a validation matched its expected condition.

Examples:

```text
Expected count = actual count
Duplicate count = 0
Rejected record count matches expected value
MERGE history contains at least one MERGE operation
Revenue total matches between fact and summary
```

A `PASS` result does not mean the pipeline is production-ready.

It means the MVP produced outputs that match the expected rules for this project.

### 30. What FAIL Means

A `FAIL` result means that a validation did not match its expected condition.

Examples:

```text
Actual row count differs from expected row count
A duplicate order exists in the fact table
A customer has more than one current SCD2 record
A product duplicate was created after MERGE
Revenue summary does not match fact revenue
```

If a validation fails, the expected response is:

```text
1. Review the failed validation.
2. Identify the affected table.
3. Trace the issue back to the notebook that created the table.
4. Correct the logic or expected value.
5. Rerun the affected notebooks.
6. Rerun the validation notebook.
```

### 31. Data Quality Design Decisions

#### Validate in Silver

Business and referential validation happen in Silver because Silver is the quality gate.

#### Preserve Invalid Records

Invalid records are written to a rejected records table rather than silently discarded.

#### Validate Again at the End

Final validation confirms that the full pipeline output remains consistent after Gold, MERGE, and time travel steps.

#### Persist Validation Results

Validation reports are written to the metadata container to create durable project evidence.

#### Keep Validation Logic Simple

The MVP uses explicit validation checks instead of a full data quality framework.

This makes the logic easier to understand and defend.

### 32. Known Limitations

This data quality implementation is intentionally scoped for an MVP.

Known limitations:

- It captures only the first rejection reason per record.
- It does not use a dedicated data quality framework.
- It does not implement configurable validation rules.
- It does not send alerts on validation failure.
- It does not stop an orchestrated production job.
- It does not maintain historical quality trend dashboards.
- It does not implement SLA monitoring.
- It does not use Unity Catalog expectations or production governance features.

These limitations are acceptable for the current portfolio project scope.

### 33. Future Enhancements

Possible future enhancements include:

- Capturing multiple rejection reasons per record
- Externalizing validation rules into configuration tables
- Adding Great Expectations or another validation framework
- Adding automated test assertions
- Adding validation trend reporting
- Adding alerting on validation failure
- Adding production orchestration with Databricks Jobs
- Adding quality metrics dashboards
- Adding data contracts for source inputs
- Registering tables and expectations in Unity Catalog

### 34. Data Quality Summary

The data quality validation design demonstrates that the project produces trusted outputs, not just transformed files.

It validates:

- Bronze ingestion counts
- Silver clean records
- Silver rejected records
- Gold dimensions
- Gold facts
- Gold summaries
- SCD2 rules
- Duplicate prevention
- MERGE results
- Delta history
- Revenue consistency
- Persisted validation reporting

This makes the Lakehouse pipeline more reliable, explainable, and defensible as a portfolio project.