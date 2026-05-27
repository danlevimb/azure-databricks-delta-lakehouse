# Gold Layer

This document describes the Gold layer implementation for the `azure-databricks-delta-lakehouse` project.

The Gold layer creates analytics-ready Delta tables from trusted Silver data. It contains dimensional models, fact tables, analytical summaries, Delta MERGE behavior, and validation-ready outputs.

### 1. Gold Layer Objective

The objective of the Gold layer is to convert clean and validated Silver data into analytical tables that can support reporting, business analysis, and downstream consumption.

The Gold layer answers these questions:

- What are the current and historical customer attributes?
- Which customer version was valid when an order occurred?
- What is the latest valid state of each order?
- What are the recognized revenue amounts?
- What are daily sales totals?
- What are customer-level sales totals?
- Can reference data be updated incrementally through Delta MERGE?

The Gold layer represents the business-ready layer of the Lakehouse.

```text
Bronze = raw and traceable
Silver = clean, typed, and validated
Gold = modeled, analytical, and business-ready
```

### 2. Gold Position in the Lakehouse

The Gold layer sits after Silver.

```text
silver/delta_lakehouse
      ↓
gold/delta_lakehouse
```

Silver provides trusted data.

Gold converts that trusted data into analytical models.

The Gold layer is the primary serving layer for this MVP.

### 3. Gold Source Inputs

The Gold layer reads from Silver clean Delta tables.

Input tables:

```text
silver_customers_clean
silver_products_clean
silver_orders_clean
silver_order_items_clean
```

The Gold layer also reads previously created Gold dimensions when building facts and summaries.

Gold dimensions used by later notebooks:

```text
gold_dim_customer_scd2
gold_dim_product
```

### 4. Gold Outputs

The Gold layer writes analytical Delta tables to the `gold` container.

Output base path:

```text
gold/delta_lakehouse/
```

Gold tables:

```text
gold_dim_customer_scd2
gold_dim_product
gold_fact_orders
gold_daily_sales_summary
gold_customer_sales_summary
```

### 5. Gold Notebooks

The Gold layer is built across multiple notebooks.

| Notebook | Responsibility |
|---|---|
| `04_gold_dimensions` | Builds customer and product dimensions |
| `05_gold_facts_and_aggregates` | Builds fact and summary tables |
| `06_delta_merge_upsert_demo` | Demonstrates Delta MERGE on product dimension |
| `07_time_travel_validation` | Validates before/after Delta versions |
| `08_data_quality_validation` | Validates final Gold outputs |

### 6. Gold Processing Flow

The Gold flow follows this logical sequence:

```text
Silver clean customers
      ↓
gold_dim_customer_scd2

Silver clean products
      ↓
gold_dim_product

Silver clean orders + Silver clean order items + Gold dimensions
      ↓
gold_fact_orders
      ↓
gold_daily_sales_summary
gold_customer_sales_summary

gold_dim_product
      ↓
Delta MERGE / upsert
      ↓
Time travel validation
```

This flow creates both dimensional and analytical outputs.

### 7. Gold Design Philosophy

The Gold layer is designed to be business-oriented.

It avoids exposing raw source noise directly to analytical consumers.

Instead, it provides:

- Dimensions
- Facts
- Summaries
- Historical customer context
- Revenue-ready metrics
- Validated analytical outputs

Gold is not only a storage layer. It represents analytical modeling decisions.

### 8. Gold Dimension Tables

The project creates two dimension tables.

```text
gold_dim_customer_scd2
gold_dim_product
```

#### Customer Dimension

The customer dimension preserves historical changes through SCD Type 2.

It tracks changes in selected customer attributes and creates a new version when those attributes change.

#### Product Dimension

The product dimension stores product reference data.

It is later used to demonstrate Delta MERGE / upsert behavior.

### 9. Customer SCD Type 2 Dimension

Output table:

```text
gold_dim_customer_scd2
```

Source table:

```text
silver_customers_clean
```

The customer dimension implements SCD Type 2 to preserve historical customer attribute changes.

#### Why SCD Type 2 Is Used

Customer data changes over time.

Examples:

```text
Customer changes city
Customer changes segment
Customer changes email
Customer gains loyalty tier
```

A simple current-state table would overwrite previous customer values.

SCD Type 2 preserves the historical versions.

This allows analytical queries to associate facts with the customer version that was valid at the time of the transaction.

### 10. Customer SCD2 Tracked Attributes

The SCD2 logic tracks these customer attributes:

```text
customer_name
email
city
state
customer_segment
loyalty_tier
```

When one of these attributes changes, the customer receives a new historical version.

### 11. Customer SCD2 Columns

The customer SCD2 dimension includes:

| Column | Purpose |
|---|---|
| `customer_sk` | Surrogate key for the customer version |
| `customer_id` | Business key |
| `customer_name` | Customer name |
| `email` | Customer email |
| `city` | Customer city |
| `state` | Customer state |
| `customer_segment` | Customer segment |
| `loyalty_tier` | Optional loyalty tier |
| `effective_start_ts` | Version start timestamp |
| `effective_end_ts` | Version end timestamp |
| `is_current` | Indicates current active version |
| `record_hash` | Hash of tracked business attributes |
| `ingestion_batch_id` | Source batch traceability |
| `source_file_path` | Source file traceability |
| `raw_record_hash` | Raw source fingerprint |
| `silver_processed_ts` | Silver processing timestamp |
| `gold_processed_ts` | Gold processing timestamp |

### 12. Customer SCD2 Change Detection

The project calculates a `record_hash` based on tracked customer attributes.

Conceptually:

```text
record_hash = sha2(customer_name, email, city, state, customer_segment, loyalty_tier)
```

The notebook compares each customer record against the previous version.

```text
Same hash      → no new version
Different hash → create new version
```

This avoids creating unnecessary versions when the tracked attributes did not change.

### 13. Customer SCD2 Versioning Logic

The customer history is created by ordering records by:

```text
customer_id
effective_start_ts
```

Then the next version start timestamp becomes the current version end timestamp.

Conceptually:

```text
effective_end_ts = next effective_start_ts
```

The current version has:

```text
effective_end_ts = null
is_current = true
```

Historical versions have:

```text
effective_end_ts populated
is_current = false
```

### 14. Customer SCD2 Example

Example scenario:

```text
CUST-002 changed from Monterrey / SMB to San Pedro Garza Garcia / Enterprise.
```

Conceptual result:

```text
customer_id | city                    | segment    | effective_start_ts | effective_end_ts | is_current
CUST-002    | Monterrey               | SMB        | 2026-05-01         | 2026-05-02       | false
CUST-002    | San Pedro Garza Garcia  | Enterprise | 2026-05-02         | null             | true
```

This demonstrates historical customer versioning.

### 15. Product Dimension

Output table:

```text
gold_dim_product
```

Source table:

```text
silver_products_clean
```

The product dimension stores product reference data.

#### Product Dimension Columns

| Column | Purpose |
|---|---|
| `product_sk` | Product surrogate key |
| `product_id` | Product business key |
| `product_name` | Product name |
| `category` | Product category |
| `unit_price` | Product unit price |
| `is_active` | Product active flag |
| `record_hash` | Hash of product attributes |
| `ingestion_batch_id` | Source batch traceability |
| `source_file_path` | Source file traceability |
| `raw_record_hash` | Raw source fingerprint |
| `silver_processed_ts` | Silver processing timestamp |
| `gold_processed_ts` | Gold processing timestamp |

#### Product Dimension Scope

The product dimension is implemented as a current-state dimension for this MVP.

It does not implement SCD Type 2.

The product dimension is later updated using Delta MERGE to demonstrate upsert behavior.

### 16. Gold Fact Table

Output table:

```text
gold_fact_orders
```

The fact table stores one row per valid order.

It is built from:

```text
silver_orders_clean
silver_order_items_clean
gold_dim_customer_scd2
```

### 17. Latest Order State Handling

Some orders appear in more than one batch.

Example:

```text
ORD-1002 appears in batch_001 as PAID.
ORD-1002 appears in batch_002 as COMPLETED.
```

The Gold fact process keeps the latest valid state for each order.

This prevents the same business order from being counted multiple times.

Conceptually:

```text
One valid final row per order_id
```

The notebook uses batch sequence and ingestion timestamp to select the latest order record.

### 18. Latest Order Item Handling

Order items may also appear across batches.

The Gold fact process selects the latest valid order item state by:

```text
order_id
product_id
```

This helps prevent duplicate line-level contributions when order item records are updated across batches.

### 19. Order Item Aggregation

Before building the fact table, order items are aggregated at the order level.

Metrics include:

| Metric | Description |
|---|---|
| `order_line_count` | Number of valid order item lines |
| `total_quantity` | Total item quantity |
| `gross_order_amount` | Quantity multiplied by unit price |
| `total_discount_amount` | Total discount amount |
| `net_order_amount` | Gross amount minus discounts |

This produces one order-level metric row per `order_id`.

### 20. Fact Table Columns

The `gold_fact_orders` table includes:

| Column | Purpose |
|---|---|
| `order_id` | Order business key |
| `customer_sk` | Historical customer surrogate key |
| `customer_id` | Customer business key |
| `order_status` | Latest valid order status |
| `order_ts` | Order timestamp |
| `order_date` | Order date |
| `currency_code` | Currency code |
| `payment_method` | Payment method |
| `source_system` | Source system |
| `order_line_count` | Number of order item lines |
| `total_quantity` | Total item quantity |
| `gross_order_amount` | Gross order amount |
| `total_discount_amount` | Total discount amount |
| `net_order_amount` | Net order amount |
| `is_revenue_order` | Indicates if order contributes to recognized revenue |
| `recognized_revenue_amount` | Revenue amount recognized for reporting |
| `ingestion_batch_id` | Source batch traceability |
| `source_file_path` | Source file traceability |
| `raw_record_hash` | Raw source fingerprint |
| `silver_processed_ts` | Silver processing timestamp |
| `gold_processed_ts` | Gold processing timestamp |

### 21. SCD2 As-Of Join

The fact table uses an as-of join to connect orders to the correct historical customer version.

The join logic is based on the order timestamp.

Conceptually:

```text
order_ts >= customer.effective_start_ts
and order_ts < customer.effective_end_ts
```

For current customer versions:

```text
effective_end_ts is null
```

This ensures historically accurate reporting.

#### Why This Matters

If a customer changed segment after placing an order, historical orders should remain tied to the segment that was valid when the order happened.

This prevents historical facts from being incorrectly reported using only the latest customer attributes.

### 22. Revenue Recognition Logic

The project defines a revenue order as an order with one of these statuses:

```text
PAID
COMPLETED
```

Orders with these statuses contribute to `recognized_revenue_amount`.

Other statuses, such as:

```text
CREATED
CANCELLED
```

do not contribute to recognized revenue.

#### Revenue Logic

Conceptually:

```text
if order_status in (PAID, COMPLETED):
    recognized_revenue_amount = net_order_amount
else:
    recognized_revenue_amount = 0
```

This creates a simple but realistic analytical revenue rule.

### 23. Fact Table Partitioning

The `gold_fact_orders` table is partitioned by:

```text
order_date
```

This is a practical partitioning choice because analytical queries commonly filter or aggregate by date.

#### Partitioning Rationale

Partitioning by order date supports:

- Date-based filtering
- Daily analytical summaries
- Incremental analytical patterns
- Cleaner folder organization

The project avoids over-partitioning because the dataset is intentionally small.

### 24. Daily Sales Summary

Output table:

```text
gold_daily_sales_summary
```

The daily sales summary aggregates `gold_fact_orders` by:

```text
order_date
currency_code
```

#### Daily Summary Metrics

The table includes:

| Metric | Description |
|---|---|
| `total_orders` | Distinct order count |
| `created_orders` | Orders in created status |
| `paid_orders` | Orders in paid status |
| `completed_orders` | Orders in completed status |
| `cancelled_orders` | Cancelled orders |
| `revenue_orders` | Orders contributing to revenue |
| `total_quantity` | Total quantity sold |
| `gross_sales_amount` | Gross sales amount |
| `total_discount_amount` | Total discount amount |
| `net_order_amount` | Net order amount |
| `recognized_revenue_amount` | Revenue recognized for reporting |

The table is also partitioned by:

```text
order_date
```

### 25. Customer Sales Summary

Output table:

```text
gold_customer_sales_summary
```

The customer sales summary aggregates sales by customer and currency.

It joins fact orders to the customer SCD2 dimension through `customer_sk`.

#### Customer Summary Grouping

The table groups by:

```text
customer_sk
customer_id
customer_name
city
state
customer_segment
loyalty_tier
currency_code
```

#### Customer Summary Metrics

The table includes:

| Metric | Description |
|---|---|
| `total_orders` | Distinct order count |
| `revenue_orders` | Revenue-generating order count |
| `total_quantity` | Total quantity |
| `gross_sales_amount` | Gross sales amount |
| `total_discount_amount` | Discount amount |
| `net_order_amount` | Net amount |
| `recognized_revenue_amount` | Recognized revenue |
| `first_order_date` | First order date for the customer grouping |
| `last_order_date` | Last order date for the customer grouping |

### 26. Delta MERGE on Product Dimension

Notebook:

```text
06_delta_merge_upsert_demo
```

The project demonstrates Delta MERGE using:

```text
gold_dim_product
```

The MERGE source contains:

```text
PROD-002 → existing product with updated price
PROD-005 → new product
```

Expected behavior:

```text
PROD-002 exists     → update
PROD-005 not found  → insert
```

### 27. MERGE Result

After MERGE, the product dimension contains five distinct products.

Expected validation:

| Metric | Expected value |
|---|---:|
| Total products | 5 |
| Distinct products | 5 |

Product-level results:

| Product | Expected result |
|---|---|
| `PROD-002` | Updated price from `35.00` to `38.00` |
| `PROD-005` | Inserted as a new product |

This demonstrates a common incremental upsert pattern in Delta Lake.

### 28. Delta Time Travel

Notebook:

```text
07_time_travel_validation
```

The project validates Delta time travel using the product dimension.

The notebook reads product table versions before and after MERGE.

Expected comparison:

```text
Before MERGE → 4 products
After MERGE  → 5 products
```

Affected product comparison:

```text
PROD-002 before MERGE → 35.00
PROD-002 after MERGE  → 38.00
PROD-005 before MERGE → not present
PROD-005 after MERGE  → present
```

This shows that Delta table versions can be queried for validation, debugging, and audit-style comparisons.

### 29. Gold Validation

The Gold layer is validated in the final validation notebook.

Notebook:

```text
08_data_quality_validation
```

Gold validations include:

- Gold table row counts
- No duplicate `order_id` values in `gold_fact_orders`
- One current SCD2 record per customer
- Valid SCD2 effective dates
- No duplicate `product_id` values after MERGE
- `PROD-002` update validation
- `PROD-005` insert validation
- Fact orders resolve customer surrogate keys
- Daily revenue summary matches fact revenue
- Delta history contains a MERGE operation

### 30. Expected Gold Counts

Expected final Gold counts:

| Gold table | Expected row count |
|---|---:|
| `gold_dim_customer_scd2` | 9 |
| `gold_dim_product` | 5 |
| `gold_fact_orders` | 6 |
| `gold_daily_sales_summary` | 4 |
| `gold_customer_sales_summary` | 6 |

### 31. Gold Evidence

Recommended evidence for this layer is defined in:

```text
docs/evidence_index.md
```

Recommended Gold evidence includes:

| Evidence file | Purpose |
|---|---|
| `01_gold_customer_scd2_versions.png` | Shows customer history versions |
| `02_gold_customer_current_validation.png` | Shows one current version per customer |
| `03_gold_product_dimension.png` | Shows product dimension |
| `04_gold_dimensions_summary.png` | Shows dimension row counts |
| `01_gold_fact_orders.png` | Shows fact table preview |
| `02_fact_orders_scd2_join.png` | Shows fact orders joined to customer SCD2 |
| `03_gold_daily_sales_summary.png` | Shows daily summary |
| `04_gold_customer_sales_summary.png` | Shows customer summary |
| `05_gold_facts_summary.png` | Shows Gold fact and summary row counts |

### 32. Gold Design Decisions

#### Use SCD Type 2 for Customers

Customers are modeled as SCD Type 2 because customer attributes can change over time and historical reporting should preserve those changes.

#### Use Current-State Product Dimension

Products are modeled as a current-state dimension for MVP scope.

This keeps product modeling simple while still supporting Delta MERGE demonstration.

#### Use Latest Valid Order State

The fact table uses the latest valid state of each order to prevent double-counting order updates.

#### Use As-Of Customer Join

Orders join to the customer version that was valid at the order timestamp.

This supports historically accurate reporting.

#### Use Revenue Recognition Flag

The fact table separates order amount from recognized revenue.

Only `PAID` and `COMPLETED` orders contribute to recognized revenue.

#### Use Delta MERGE on Product Dimension

The product dimension demonstrates incremental mutation through Delta MERGE.

#### Use Time Travel for Validation

Delta time travel is used to validate before/after states around the MERGE operation.

### 33. Gold Layer Limitations

The Gold implementation is intentionally scoped for an MVP.

Known limitations:

- Product dimension is not implemented as SCD Type 2.
- Fact table is rebuilt during development reruns.
- Revenue recognition logic is simplified.
- Currency conversion is not implemented.
- Tax, shipping, and payment settlement logic are not modeled.
- Gold tables are path-based and not registered in Unity Catalog.
- BI semantic model is not included.
- No production orchestration is implemented through Databricks Jobs.
- No automated deployment pipeline is included.

These limitations are acceptable for the current portfolio project scope.

### 34. Gold Layer Summary

The Gold layer converts validated Silver data into analytical Delta tables.

It demonstrates:

- Customer SCD Type 2
- Product dimension modeling
- Latest order state handling
- Order-level fact table construction
- Historical customer as-of joins
- Revenue recognition logic
- Daily sales aggregation
- Customer sales aggregation
- Delta MERGE / upsert
- Delta time travel validation
- Final Gold-level validation checks

The Gold layer provides the business-ready analytical outputs of the Lakehouse MVP.