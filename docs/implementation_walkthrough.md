# Implementation Walkthrough

This document explains how the `azure-databricks-delta-lakehouse` project was implemented from end to end.

It focuses on what each part of the Lakehouse pipeline does, how the data flows across Bronze, Silver, and Gold layers, and which Data Engineering patterns are demonstrated.

For step-by-step execution instructions, see:

```text
docs/notebook_execution_guide.md
```

### 1. Project Overview

The project implements a portfolio-oriented Azure Databricks Delta Lakehouse using PySpark, ADLS Gen2, and Delta Lake.

The main objective is to demonstrate how raw source files can be transformed into validated, modeled, versioned, and analytics-ready Delta tables.

The pipeline follows this flow:

```text
Landing source files
        ↓
Bronze Delta tables
        ↓
Silver clean and rejected Delta tables
        ↓
Gold dimensional and fact model
        ↓
Delta MERGE, time travel, and validation report
```

The project demonstrates:

- Azure Databricks notebook execution
- PySpark DataFrame transformations
- ADLS Gen2 storage access using ABFSS paths
- Delta Lake path-based tables
- Bronze / Silver / Gold Lakehouse architecture
- Schema handling and controlled schema evolution
- Technical metadata capture
- Data quality validation
- Rejected record handling
- SCD Type 2 customer dimension
- Fact table construction
- Analytical summary tables
- Delta MERGE / upsert
- Delta time travel
- Final validation reporting

### 2. High-Level Architecture

The implementation uses a clean Azure environment dedicated to this project.

```text
Azure Databricks
    └── PySpark notebooks
            ↓
Azure Data Lake Storage Gen2
    ├── landing
    ├── bronze
    ├── silver
    ├── gold
    └── metadata
```

The storage account contains separate containers for each logical Lakehouse area.

```text
landing   → source CSV files
bronze    → raw Delta tables with technical metadata
silver    → clean Delta tables and rejected records
gold      → analytical dimensions, facts, and summaries
metadata  → final validation reports
```

The notebooks are stored and executed through a Databricks Git Folder connected to the GitHub repository.

```text
VS Code local repo ↔ GitHub ↔ Databricks Git Folder
```

This allows the project to remain version-controlled while still being executable inside Azure Databricks.

### 3. Source Data Strategy

The project uses a small controlled retail order dataset.

The goal is not to simulate massive production volume, but to create enough variation to demonstrate realistic Lakehouse patterns.

Source entities:

| Entity | Purpose |
|---|---|
| `customers` | Customer master data and SCD Type 2 history |
| `products` | Product reference data |
| `orders` | Order header data |
| `order_items` | Order line-level data |

The source files are generated into the `landing` container.

```text
landing/source_data/
  batch_001/
    customers.csv
    products.csv
    orders.csv
    order_items.csv

  batch_002/
    customers.csv
    orders.csv
    order_items.csv

  batch_003_schema_evolution/
    customers.csv
    orders.csv
    order_items.csv
```

The source batches were intentionally designed to include:

- Normal source records
- Updated customer attributes
- Updated order status
- New customers
- New orders
- Invalid customer references
- Invalid order status
- Unsupported currency code
- Invalid order item parent references
- A controlled schema evolution scenario using `loyalty_tier`

### 4. Notebook Implementation Flow

The implementation is divided into nine notebooks.

| Order | Notebook | Main Responsibility |
|---:|---|---|
| 00 | `00_environment_setup` | Validates project paths and ADLS access |
| 01 | `01_generate_sample_data` | Generates sample source files |
| 02 | `02_bronze_ingestion` | Builds Bronze Delta tables |
| 03 | `03_silver_transformations` | Cleans, types, validates, and rejects records |
| 04 | `04_gold_dimensions` | Builds dimensions including SCD Type 2 |
| 05 | `05_gold_facts_and_aggregates` | Builds fact and summary tables |
| 06 | `06_delta_merge_upsert_demo` | Demonstrates Delta MERGE / upsert |
| 07 | `07_time_travel_validation` | Demonstrates Delta time travel |
| 08 | `08_data_quality_validation` | Runs final technical validation checks |

### 5. Environment Setup

Notebook:

```text
00_environment_setup
```

This notebook defines the base ABFSS paths used by the project.

It does not store or configure storage account keys inside the notebook.

Storage access is expected to be configured at the Databricks compute level using a Databricks-backed secret scope.

The notebook validates that the Databricks compute can access the source data location.

Example logical paths:

```text
abfss://landing@<storage-account>.dfs.core.windows.net
abfss://bronze@<storage-account>.dfs.core.windows.net
abfss://silver@<storage-account>.dfs.core.windows.net
abfss://gold@<storage-account>.dfs.core.windows.net
abfss://metadata@<storage-account>.dfs.core.windows.net
```

This setup keeps secrets out of the repository and allows notebooks to remain safe for GitHub.

### 6. Sample Data Generation

Notebook:

```text
01_generate_sample_data
```

This notebook creates controlled CSV source files in the `landing` container.

It writes data for:

- Customers
- Products
- Orders
- Order items

The source data is divided into batches.

#### Batch 001

Initial baseline data.

Includes:

- Four customers
- Four products
- Four orders
- Five order item rows

#### Batch 002

Incremental update scenario.

Includes:

- Updated customer information for `CUST-002`
- New customer `CUST-005`
- Updated order `ORD-1002`
- New order `ORD-1005`
- Invalid order `ORD-1006` with customer `CUST-999`
- Invalid order item connected to rejected order `ORD-1006`

#### Batch 003

Schema evolution and additional validation scenario.

Includes:

- New optional customer attribute `loyalty_tier`
- Customer changes for `CUST-001` and `CUST-003`
- New customer `CUST-006`
- Unsupported currency example
- Invalid order status example

The purpose of this notebook is to create repeatable input data for the Lakehouse pipeline.

### 7. Bronze Layer

Notebook:

```text
02_bronze_ingestion
```

The Bronze layer reads CSV files from the `landing` container and writes them as Delta tables in the `bronze` container.

Bronze tables created:

```text
bronze_customers
bronze_products
bronze_orders
bronze_order_items
```

The Bronze layer preserves data close to the source shape while adding technical metadata.

Technical metadata includes:

| Column | Purpose |
|---|---|
| `ingestion_batch_id` | Identifies the source batch |
| `source_entity` | Identifies the business entity |
| `source_file_path` | Captures the original file path |
| `ingestion_ts` | Captures ingestion timestamp |
| `bronze_load_date` | Captures load date |
| `raw_record_hash` | Provides a technical record fingerprint |

The Bronze layer intentionally does not reject business-invalid records.

For example, records with invalid currency, invalid order status, or invalid references are still preserved in Bronze.

This reflects the purpose of Bronze:

```text
Preserve raw source data with traceability.
```

### 8. Silver Layer

Notebook:

```text
03_silver_transformations
```

The Silver layer reads Bronze Delta tables and produces clean, typed, validated Delta tables.

Silver clean tables created:

```text
silver_customers_clean
silver_products_clean
silver_orders_clean
silver_order_items_clean
```

Rejected records table created:

```text
silver_rejected_records
```

The Silver layer performs:

- Data type casting
- String cleanup
- Required field validation
- Business rule validation
- Referential validation
- Rejected record capture

#### Silver Validation Examples

Customer validation:

```text
customer_id is required
customer_name is required
effective_update_ts must be valid
```

Product validation:

```text
product_id is required
product_name is required
unit_price must be positive
```

Order validation:

```text
order_id is required
customer_id must exist
order_ts must be valid
order_status must be allowed
currency_code must be allowed
```

Order item validation:

```text
order_id must exist in valid orders
product_id must exist
quantity must be positive
unit_price must be positive
discount_amount cannot be negative
line_total cannot be negative
```

Rejected records are normalized into one table containing:

| Column | Purpose |
|---|---|
| `entity_name` | Source entity that failed validation |
| `reject_reason` | First validation reason detected |
| `ingestion_batch_id` | Source batch |
| `source_file_path` | Original source path |
| `raw_record_hash` | Source record fingerprint |
| `rejected_at` | Rejection timestamp |
| `record_json` | Rejected record payload |

The Silver layer represents the point where raw data becomes trustworthy for analytical modeling.

### 9. Gold Dimensions

Notebook:

```text
04_gold_dimensions
```

The Gold dimension notebook creates analytics-ready dimension tables.

Gold dimension tables created:

```text
gold_dim_customer_scd2
gold_dim_product
```

#### Customer SCD Type 2 Dimension

The customer dimension implements a Type 2 slowly changing dimension pattern.

It preserves customer attribute changes over time.

Tracked customer attributes include:

```text
customer_name
email
city
state
customer_segment
loyalty_tier
```

The notebook creates a business attribute hash to detect changes.

When a customer's tracked attributes change, a new historical version is created.

SCD Type 2 columns include:

| Column | Purpose |
|---|---|
| `customer_sk` | Surrogate key |
| `customer_id` | Business key |
| `effective_start_ts` | Version start timestamp |
| `effective_end_ts` | Version end timestamp |
| `is_current` | Indicates active version |
| `record_hash` | Business attribute hash |

Example SCD2 concept:

```text
CUST-002 | Monterrey              | SMB        | 2026-05-01 | 2026-05-02 | false
CUST-002 | San Pedro Garza Garcia | Enterprise | 2026-05-02 | null       | true
```

This allows facts to be associated with the customer version that was valid at the time of the transaction.

#### Product Dimension

The product dimension is implemented as a current-state dimension.

It contains:

- Product surrogate key
- Product business key
- Product name
- Category
- Unit price
- Active flag
- Record hash

The product dimension is later used for the Delta MERGE / upsert demonstration.

### 10. Gold Facts and Aggregates

Notebook:

```text
05_gold_facts_and_aggregates
```

This notebook creates analytical fact and summary tables.

Gold fact and summary tables created:

```text
gold_fact_orders
gold_daily_sales_summary
gold_customer_sales_summary
```

#### Latest Order State Handling

Some orders appear in multiple batches because they were updated.

For example:

```text
ORD-1002 appears in batch_001 and batch_002.
```

The Gold fact process keeps the latest valid state for each order.

This prevents double-counting order updates as separate sales.

#### Fact Table

The `gold_fact_orders` table is built from:

```text
silver_orders_clean
silver_order_items_clean
gold_dim_customer_scd2
```

The fact table includes:

- Order identifiers
- Customer surrogate key
- Order status
- Order date and timestamp
- Currency
- Payment method
- Source system
- Quantity and amount metrics
- Revenue recognition flag
- Recognized revenue amount

#### SCD2 As-Of Join

The fact table joins orders to the customer dimension using the order timestamp.

This means each order is linked to the customer version that was effective when the order occurred.

Conceptually:

```text
order_ts >= customer.effective_start_ts
and order_ts < customer.effective_end_ts
```

or, if the customer version is current:

```text
effective_end_ts is null
```

This is one of the most important analytical modeling patterns in the project.

#### Daily Sales Summary

The `gold_daily_sales_summary` table aggregates sales by:

```text
order_date
currency_code
```

It includes:

- Total orders
- Created orders
- Paid orders
- Completed orders
- Cancelled orders
- Revenue orders
- Total quantity
- Gross sales amount
- Discount amount
- Net order amount
- Recognized revenue amount

#### Customer Sales Summary

The `gold_customer_sales_summary` table aggregates sales by customer and currency.

It includes:

- Customer surrogate key
- Customer attributes
- Currency
- Total orders
- Revenue orders
- Quantity metrics
- Sales metrics
- First order date
- Last order date

### 11. Delta MERGE / Upsert

Notebook:

```text
06_delta_merge_upsert_demo
```

This notebook demonstrates Delta Lake MERGE behavior using the product dimension.

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

This demonstrates a standard upsert pattern.

```text
upsert = update existing records + insert new records
```

The notebook validates that:

- Product count increases from 4 to 5
- Product IDs remain distinct
- `PROD-002` is updated
- `PROD-005` is inserted
- Delta history records a `MERGE` operation

This notebook is important because it demonstrates that the project is not limited to full overwrites. It includes incremental mutation behavior through Delta Lake transactions.

### 12. Delta Time Travel

Notebook:

```text
07_time_travel_validation
```

This notebook validates Delta Lake versioned reads.

It compares the product dimension before and after the MERGE operation.

Expected behavior:

```text
Before MERGE → 4 products
After MERGE  → 5 products
```

It also validates:

```text
PROD-002 price before MERGE → 35.00
PROD-002 price after MERGE  → 38.00
PROD-005 before MERGE       → does not exist
PROD-005 after MERGE        → exists
```

This demonstrates that Delta Lake keeps table versions that can be queried for validation, auditing, and debugging.

The latest version number may change if the MERGE notebook is executed more than once.

The important concept is not a fixed version number, but the ability to compare historical and current table states.

### 13. Data Quality Validation

Notebook:

```text
08_data_quality_validation
```

This notebook runs final validation checks across Bronze, Silver, Gold, MERGE, and time travel outputs.

It validates:

- Bronze row counts
- Silver row counts
- Rejected record counts
- Gold dimension counts
- Gold fact counts
- Gold summary counts
- No duplicate order IDs in fact table
- One current SCD2 record per customer
- Valid SCD2 effective dates
- No duplicate product IDs after MERGE
- `PROD-002` updated successfully
- `PROD-005` inserted successfully
- Fact orders resolve customer surrogate keys
- Daily revenue summary matches fact table revenue
- Delta history contains a MERGE operation

The final report is written to the `metadata` container.

```text
metadata/delta_lakehouse/validation_reports/
```

Expected validation result:

```text
PASS for all validations
```

The validation report is an important project artifact because it shows that the pipeline was not only built, but also checked for consistency.

### 14. Final Expected Tables

After successful execution, the project produces the following Delta tables.

#### Bronze

```text
bronze_customers
bronze_products
bronze_orders
bronze_order_items
```

#### Silver

```text
silver_customers_clean
silver_products_clean
silver_orders_clean
silver_order_items_clean
silver_rejected_records
```

#### Gold

```text
gold_dim_customer_scd2
gold_dim_product
gold_fact_orders
gold_daily_sales_summary
gold_customer_sales_summary
```

#### Metadata

```text
validation_reports
```

### 15. Final Expected Row Counts

| Table | Expected row count |
|---|---:|
| `bronze_customers` | 9 |
| `bronze_products` | 4 |
| `bronze_orders` | 10 |
| `bronze_order_items` | 11 |
| `silver_customers_clean` | 9 |
| `silver_products_clean` | 4 |
| `silver_orders_clean` | 7 |
| `silver_order_items_clean` | 8 |
| `silver_rejected_records` | 6 |
| `gold_dim_customer_scd2` | 9 |
| `gold_dim_product` | 5 |
| `gold_fact_orders` | 6 |
| `gold_daily_sales_summary` | 4 |
| `gold_customer_sales_summary` | 6 |

### 16. Main Data Engineering Patterns Demonstrated

| Pattern | Where it is demonstrated |
|---|---|
| Layered Lakehouse architecture | Bronze, Silver, Gold notebooks |
| Raw data preservation | Bronze layer |
| Technical metadata capture | Bronze layer |
| Schema evolution handling | Customer data with `loyalty_tier` |
| Data quality rules | Silver layer |
| Rejected records | `silver_rejected_records` |
| Dimensional modeling | Gold layer |
| SCD Type 2 | `gold_dim_customer_scd2` |
| Fact table construction | `gold_fact_orders` |
| Analytical aggregation | Gold summaries |
| Delta MERGE | Product dimension upsert |
| Time travel | Product dimension version comparison |
| Validation reporting | Final validation notebook |
| Cost-aware development | Personal Compute and auto-termination |
| Secure credential handling | Databricks secret scope and compute-level Spark config |

### 17. Security and Secret Handling

The project avoids committing credentials to Git.

Storage access was configured outside the notebooks using:

```text
Databricks-backed secret scope
Databricks compute Spark configuration
```

The notebooks do not contain:

```text
Storage account keys
Connection strings
AccountKey values
Plaintext credentials
```

GitHub push protection was used as an additional safety signal during repository publication.

If a secret is ever exposed during development, the expected remediation is:

```text
1. Rotate the secret.
2. Remove it from files.
3. Remove it from commit history if necessary.
4. Push only sanitized commits.
```

### 18. Cost-Aware Implementation

The project was implemented with cost control in mind.

Cost-aware decisions include:

- Dedicated clean resource group for this project
- Small single-node Databricks Personal Compute
- Auto-termination enabled
- Manual compute termination after work sessions
- No SQL Warehouse for the MVP
- No scheduled jobs for the MVP
- No pools for the MVP
- Small controlled sample datasets
- No Event Hubs in this project
- No unnecessary always-on services

This project accepts compute startup latency as a trade-off for reduced idle cost.

### 19. Known Implementation Scope

This MVP intentionally focuses on core Databricks, PySpark, Delta Lake, and Lakehouse engineering patterns.

The following items are not part of the MVP scope:

- Unity Catalog governance design
- Production-grade access model
- CI/CD deployment pipeline
- Databricks Jobs orchestration
- Delta Live Tables / Lakeflow Declarative Pipelines
- Streaming ingestion
- External BI semantic layer
- Production monitoring and alerting
- Data catalog and lineage tooling

These can be considered future improvements.

### 20. Technical review

The strongest points to discuss are:

- Why Bronze keeps imperfect data
- Why Silver separates clean and rejected records
- How SCD Type 2 preserves customer history
- How fact orders join to the correct historical customer version
- Why MERGE is needed for incremental upserts
- How Delta time travel supports validation and debugging
- Why a final validation notebook improves trust in the pipeline
- How secrets were moved out of notebooks and into Databricks compute configuration