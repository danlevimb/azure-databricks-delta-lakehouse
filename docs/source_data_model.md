# Source Data Model

This document describes the source data model used by the `azure-databricks-delta-lakehouse` project.

The project uses a controlled retail order dataset designed to demonstrate Lakehouse processing patterns in Azure Databricks, including Bronze ingestion, Silver validation, Gold dimensional modeling, SCD Type 2, Delta MERGE, and time travel validation.

### 1. Source Data Objective

The source data model was designed to be small, readable, and technically useful.

The objective is not to simulate large production volume.

The objective is to provide enough controlled variation to demonstrate realistic Data Engineering patterns:

- Multi-batch ingestion
- Customer changes over time
- Order status updates
- New customers and orders
- Invalid references
- Invalid business values
- Rejected records
- Schema evolution
- SCD Type 2 customer history
- Delta MERGE / upsert
- Time travel validation

### 2. Business Domain

The project uses a retail order processing domain.

The main business process is:

```text
Customers place orders.
Orders contain order items.
Order items reference products.
Orders have statuses and currencies.
Customer attributes may change over time.
```

This domain was selected because it is simple to understand while still supporting common data engineering scenarios such as validation, referential integrity, historical dimensions, and fact table construction.

### 3. Source Entities

The source model includes four core entities.

| Entity | Description |
|---|---|
| `customers` | Customer master data |
| `products` | Product reference data |
| `orders` | Order header data |
| `order_items` | Order line-level data |

These entities are generated as CSV files into the ADLS Gen2 `landing` container.

### 4. Source File Structure

The generated source files are stored under:

```text
landing/source_data/
```

The source data is organized by batch.

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

The batches represent controlled source arrivals.

Each batch introduces specific scenarios required by the Lakehouse pipeline.

### 5. Batch Design

The project uses three source batches.

| Batch | Purpose |
|---|---|
| `batch_001` | Initial baseline dataset |
| `batch_002` | Incremental updates, new data, and invalid references |
| `batch_003_schema_evolution` | Schema evolution and additional validation scenarios |

#### Batch 001

`batch_001` represents the initial source load.

It includes:

| Entity | Rows |
|---|---:|
| `customers` | 4 |
| `products` | 4 |
| `orders` | 4 |
| `order_items` | 5 |

This batch establishes the initial dataset used by Bronze, Silver, and Gold.

#### Batch 002

`batch_002` introduces incremental changes.

It includes:

| Entity | Rows |
|---|---:|
| `customers` | 2 |
| `orders` | 3 |
| `order_items` | 3 |

This batch introduces:

- Updated customer attributes for `CUST-002`
- New customer `CUST-005`
- Updated order status for `ORD-1002`
- New order `ORD-1005`
- Invalid order `ORD-1006` with unknown customer `CUST-999`
- Invalid order item connected to rejected order `ORD-1006`

#### Batch 003

`batch_003_schema_evolution` introduces schema evolution and additional validation scenarios.

It includes:

| Entity | Rows |
|---|---:|
| `customers` | 3 |
| `orders` | 3 |
| `order_items` | 3 |

This batch introduces:

- New customer attribute `loyalty_tier`
- Updated customer email for `CUST-001`
- Loyalty tier values for selected customers
- New customer `CUST-006`
- Unsupported currency value `EUR`
- Invalid order status `INVALID_STATUS`

### 6. Customers Source Model

Source file:

```text
customers.csv
```

The `customers` source contains customer master data.

#### Columns

| Column | Description | Example |
|---|---|---|
| `customer_id` | Customer business key | `CUST-001` |
| `customer_name` | Customer name | `Northwind Cafe` |
| `email` | Customer email | `contact@northwind.example` |
| `city` | Customer city | `Saltillo` |
| `state` | Customer state | `Coahuila` |
| `customer_segment` | Customer segment | `SMB` |
| `effective_update_ts` | Source effective update timestamp | `2026-05-01 08:00:00` |
| `loyalty_tier` | Optional loyalty tier introduced in batch 003 | `Gold` |

#### Schema Evolution

The `loyalty_tier` column is introduced only in `batch_003_schema_evolution`.

This column is intentionally absent from earlier batches.

The purpose is to demonstrate controlled schema evolution.

```text
batch_001 → no loyalty_tier
batch_002 → no loyalty_tier
batch_003_schema_evolution → loyalty_tier present
```

Bronze preserves this new column.

Silver handles the optional column explicitly.

Gold includes it as part of customer history tracking.

#### Customer Change Scenarios

The customer source includes changes over time.

| Customer | Scenario |
|---|---|
| `CUST-001` | Email changes and loyalty tier appears in batch 003 |
| `CUST-002` | City and segment change in batch 002 |
| `CUST-003` | Loyalty tier appears in batch 003 |
| `CUST-005` | New customer in batch 002 |
| `CUST-006` | New customer in batch 003 |

These scenarios support the SCD Type 2 customer dimension.

### 7. Products Source Model

Source file:

```text
products.csv
```

The `products` source contains product reference data.

#### Columns

| Column | Description | Example |
|---|---|---|
| `product_id` | Product business key | `PROD-001` |
| `product_name` | Product name | `Purified Water 20L` |
| `category` | Product category | `Water` |
| `unit_price` | Product unit price | `48.00` |
| `is_active` | Product active flag | `true` |

#### Product Records

Initial products include:

| Product ID | Product Name | Category |
|---|---|---|
| `PROD-001` | `Purified Water 20L` | `Water` |
| `PROD-002` | `Ice Bag 5kg` | `Ice` |
| `PROD-003` | `Water Bottle 600ml` | `Water` |
| `PROD-004` | `Premium Mineral Water 1L` | `Water` |

#### MERGE Scenario

The initial product source contains four products.

Later, the Delta MERGE notebook introduces:

| Product ID | Scenario |
|---|---|
| `PROD-002` | Existing product price updated |
| `PROD-005` | New product inserted |

This supports the Delta MERGE / upsert demonstration.

### 8. Orders Source Model

Source file:

```text
orders.csv
```

The `orders` source contains order header data.

#### Columns

| Column | Description | Example |
|---|---|---|
| `order_id` | Order business key | `ORD-1001` |
| `customer_id` | Customer business key | `CUST-001` |
| `order_status` | Order status | `PAID` |
| `order_ts` | Order timestamp | `2026-05-01 09:00:00` |
| `currency_code` | Transaction currency | `MXN` |
| `payment_method` | Payment method | `CARD` |
| `source_system` | Source system name | `pos` |

#### Allowed Order Status Values

The Silver layer allows the following order statuses:

```text
CREATED
PAID
COMPLETED
CANCELLED
REFUNDED
```

Any other status is rejected.

#### Allowed Currency Values

The Silver layer allows the following currency codes:

```text
MXN
USD
```

The value `EUR` is intentionally rejected in this MVP to demonstrate currency validation.

#### Order Update Scenario

`ORD-1002` appears in more than one batch.

```text
batch_001 → ORD-1002 status = PAID
batch_002 → ORD-1002 status = COMPLETED
```

This demonstrates latest-state handling in the Gold fact table.

The Gold fact process keeps the latest valid order state to avoid double-counting the same order.

#### Invalid Order Scenarios

The source data includes intentionally invalid order scenarios.

| Order | Invalid condition | Expected Silver result |
|---|---|---|
| `ORD-1006` | Customer `CUST-999` does not exist | Rejected |
| `ORD-1008` | Currency `EUR` is not allowed | Rejected |
| `ORD-1009` | Status `INVALID_STATUS` is not allowed | Rejected |

These invalid records are preserved in Bronze and rejected in Silver.

### 9. Order Items Source Model

Source file:

```text
order_items.csv
```

The `order_items` source contains order line-level data.

#### Columns

| Column | Description | Example |
|---|---|---|
| `order_id` | Order business key | `ORD-1001` |
| `product_id` | Product business key | `PROD-001` |
| `quantity` | Quantity ordered | `3` |
| `unit_price` | Unit price used in the order line | `48.00` |
| `discount_amount` | Discount applied to the line | `0.00` |

#### Derived Field

The Silver layer calculates:

```text
line_total = quantity * unit_price - discount_amount
```

#### Order Item Validation

The Silver layer validates that:

- `order_id` is present
- `order_id` exists in valid Silver orders
- `product_id` is present
- `product_id` exists in valid Silver products
- `quantity` is positive
- `unit_price` is positive
- `discount_amount` is not negative
- `line_total` is not negative

#### Invalid Order Item Scenarios

The source data includes order items connected to rejected parent orders.

| Order ID | Reason parent order is rejected |
|---|---|
| `ORD-1006` | Customer does not exist |
| `ORD-1008` | Currency is not allowed |
| `ORD-1009` | Order status is not allowed |

These order items are rejected in Silver because their parent orders are not valid.

### 10. Source Relationships

The source model has the following logical relationships:

```text
customers.customer_id → orders.customer_id
orders.order_id → order_items.order_id
products.product_id → order_items.product_id
```

#### Relationship Purpose

These relationships support:

- Referential validation in Silver
- Fact table construction in Gold
- Customer dimension joins
- Product reference validation
- Order-level metric aggregation

#### Relationship Validation

The project intentionally includes broken references to test Silver validation.

Examples:

```text
orders.customer_id = CUST-999
order_items.order_id = ORD-1006
```

These records are preserved in Bronze but rejected in Silver.

### 11. Source Row Counts

Expected source row counts across all batches are:

| Entity | Expected source rows |
|---|---:|
| `customers` | 9 |
| `products` | 4 |
| `orders` | 10 |
| `order_items` | 11 |

These counts match the expected Bronze row counts.

### 12. Data Quality Test Scenarios

The source model includes controlled data quality scenarios.

| Scenario | Source Entity | Expected Handling |
|---|---|---|
| Valid customer records | `customers` | Silver clean |
| Customer attribute changes | `customers` | Gold SCD2 |
| New customer | `customers` | Silver clean and Gold current version |
| Optional new column | `customers` | Schema evolution handling |
| Valid products | `products` | Silver clean and Gold product dimension |
| Existing product update | `gold_dim_product` MERGE source | Delta MERGE update |
| New product insert | `gold_dim_product` MERGE source | Delta MERGE insert |
| Valid order records | `orders` | Silver clean |
| Updated order state | `orders` | Latest-state handling in Gold |
| Unknown customer | `orders` | Silver rejected |
| Unsupported currency | `orders` | Silver rejected |
| Invalid order status | `orders` | Silver rejected |
| Valid order items | `order_items` | Silver clean |
| Order item with rejected parent order | `order_items` | Silver rejected |

### 13. Schema Evolution Scenario

The project includes a controlled schema evolution scenario.

The column:

```text
loyalty_tier
```

is introduced in `batch_003_schema_evolution/customers.csv`.

#### Purpose

This tests whether the pipeline can handle a new optional attribute arriving from the source.

#### Layer Handling

| Layer | Handling |
|---|---|
| Landing | Stores the file as received |
| Bronze | Preserves the new column |
| Silver | Adds the missing column for earlier batches and types it consistently |
| Gold | Includes the attribute in customer SCD2 tracking |

#### Expected Result

Earlier customer records have:

```text
loyalty_tier = null
```

Batch 003 customer records may have:

```text
Gold
Platinum
Silver
```

This allows the customer SCD2 dimension to capture new versions when loyalty tier becomes available.

### 14. Rejected Records Scenario

The project expects six rejected records in Silver.

#### Expected Rejection Summary

| Entity | Rejection reason | Expected count |
|---|---|---:|
| `orders` | `currency_code_is_not_allowed` | 1 |
| `orders` | `customer_id_not_found` | 1 |
| `orders` | `order_status_is_not_allowed` | 1 |
| `order_items` | `order_id_not_found_or_parent_order_rejected` | 3 |

#### First-Rejection Rule

The project records the first applicable rejection reason for each invalid record.

This keeps the MVP simple and readable.

A future enhancement could capture multiple rejection reasons per record.

### 15. Gold Modeling Scenarios

The source model supports the following Gold modeling patterns.

#### Customer SCD2

Customer changes support historical dimension modeling.

Examples:

| Customer | Change |
|---|---|
| `CUST-001` | Email and loyalty tier change |
| `CUST-002` | City and segment change |
| `CUST-003` | Loyalty tier change |

#### Latest Order State

Order updates support latest-state logic.

Example:

```text
ORD-1002 moves from PAID to COMPLETED.
```

The Gold fact table keeps one row per final valid order state.

#### Fact Metrics

Order and order item data support metrics such as:

- Total quantity
- Gross order amount
- Discount amount
- Net order amount
- Recognized revenue

#### Analytical Summaries

Source data supports:

- Daily sales summary
- Customer sales summary
- Revenue recognition by status
- Currency-level aggregation

### 16. Final Expected Pipeline Counts

After complete pipeline execution, expected counts are:

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

### 17. Source Data Design Summary

The source data model is intentionally compact but scenario-rich.

It supports:

- Raw ingestion
- Technical metadata capture
- Schema evolution
- Data type casting
- Business validation
- Referential validation
- Rejected records
- SCD Type 2 history
- Fact table construction
- Analytical aggregation
- Delta MERGE
- Time travel validation
- Final data quality reporting

This makes the dataset suitable for a focused Azure Databricks Delta Lakehouse portfolio project.