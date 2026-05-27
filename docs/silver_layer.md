# Silver Layer

This document describes the Silver layer implementation for the `azure-databricks-delta-lakehouse` project.

The Silver layer is responsible for transforming Bronze data into clean, typed, validated, and trusted Delta tables.

### 1. Silver Layer Objective

The objective of the Silver layer is to convert raw but traceable Bronze data into trusted datasets that can safely feed analytical Gold models.

The Silver layer answers these questions:

- Which records are valid?
- Which records are invalid?
- Why were records rejected?
- Are data types consistent?
- Do required fields exist?
- Do business values follow expected rules?
- Do relationships between entities hold?

The Silver layer acts as the quality gate of the Lakehouse.

```text
Bronze = raw and traceable
Silver = clean, typed, and validated
Gold = modeled and analytical
```

### 2. Silver Position in the Lakehouse

The Silver layer sits between Bronze and Gold.

```text
bronze/delta_lakehouse
      ↓
silver/delta_lakehouse
      ↓
gold/delta_lakehouse
```

Bronze preserves what arrived.

Silver decides what is clean enough to be modeled.

Gold uses the trusted Silver outputs to build dimensions, facts, and summaries.

### 3. Silver Source Inputs

The Silver notebook reads Bronze Delta tables.

Input tables:

```text
bronze_customers
bronze_products
bronze_orders
bronze_order_items
```

Input base path:

```text
bronze/delta_lakehouse/
```

The Silver layer depends on the Bronze layer being successfully executed first.

### 4. Silver Outputs

The Silver notebook writes clean and rejected Delta tables to the `silver` container.

Output base path:

```text
silver/delta_lakehouse/
```

Silver clean tables:

```text
silver_customers_clean
silver_products_clean
silver_orders_clean
silver_order_items_clean
```

Rejected records table:

```text
silver_rejected_records
```

### 5. Silver Notebook

Notebook:

```text
03_silver_transformations
```

Main responsibilities:

- Read Bronze Delta tables
- Cast columns to expected data types
- Clean string fields
- Add missing optional columns when needed
- Apply required-field validation
- Apply allowed-value validation
- Apply referential validation
- Calculate derived fields
- Separate valid and rejected records
- Write Silver clean Delta tables
- Write Silver rejected records Delta table
- Validate Silver row counts and Delta history

### 6. Silver Processing Flow

The Silver processing flow follows this pattern:

```text
Read Bronze table
      ↓
Standardize and cast columns
      ↓
Apply validation rules
      ↓
Split valid and rejected records
      ↓
Write clean records to Silver
      ↓
Write rejected records to Silver rejected table
```

This pattern is applied to customers, products, orders, and order items.

### 7. Silver Design Philosophy

The Silver layer does not silently drop invalid records.

Instead, it separates invalid records into `silver_rejected_records`.

This creates a clear audit trail.

```text
Valid records   → Silver clean tables
Invalid records → Silver rejected records
```

This approach improves observability and makes data quality issues easier to review.

### 8. Common Silver Transformations

The Silver layer applies common transformations across entities.

#### String Cleanup

String columns are trimmed and normalized.

Blank strings are treated as null values.

#### Type Casting

Columns are converted into expected data types.

Examples:

```text
effective_update_ts → timestamp
order_ts            → timestamp
unit_price          → decimal
quantity            → integer
is_active           → boolean
```

#### Standardization

Some values are standardized for consistency.

Examples:

```text
order_status  → uppercase
currency_code → uppercase
payment_method → uppercase
source_system → lowercase
```

#### Derived Fields

The order items process calculates:

```text
line_total = quantity * unit_price - discount_amount
```

### 9. Customers Silver Design

Output table:

```text
silver_customers_clean
```

Source:

```text
bronze_customers
```

The customer Silver process prepares customer records for Gold SCD Type 2 processing.

#### Customer Columns

Important output fields include:

| Column | Purpose |
|---|---|
| `customer_id` | Customer business key |
| `customer_name` | Customer name |
| `email` | Customer email |
| `city` | Customer city |
| `state` | Customer state |
| `customer_segment` | Customer segment |
| `loyalty_tier` | Optional loyalty tier |
| `effective_update_ts` | Source effective timestamp |
| `silver_processed_ts` | Silver processing timestamp |
| Bronze metadata columns | Traceability |

#### Customer Validation Rules

Customer validation rules:

```text
customer_id is required
customer_name is required
effective_update_ts must be valid
```

#### Schema Evolution Handling

The `loyalty_tier` column is introduced only in batch 003.

The Silver layer explicitly handles this optional column.

If the column is missing from earlier records, it is added as null.

This ensures that the Silver customer table has a consistent schema across all batches.

Expected row count:

```text
9
```

### 10. Products Silver Design

Output table:

```text
silver_products_clean
```

Source:

```text
bronze_products
```

The product Silver process prepares product reference data for the Gold product dimension.

#### Product Columns

Important output fields include:

| Column | Purpose |
|---|---|
| `product_id` | Product business key |
| `product_name` | Product name |
| `category` | Product category |
| `unit_price` | Product unit price |
| `is_active` | Product active flag |
| `silver_processed_ts` | Silver processing timestamp |
| Bronze metadata columns | Traceability |

#### Product Validation Rules

Product validation rules:

```text
product_id is required
product_name is required
unit_price must be valid
unit_price must be greater than 0
```

Expected row count:

```text
4
```

### 11. Orders Silver Design

Output table:

```text
silver_orders_clean
```

Source:

```text
bronze_orders
```

The order Silver process validates order headers and ensures that each order references a valid customer.

#### Order Columns

Important output fields include:

| Column | Purpose |
|---|---|
| `order_id` | Order business key |
| `customer_id` | Customer business key |
| `order_status` | Standardized order status |
| `order_ts` | Order timestamp |
| `order_date` | Derived order date |
| `currency_code` | Standardized currency code |
| `payment_method` | Payment method |
| `source_system` | Source system |
| `silver_processed_ts` | Silver processing timestamp |
| Bronze metadata columns | Traceability |

#### Allowed Order Status Values

Allowed values:

```text
CREATED
PAID
COMPLETED
CANCELLED
REFUNDED
```

Any other status is rejected.

#### Allowed Currency Values

Allowed values:

```text
MXN
USD
```

The source value `EUR` is intentionally rejected in this MVP.

#### Order Validation Rules

Order validation rules:

```text
order_id is required
customer_id is required
customer_id must exist in valid Silver customers
order_ts must be valid
order_status must be allowed
currency_code must be allowed
```

#### Rejected Order Examples

| Order | Rejection reason |
|---|---|
| `ORD-1006` | `customer_id_not_found` |
| `ORD-1008` | `currency_code_is_not_allowed` |
| `ORD-1009` | `order_status_is_not_allowed` |

Expected clean row count:

```text
7
```

### 12. Order Items Silver Design

Output table:

```text
silver_order_items_clean
```

Source:

```text
bronze_order_items
```

The order items Silver process validates order line-level data and ensures that each order item references a valid order and product.

#### Order Item Columns

Important output fields include:

| Column | Purpose |
|---|---|
| `order_id` | Order business key |
| `product_id` | Product business key |
| `quantity` | Quantity ordered |
| `unit_price` | Unit price |
| `discount_amount` | Discount amount |
| `line_total` | Derived line total |
| `silver_processed_ts` | Silver processing timestamp |
| Bronze metadata columns | Traceability |

#### Order Item Validation Rules

Order item validation rules:

```text
order_id is required
order_id must exist in valid Silver orders
product_id is required
product_id must exist in valid Silver products
quantity must be valid
quantity must be greater than 0
unit_price must be valid
unit_price must be greater than 0
discount_amount must be valid
discount_amount cannot be negative
line_total cannot be negative
```

#### Parent Order Validation

If an order item references an order that was rejected from `silver_orders_clean`, the item is also rejected.

This prevents Gold fact tables from including item details for invalid parent orders.

Rejected order item reason:

```text
order_id_not_found_or_parent_order_rejected
```

Expected clean row count:

```text
8
```

### 13. Rejected Records Table

Output table:

```text
silver_rejected_records
```

The rejected records table stores invalid records from all Silver entity validations.

It uses a normalized structure so different entity failures can be reviewed in one place.

#### Rejected Record Columns

| Column | Purpose |
|---|---|
| `entity_name` | Entity that failed validation |
| `reject_reason` | First detected validation failure |
| `ingestion_batch_id` | Source batch |
| `source_file_path` | Original source file path |
| `raw_record_hash` | Raw record fingerprint from Bronze |
| `rejected_at` | Rejection timestamp |
| `record_json` | Original rejected record serialized as JSON |

#### Expected Rejected Records

The project expects six rejected records.

| Entity | Rejection reason | Expected count |
|---|---|---:|
| `orders` | `currency_code_is_not_allowed` | 1 |
| `orders` | `customer_id_not_found` | 1 |
| `orders` | `order_status_is_not_allowed` | 1 |
| `order_items` | `order_id_not_found_or_parent_order_rejected` | 3 |

Expected row count:

```text
6
```

### 14. First-Rejection Rule

The current MVP records the first applicable rejection reason for each invalid record.

For example, an order item may have more than one possible issue.

However, the validation logic records the first detected reason.

This keeps the MVP simple and readable.

A future enhancement could capture multiple validation errors per record.

### 15. Referential Validation

The Silver layer includes referential validation.

#### Orders to Customers

Orders must reference valid customers.

```text
orders.customer_id → silver_customers_clean.customer_id
```

If the customer does not exist, the order is rejected.

#### Order Items to Orders

Order items must reference valid orders.

```text
order_items.order_id → silver_orders_clean.order_id
```

If the order was rejected, the order item is rejected.

#### Order Items to Products

Order items must reference valid products.

```text
order_items.product_id → silver_products_clean.product_id
```

If the product does not exist, the order item is rejected.

### 16. Silver Expected Counts

After successful execution, expected Silver counts are:

| Silver table | Expected row count |
|---|---:|
| `silver_customers_clean` | 9 |
| `silver_products_clean` | 4 |
| `silver_orders_clean` | 7 |
| `silver_order_items_clean` | 8 |
| `silver_rejected_records` | 6 |

These counts are validated in the final data quality notebook.

### 17. Silver Delta Format

All Silver outputs are written as Delta tables.

Each Silver table path contains:

```text
_delta_log/
part-...
```

This allows the project to maintain Delta transaction history for clean and rejected records.

Silver tables are later consumed by Gold processing notebooks.

### 18. Silver Validation

The Silver notebook validates this layer through notebook outputs.

#### Silver Summary

The Silver summary shows row counts for all Silver tables.

Expected summary:

| Table | Expected row count |
|---|---:|
| `silver_customers_clean` | 9 |
| `silver_products_clean` | 4 |
| `silver_orders_clean` | 7 |
| `silver_order_items_clean` | 8 |
| `silver_rejected_records` | 6 |

#### Rejected Records Summary

The notebook displays rejected records grouped by:

```text
entity_name
reject_reason
```

This confirms that expected validation failures were captured.

#### Valid Orders Preview

The notebook displays valid orders that passed Silver validation.

#### Valid Order Items Preview

The notebook displays valid order item records that passed Silver validation.

#### Delta History

The notebook can inspect Delta history for Silver outputs using the Delta API.

### 19. Silver Evidence

Recommended evidence for this layer is defined in:

```text
docs/evidence_index.md
```

Recommended Silver evidence includes:

| Evidence file | Purpose |
|---|---|
| `01_silver_summary.png` | Shows Silver table row counts |
| `02_rejected_records_summary.png` | Shows rejected records grouped by entity and reason |
| `03_silver_orders_clean.png` | Shows valid Silver orders |
| `04_silver_order_items_clean.png` | Shows valid Silver order items |
| `05_silver_delta_history.png` | Shows Delta history for Silver tables |

### 20. Silver Design Decisions

#### Separate Clean and Rejected Records

The project writes valid records to clean tables and invalid records to a rejected records table.

This avoids losing bad data while preventing it from contaminating Gold analytics.

#### Validate Parent-Child Relationships

The project rejects order items whose parent order is invalid.

This keeps Gold fact tables consistent.

#### Keep Bronze Metadata

The Silver layer preserves Bronze metadata fields.

This allows downstream records to be traced back to source batches and files.

#### Use Delta Tables

Silver outputs are written in Delta format to maintain transactional consistency and history.

### 21. Silver Layer Limitations

The Silver implementation is intentionally scoped for an MVP.

Known limitations:

- It captures the first rejection reason only.
- It does not capture multiple validation errors per record.
- It does not implement a configurable rule engine.
- It does not use external data quality frameworks.
- It does not implement quarantine workflows beyond rejected records.
- It overwrites Silver outputs during development reruns.
- It does not register Silver tables in Unity Catalog.

These limitations are acceptable for the current project scope.

### 22. Silver Layer Summary

The Silver layer converts Bronze data into trusted Delta tables.

It demonstrates:

- Data type casting
- String standardization
- Required-field validation
- Allowed-value validation
- Referential validation
- Derived field calculation
- Rejected records handling
- Clean table creation
- Delta table writes
- Traceability through preserved metadata

The Silver layer is the quality gate that enables reliable Gold dimensional modeling and analytical outputs.