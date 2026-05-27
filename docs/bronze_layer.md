# Bronze Layer

This document describes the Bronze layer implementation for the `azure-databricks-delta-lakehouse` project.

The Bronze layer is the first Delta Lake layer in the pipeline. Its main purpose is to preserve source data with technical traceability.

### 1. Bronze Layer Objective

The Bronze layer converts source CSV files from the landing container into Delta tables.

The objective is to preserve source records close to their original shape while adding technical metadata required for traceability.

The Bronze layer answers these questions:

- What data arrived?
- Which source entity did it come from?
- Which batch did it come from?
- Which file did it come from?
- When was it ingested?
- What is the technical fingerprint of the raw record?

The Bronze layer does not apply strict business validation.

Invalid business records are intentionally preserved in Bronze and handled later in Silver.

### 2. Bronze Position in the Lakehouse

The Bronze layer sits between the raw landing area and the validated Silver layer.

```text
landing/source_data
      ↓
bronze/delta_lakehouse
      ↓
silver/delta_lakehouse
```

Bronze is the first point where source data becomes Delta Lake data.

This means the project moves from raw CSV files into transactional Delta tables with `_delta_log` history.

### 3. Source Inputs

The Bronze notebook reads CSV files from the `landing` container.

Input path:

```text
landing/source_data/
```

Expected source batches:

```text
batch_001
batch_002
batch_003_schema_evolution
```

Expected source entities:

```text
customers
products
orders
order_items
```

Source file examples:

```text
landing/source_data/batch_001/customers.csv
landing/source_data/batch_001/products.csv
landing/source_data/batch_001/orders.csv
landing/source_data/batch_001/order_items.csv

landing/source_data/batch_002/customers.csv
landing/source_data/batch_002/orders.csv
landing/source_data/batch_002/order_items.csv

landing/source_data/batch_003_schema_evolution/customers.csv
landing/source_data/batch_003_schema_evolution/orders.csv
landing/source_data/batch_003_schema_evolution/order_items.csv
```

### 4. Bronze Outputs

The Bronze notebook writes Delta tables to the `bronze` container.

Output base path:

```text
bronze/delta_lakehouse/
```

Bronze Delta tables:

```text
bronze_customers
bronze_products
bronze_orders
bronze_order_items
```

Full logical paths:

```text
bronze/delta_lakehouse/bronze_customers
bronze/delta_lakehouse/bronze_products
bronze/delta_lakehouse/bronze_orders
bronze/delta_lakehouse/bronze_order_items
```

### 5. Bronze Notebook

Notebook:

```text
02_bronze_ingestion
```

Main responsibilities:

- Read source CSV files from landing
- Iterate through configured batches
- Iterate through configured entities
- Skip missing source files safely
- Add Bronze technical metadata
- Union data across batches by entity
- Preserve schema evolution using `unionByName`
- Write Delta tables to the Bronze container
- Validate row counts and Delta history

### 6. Bronze Processing Flow

The notebook follows this logical flow:

```text
Read entity files from each batch
        ↓
Add technical metadata
        ↓
Union records by entity
        ↓
Write entity-level Bronze Delta table
        ↓
Validate output row counts
        ↓
Inspect Delta history
```

For each entity, the notebook reads all available batch files and writes one consolidated Bronze Delta table.

Example:

```text
batch_001/customers.csv
batch_002/customers.csv
batch_003_schema_evolution/customers.csv
        ↓
bronze_customers
```

### 7. Bronze Configuration

The notebook uses three main configuration lists.

#### Batches

```text
batch_001
batch_002
batch_003_schema_evolution
```

#### Entities

```text
customers
products
orders
order_items
```

#### Base Paths

```text
landing_base_path
source_base_path
bronze_base_path
```

The configuration allows the notebook to process multiple entities and batches without hardcoding every input file separately.

### 8. Bronze Technical Metadata

Bronze adds technical metadata to every record.

| Column | Purpose |
|---|---|
| `ingestion_batch_id` | Identifies the source batch |
| `source_entity` | Identifies the source entity |
| `source_file_path` | Captures the source file path |
| `ingestion_ts` | Captures when the row was ingested |
| `bronze_load_date` | Captures the load date |
| `raw_record_hash` | Creates a technical fingerprint of the raw record |

This metadata is used for traceability across later layers.

#### Example Metadata Use

If a record is rejected in Silver, the project can still trace it back to:

```text
source entity
source batch
source file path
raw record hash
```

This is important for auditability and debugging.

### 9. Raw Record Hash

The Bronze layer creates a `raw_record_hash` using the source columns.

The purpose of this hash is to create a deterministic fingerprint of the source record values.

Conceptually:

```text
raw_record_hash = sha2(concat(all source column values), 256)
```

This supports:

- Traceability
- Change detection patterns
- Debugging
- Rejected record analysis
- Record-level lineage

The hash is not used as a security feature.

It is used as a technical metadata field.

### 10. Schema Evolution Handling

The project includes a controlled schema evolution scenario in the `customers` source.

The column:

```text
loyalty_tier
```

appears only in:

```text
batch_003_schema_evolution/customers.csv
```

Earlier customer batches do not contain this column.

The Bronze layer preserves this new column when building `bronze_customers`.

This demonstrates that Bronze can keep source schema changes visible instead of discarding them.

#### Expected Behavior

Earlier customer records have:

```text
loyalty_tier = null
```

Batch 003 customer records may contain:

```text
Gold
Platinum
Silver
```

This preserved column is later handled explicitly in Silver and included in Gold SCD Type 2 tracking.

### 11. Why Bronze Preserves Invalid Data

The Bronze layer intentionally keeps invalid business records.

Examples of invalid records preserved in Bronze include:

```text
orders.customer_id = CUST-999
orders.currency_code = EUR
orders.order_status = INVALID_STATUS
order_items.quantity = -1
order_items connected to rejected parent orders
```

Bronze does not reject these records because its role is not to decide business validity.

Its role is to preserve what arrived.

Business validation happens in Silver.

This separation keeps the Lakehouse design clear:

```text
Bronze = raw and traceable
Silver = clean and validated
Gold = modeled and analytical
```

### 12. Bronze Table Design

#### bronze_customers

Source:

```text
customers.csv
```

Purpose:

```text
Preserve customer source records across all batches.
```

Important fields:

- `customer_id`
- `customer_name`
- `email`
- `city`
- `state`
- `customer_segment`
- `effective_update_ts`
- `loyalty_tier`
- Bronze technical metadata

Expected row count:

```text
9
```

#### bronze_products

Source:

```text
products.csv
```

Purpose:

```text
Preserve product reference data.
```

Important fields:

- `product_id`
- `product_name`
- `category`
- `unit_price`
- `is_active`
- Bronze technical metadata

Expected row count:

```text
4
```

#### bronze_orders

Source:

```text
orders.csv
```

Purpose:

```text
Preserve order header data across all batches.
```

Important fields:

- `order_id`
- `customer_id`
- `order_status`
- `order_ts`
- `currency_code`
- `payment_method`
- `source_system`
- Bronze technical metadata

Expected row count:

```text
10
```

#### bronze_order_items

Source:

```text
order_items.csv
```

Purpose:

```text
Preserve order line-level data across all batches.
```

Important fields:

- `order_id`
- `product_id`
- `quantity`
- `unit_price`
- `discount_amount`
- Bronze technical metadata

Expected row count:

```text
11
```

### 13. Expected Bronze Counts

After successful execution, the expected Bronze counts are:

| Bronze table | Expected row count |
|---|---:|
| `bronze_customers` | 9 |
| `bronze_products` | 4 |
| `bronze_orders` | 10 |
| `bronze_order_items` | 11 |

These counts match the total source rows generated across all source batches.

### 14. Bronze Delta Format

Each Bronze table is written using Delta format.

This means each table path contains:

```text
_delta_log/
part-...
```

The `_delta_log` directory stores Delta transaction metadata.

This allows the project to demonstrate that source CSV files were not only copied as files, but converted into Delta Lake tables.

### 15. Bronze Validation

The Bronze notebook validates the layer in multiple ways.

#### Ingestion Summary

The notebook produces a summary showing:

- Entity name
- Batch ID
- Source file path
- Rows read

This confirms that each expected source file was processed.

#### Row Count Validation

The notebook reads each Bronze Delta table and counts rows by batch.

This confirms that records from all expected batches were included.

#### Schema Validation

The notebook prints and displays the `bronze_customers` schema to confirm that schema evolution was preserved.

The important column is:

```text
loyalty_tier
```

#### Delta History Validation

The notebook reads Delta history for Bronze tables using the Delta API.

This confirms that Bronze outputs are Delta tables with transaction history.

### 16. Bronze Evidence

Recommended evidence for this layer is defined in:

```text
docs/evidence_index.md
```

Recommended Bronze evidence includes:

| Evidence file | Purpose |
|---|---|
| `01_bronze_ingestion_summary.png` | Shows rows processed by entity and batch |
| `02_bronze_customers_schema_evolution.png` | Shows `loyalty_tier` preserved in Bronze |
| `03_bronze_table_folders.png` | Shows Bronze Delta table folders |
| `04_bronze_delta_history.png` | Shows Delta history for a Bronze table |

### 17. Bronze Design Decisions

#### Use Delta Instead of Keeping CSV

CSV files are only the source input.

Bronze converts them to Delta tables to demonstrate Lakehouse table storage.

#### Preserve Invalid Records

Invalid records remain in Bronze because validation belongs to Silver.

This supports full traceability from rejected records back to raw source data.

#### Add Technical Metadata

Metadata is added early so all downstream layers can preserve lineage information.

#### Use Path-Based Delta Tables

The project uses path-based Delta tables to keep the MVP focused on PySpark and Delta Lake concepts without adding catalog governance complexity.

### 18. Bronze Layer Limitations

The Bronze implementation is intentionally scoped for an MVP.

Known limitations:

- It uses controlled sample files rather than a production ingestion source.
- It overwrites Bronze outputs during development reruns.
- It does not implement streaming ingestion.
- It does not implement incremental file discovery.
- It does not register tables in Unity Catalog.
- It records a single raw record hash but does not maintain a full ingestion audit table.

These limitations are acceptable for the current project scope.

### 19. Bronze Layer Summary

The Bronze layer establishes the first Delta Lake stage of the project.

It demonstrates:

- Reading source CSV files from ADLS Gen2
- Processing multiple batches
- Consolidating data by entity
- Adding technical metadata
- Preserving schema evolution
- Keeping invalid records for traceability
- Writing path-based Delta tables
- Validating Delta table history

The Bronze layer provides a traceable foundation for Silver validation and Gold analytical modeling.