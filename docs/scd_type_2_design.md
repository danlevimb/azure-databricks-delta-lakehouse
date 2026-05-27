# SCD Type 2 Design

This document describes the Slowly Changing Dimension Type 2 design implemented in the `azure-databricks-delta-lakehouse` project.

The SCD Type 2 pattern is used to preserve customer history in the Gold layer.

### 1. SCD Type 2 Objective

The objective of the SCD Type 2 design is to preserve historical customer attribute changes instead of overwriting previous customer values.

In analytical systems, customer attributes may change over time.

Examples:

```text
Customer changes city
Customer changes state
Customer changes segment
Customer changes email
Customer receives a loyalty tier
```

If the pipeline only stored the current customer state, historical orders could be incorrectly analyzed using the latest customer attributes.

SCD Type 2 solves this by storing multiple versions of a customer record.

### 2. Where SCD Type 2 Is Implemented

SCD Type 2 is implemented in the Gold layer.

Notebook:

```text
04_gold_dimensions
```

Target table:

```text
gold_dim_customer_scd2
```

Source table:

```text
silver_customers_clean
```

The output is stored as a Delta table in the `gold` container.

Logical path:

```text
gold/delta_lakehouse/gold_dim_customer_scd2
```

### 3. Why Customer Uses SCD Type 2

The customer dimension was selected for SCD Type 2 because customer attributes are likely to change over time and those changes can affect analytical reporting.

Examples:

| Customer | Change scenario |
|---|---|
| `CUST-001` | Email changes and loyalty tier appears |
| `CUST-002` | City and customer segment change |
| `CUST-003` | Loyalty tier appears |
| `CUST-005` | New customer appears |
| `CUST-006` | New customer appears |

This provides controlled data scenarios for validating historical customer tracking.

### 4. Business Problem Solved

Without SCD Type 2, a customer record would be overwritten.

Example:

```text
CUST-002 initially belongs to SMB segment.
Later, CUST-002 becomes Enterprise.
```

A current-state-only dimension would show only:

```text
CUST-002 → Enterprise
```

That would make historical reporting less accurate because older orders could appear as if they were placed by an Enterprise customer, even if the customer was SMB at the time.

SCD Type 2 preserves both states:

```text
CUST-002 → SMB        → historical version
CUST-002 → Enterprise → current version
```

### 5. SCD Type 2 Table

Target table:

```text
gold_dim_customer_scd2
```

This table stores one row per customer version.

A customer can have multiple records in this table, but only one record should be marked as current.

### 6. SCD Type 2 Columns

The SCD Type 2 table includes the following columns.

| Column | Purpose |
|---|---|
| `customer_sk` | Surrogate key for the customer version |
| `customer_id` | Business key from the source system |
| `customer_name` | Customer name |
| `email` | Customer email |
| `city` | Customer city |
| `state` | Customer state |
| `customer_segment` | Customer segment |
| `loyalty_tier` | Optional loyalty tier |
| `effective_start_ts` | Timestamp when the version becomes valid |
| `effective_end_ts` | Timestamp when the version stops being valid |
| `is_current` | Indicates the active customer version |
| `record_hash` | Hash of tracked business attributes |
| `ingestion_batch_id` | Source batch traceability |
| `source_file_path` | Source file traceability |
| `raw_record_hash` | Raw source record fingerprint |
| `silver_processed_ts` | Timestamp from Silver processing |
| `gold_processed_ts` | Timestamp from Gold processing |

### 7. Business Key

The business key for the customer dimension is:

```text
customer_id
```

This value identifies the customer across source batches.

Example:

```text
CUST-002
```

The business key does not uniquely identify a row in the SCD2 table because the same customer can have multiple historical versions.

### 8. Surrogate Key

The surrogate key is:

```text
customer_sk
```

In this MVP, the surrogate key is generated deterministically using a hash of:

```text
customer_id
effective_start_ts
record_hash
```

This creates a unique key per customer version.

Conceptually:

```text
customer_sk = sha2(customer_id + effective_start_ts + record_hash)
```

This approach is suitable for a path-based Delta Lake MVP because it avoids relying on database identity columns or warehouse-generated surrogate keys.

### 9. Tracked Attributes

The SCD2 logic tracks the following customer attributes:

```text
customer_name
email
city
state
customer_segment
loyalty_tier
```

If any of these attributes changes, the customer receives a new version.

### 10. Non-Tracked Attributes

Technical metadata columns are not used to determine whether a customer business version changed.

Examples of metadata columns not used for SCD2 change detection:

```text
ingestion_batch_id
source_file_path
raw_record_hash
silver_processed_ts
gold_processed_ts
```

These fields support traceability, but they should not create new business versions by themselves.

### 11. Change Detection Strategy

The notebook calculates a `record_hash` from the tracked customer attributes.

Conceptually:

```text
record_hash = sha2(customer_name, email, city, state, customer_segment, loyalty_tier)
```

Then, for each `customer_id`, records are ordered by:

```text
effective_start_ts
```

The current record hash is compared to the previous record hash.

```text
Same record_hash      → no new business version
Different record_hash → create new SCD2 version
```

This avoids creating duplicate historical versions when the business attributes did not change.

### 12. Effective Dating Strategy

The SCD2 table uses two effective timestamp columns:

```text
effective_start_ts
effective_end_ts
```

The `effective_start_ts` comes from the source field:

```text
effective_update_ts
```

The `effective_end_ts` is calculated using the next version's `effective_start_ts`.

Conceptually:

```text
effective_end_ts = next effective_start_ts
```

The current version has:

```text
effective_end_ts = null
```

### 13. Current Version Flag

The SCD2 table uses:

```text
is_current
```

Current records have:

```text
is_current = true
effective_end_ts = null
```

Historical records have:

```text
is_current = false
effective_end_ts is not null
```

The final validation notebook checks that each customer has exactly one current version.

### 14. Example: CUST-002

`CUST-002` changes from one city and segment to another.

Conceptual source changes:

```text
batch_001:
CUST-002 | Monterrey | SMB

batch_002:
CUST-002 | San Pedro Garza Garcia | Enterprise
```

Expected SCD2 output:

```text
customer_id | city                  | customer_segment | effective_start_ts | effective_end_ts | is_current
CUST-002    | Monterrey             | SMB              | 2026-05-01         | 2026-05-02       | false
CUST-002    | San Pedro Garza Garcia| Enterprise       | 2026-05-02         | null             | true
```

This proves that the customer dimension preserves historical customer attributes.

### 15. Example: CUST-001

`CUST-001` receives a changed email and a loyalty tier in batch 003.

Conceptual behavior:

```text
Initial version:
email = contact@northwind.example
loyalty_tier = null

New version:
email = newcontact@northwind.example
loyalty_tier = Gold
```

Expected result:

```text
CUST-001 has two SCD2 versions.
```

The first version is historical.

The second version is current.

### 16. Example: CUST-003

`CUST-003` receives a loyalty tier in batch 003.

Conceptual behavior:

```text
Initial version:
loyalty_tier = null

New version:
loyalty_tier = Platinum
```

Expected result:

```text
CUST-003 has two SCD2 versions.
```

This validates that the schema evolution attribute participates in historical tracking.

### 17. SCD2 Processing Flow

The SCD2 processing flow follows this sequence:

```text
Read silver_customers_clean
      ↓
Select customer business attributes
      ↓
Calculate record_hash
      ↓
Order records by customer_id and effective_start_ts
      ↓
Compare current record_hash with previous record_hash
      ↓
Keep only changed versions
      ↓
Calculate effective_end_ts using next version
      ↓
Mark current version
      ↓
Generate customer_sk
      ↓
Write gold_dim_customer_scd2
```

### 18. PySpark Concepts Used

The SCD2 notebook uses several PySpark concepts.

| Concept | Purpose |
|---|---|
| `select` | Select required fields |
| `withColumn` | Create derived columns |
| `sha2` | Generate hash values |
| `concat_ws` | Concatenate tracked attributes |
| `coalesce` | Handle null values in hash generation |
| `Window.partitionBy` | Process records by customer |
| `orderBy` | Order customer changes by timestamp |
| `lag` | Compare current and previous hash |
| `lead` | Calculate next version start timestamp |
| `filter` | Keep only changed versions |

### 19. Delta Lake Concepts Used

The output table is written as a Delta table.

Delta Lake supports:

- Transactional writes
- Table history
- Versioned data
- Reliable reads
- Integration with later Gold processing

The SCD2 table is stored using path-based Delta format.

Example logical path:

```text
gold/delta_lakehouse/gold_dim_customer_scd2
```

### 20. Relationship to Gold Fact Table

The SCD2 customer dimension is used by:

```text
gold_fact_orders
```

The fact table joins orders to customer history using an as-of join.

Conceptually:

```text
order_ts >= effective_start_ts
and order_ts < effective_end_ts
```

For current customer records:

```text
effective_end_ts is null
```

This ensures each order links to the customer version that was active when the order occurred.

### 21. Why As-Of Join Matters

An as-of join prevents historical facts from being analyzed using the wrong customer attributes.

Example:

```text
A customer was SMB when an order was placed.
Later, the customer became Enterprise.
```

Without SCD2 and as-of joins, the old order could be reported as an Enterprise order.

With SCD2, the old order remains tied to the SMB historical version.

This supports historically accurate analytics.

### 22. Validation Rules

The final validation notebook checks SCD2 correctness.

Notebook:

```text
08_data_quality_validation
```

SCD2 validations include:

```text
Each customer has exactly one current record.
Historical effective dates are valid.
Fact orders resolve customer surrogate keys.
```

### 23. Current Version Validation

The project validates that every customer has exactly one current record.

Expected condition:

```text
current_versions = 1 for each customer_id
```

If a customer has zero current versions or more than one current version, the validation should fail.

### 24. Effective Date Validation

The project validates that closed historical records have valid date ranges.

Expected condition:

```text
effective_end_ts > effective_start_ts
```

If a historical version has an end timestamp earlier than or equal to its start timestamp, the validation should fail.

### 25. Expected SCD2 Counts

The customer dimension is expected to contain nine total rows.

Expected customer version counts:

| Customer | Expected versions |
|---|---:|
| `CUST-001` | 2 |
| `CUST-002` | 2 |
| `CUST-003` | 2 |
| `CUST-004` | 1 |
| `CUST-005` | 1 |
| `CUST-006` | 1 |

Total:

```text
9 SCD2 records
```

### 26. Expected Current Records

Each customer should have exactly one current version.

Expected current version count:

| Customer | Expected current records |
|---|---:|
| `CUST-001` | 1 |
| `CUST-002` | 1 |
| `CUST-003` | 1 |
| `CUST-004` | 1 |
| `CUST-005` | 1 |
| `CUST-006` | 1 |

### 27. SCD2 Evidence

Recommended evidence for SCD2 is defined in:

```text
docs/evidence_index.md
```

Relevant evidence files include:

| Evidence file | Purpose |
|---|---|
| `01_gold_customer_scd2_versions.png` | Shows historical customer versions |
| `02_gold_customer_current_validation.png` | Shows one current version per customer |
| `02_fact_orders_scd2_join.png` | Shows fact orders joined to historical customer versions |
| `01_validation_report.png` | Shows final validation report |
| `02_validation_status_summary.png` | Shows all validations passing |

### 28. SCD2 Design Decisions

#### Use Customer Dimension for SCD2

Customer attributes are realistic candidates for historical tracking.

#### Use Hash-Based Change Detection

Hash-based detection simplifies comparison across multiple tracked attributes.

#### Use Source Effective Timestamp

The design uses `effective_update_ts` from the source data as the business-effective timestamp.

#### Use Deterministic Surrogate Key

The surrogate key is generated from customer version attributes to keep the MVP simple and reproducible.

#### Keep Product as Current-State Dimension

The project does not implement SCD2 for products in the MVP.

Products are used instead to demonstrate Delta MERGE / upsert.

### 29. SCD2 Limitations

This SCD2 implementation is intentionally scoped for an MVP.

Known limitations:

- It does not use a warehouse identity column for surrogate keys.
- It does not support late-arriving changes.
- It does not handle same-timestamp competing versions beyond deterministic ordering.
- It does not implement Type 1 overwrite behavior.
- It does not implement hybrid SCD Type 1 and Type 2 behavior.
- It does not use a production data warehouse dimension management framework.
- It does not register the dimension in Unity Catalog.
- It rebuilds the dimension during development reruns.

These limitations are acceptable for the current portfolio project scope.

### 30. Future Enhancements

Possible future enhancements include:

- Handling late-arriving dimension changes
- Supporting Type 1 and Type 2 hybrid behavior
- Adding configurable tracked attribute lists
- Adding multi-source dimension consolidation
- Adding generated surrogate keys through a managed warehouse layer
- Registering the table in Unity Catalog
- Adding automated SCD2 unit tests
- Creating a reusable SCD2 PySpark function or framework

### 31. SCD2 Summary

The SCD Type 2 implementation preserves customer history in the Gold layer.

It demonstrates:

- Business-key-based versioning
- Hash-based change detection
- Effective start and end dating
- Current record identification
- Historical customer versioning
- Fact-to-dimension as-of joins
- SCD2 validation rules
- Delta table storage

This pattern is one of the most important analytical modeling capabilities demonstrated by the project.