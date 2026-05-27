# Time Travel Validation

This document describes the Delta Lake time travel validation implemented in the `azure-databricks-delta-lakehouse` project.

The time travel validation demonstrates how Delta Lake table versions can be queried to compare data states before and after a table mutation.

### 1. Time Travel Objective

The objective of this validation is to prove that Delta Lake preserves table versions that can be queried after transactional changes.

In this project, time travel is used to compare the `gold_dim_product` table before and after a Delta MERGE operation.

The validation demonstrates:

- Delta table history
- Versioned table reads
- Before/after comparison
- MERGE impact validation
- Product update verification
- Product insert verification

### 2. Where Time Travel Is Implemented

Time travel validation is implemented in:

```text
07_time_travel_validation
```

Target table:

```text
gold_dim_product
```

Logical path:

```text
gold/delta_lakehouse/gold_dim_product
```

This table is used because the previous notebook modifies it through Delta MERGE.

### 3. Relationship to Delta MERGE

The time travel validation depends on the MERGE operation from:

```text
06_delta_merge_upsert_demo
```

The MERGE notebook applies two product changes:

```text
PROD-002 → updated existing product
PROD-005 → inserted new product
```

The time travel notebook validates the table state before and after that operation.

Expected comparison:

```text
Before MERGE → 4 products
After MERGE  → 5 products
```

### 4. Why Time Travel Matters

Time travel is useful because it allows data engineers to inspect historical table states.

This supports:

- Debugging
- Validation
- Audit-style inspection
- Before/after comparison
- Reproducibility
- Understanding the impact of data changes

In this project, time travel proves that Delta Lake is not just storing files. It is maintaining versioned table history through the Delta transaction log.

### 5. Delta History

The notebook starts by reading the Delta table history.

Conceptually:

```text
Read Delta history for gold_dim_product
```

The expected history includes at least:

```text
WRITE
MERGE
```

The initial write creates the product dimension.

The MERGE operation updates one product and inserts another.

### 6. Version Discovery

The notebook identifies available Delta versions dynamically.

It calculates:

```text
initial_version
latest_version
latest_merge_version
```

The initial version represents the first available version of the product dimension.

The latest version represents the most recent state of the table.

The latest MERGE version identifies the most recent version created by a MERGE operation.

#### Version Number Note

The exact latest version number may change if the MERGE notebook is executed more than once.

For example, the latest version may be:

```text
version 1
```

or:

```text
version 2
```

or higher.

The important validation is not the fixed version number.

The important validation is that the table can be read before and after MERGE and that the expected business state is correct.

### 7. Versioned Read Strategy

The project uses path-based Delta tables.

Because of this, the notebook reads table versions using `versionAsOf`.

Conceptually:

```python
spark.read.format("delta").option("versionAsOf", version_number).load(table_path)
```

This allows the notebook to load a historical version of the Delta table from its physical ADLS Gen2 path.

### 8. Initial Version Read

The initial version represents the product dimension before the MERGE operation.

Expected table state:

```text
PROD-001
PROD-002
PROD-003
PROD-004
```

Expected count:

```text
4 products
```

At this stage:

```text
PROD-002 unit_price = 35.00
PROD-005 does not exist
```

### 9. Latest Version Read

The latest version represents the product dimension after the MERGE operation.

Expected table state:

```text
PROD-001
PROD-002
PROD-003
PROD-004
PROD-005
```

Expected count:

```text
5 products
```

At this stage:

```text
PROD-002 unit_price = 38.00
PROD-005 exists
```

### 10. Product Count Validation

The notebook compares product counts between the initial and latest versions.

Expected result:

| Version label | Expected product count | Expected distinct product count |
|---|---:|---:|
| `before_merge` | 4 | 4 |
| `after_merge` | 5 | 5 |

This validates that the MERGE inserted one new product and did not create duplicate product IDs.

### 11. PROD-002 Update Validation

The notebook validates the update side of the MERGE.

Before MERGE:

```text
PROD-002 | Ice Bag 5kg | unit_price = 35.00
```

After MERGE:

```text
PROD-002 | Ice Bag 5kg | unit_price = 38.00
```

This proves that an existing row was updated through Delta MERGE.

### 12. PROD-005 Insert Validation

The notebook validates the insert side of the MERGE.

Before MERGE:

```text
PROD-005 does not exist
```

After MERGE:

```text
PROD-005 exists
```

Expected inserted product:

```text
PROD-005 | Ice Bag 10kg | unit_price = 62.00
```

This proves that a new row was inserted through Delta MERGE.

### 13. Affected Products Comparison

The notebook produces a focused comparison for affected products.

Products included:

```text
PROD-002
PROD-005
```

Expected comparison:

| Version label | Product ID | Expected result |
|---|---|---|
| `before_merge` | `PROD-002` | Existing product with old price |
| `after_merge` | `PROD-002` | Existing product with updated price |
| `before_merge` | `PROD-005` | Not present |
| `after_merge` | `PROD-005` | New product present |

This output is useful evidence because it clearly shows the before/after effect of the MERGE.

### 14. Delta Transaction Log

Delta Lake stores table transaction history in the `_delta_log` directory.

For the product dimension, the physical table folder contains:

```text
gold/delta_lakehouse/gold_dim_product/
  _delta_log/
  part-...
```

The `_delta_log` enables Delta Lake to reconstruct previous table states.

Time travel reads are possible because Delta maintains this versioned transaction metadata.

### 15. Time Travel vs Backup

Time travel is useful for validation and debugging, but it should not be treated as a replacement for a formal backup and disaster recovery strategy.

In this MVP, time travel is used to demonstrate:

```text
Versioned analytical table validation
```

It is not positioned as:

```text
Long-term backup
Disaster recovery
Enterprise retention strategy
```

This distinction is important for technical accuracy.

### 16. Validation Flow

The notebook follows this sequence:

```text
Load Delta table history
      ↓
Identify initial and latest versions
      ↓
Read initial version using versionAsOf
      ↓
Read latest version using versionAsOf
      ↓
Compare product counts
      ↓
Validate PROD-002 update
      ↓
Validate PROD-005 insert
      ↓
Display affected products before and after MERGE
```

### 17. Expected Outputs

The notebook produces several validation outputs.

#### Delta History Output

Expected operations include:

```text
WRITE
MERGE
```

#### Version Count Output

Expected result:

```text
before_merge → 4 products
after_merge  → 5 products
```

#### PROD-002 Output

Expected result:

```text
unit_price_before = 35.00
unit_price_after  = 38.00
```

#### PROD-005 Output

Expected result:

```text
exists_before_merge = false
exists_after_merge  = true
```

#### Affected Products Output

Expected result:

```text
PROD-002 appears before and after MERGE with changed price.
PROD-005 appears only after MERGE.
```

### 18. Relationship to Final Validation Notebook

The final validation notebook checks that the MERGE history exists and the final product dimension is correct.

Notebook:

```text
08_data_quality_validation
```

Related validations:

```text
gold_dim_product_no_duplicate_product_id
delta_merge_updated_existing_product
delta_merge_inserted_new_product
gold_dim_product_delta_history_contains_merge
```

The time travel notebook provides visual before/after evidence.

The final validation notebook provides consolidated PASS / FAIL validation.

### 19. Evidence

Recommended evidence for time travel is defined in:

```text
docs/evidence_index.md
```

Relevant evidence folder:

```text
evidence/08_time_travel/
```

Recommended evidence files:

| Evidence file | Purpose |
|---|---|
| `01_delta_history_versions.png` | Shows Delta versions and operations |
| `02_version_count_comparison.png` | Shows product counts before and after MERGE |
| `03_affected_products_comparison.png` | Shows affected products before and after MERGE |
| `04_product_insert_validation.png` | Shows `PROD-005` insert validation |

### 20. Design Decisions

#### Use Product Dimension for Time Travel

The product dimension is used because it has a simple and clear before/after MERGE scenario.

#### Use Version Numbers Instead of Timestamps

The notebook uses `versionAsOf` because version-based validation is deterministic and easier to explain in a portfolio project.

#### Detect Versions Dynamically

The notebook detects initial and latest versions dynamically instead of hardcoding all version numbers.

This makes the validation more resilient to reruns.

#### Focus on Business Impact

The validation focuses on the business effect of the MERGE:

```text
Product updated
Product inserted
No duplicate product IDs
```

This is easier to understand than only showing low-level transaction metadata.

### 21. Known Limitations

This time travel validation is intentionally scoped for an MVP.

Known limitations:

- It validates a small product dimension.
- It does not implement restore or rollback.
- It does not configure custom retention policies.
- It does not validate long-term data retention.
- It does not use timestamp-based time travel.
- It does not compare large-scale historical snapshots.
- It does not replace formal backup or disaster recovery.

These limitations are acceptable for the current project scope.

### 22. Future Enhancements

Possible future enhancements include:

- Adding timestamp-based time travel examples
- Comparing larger table versions
- Demonstrating rollback-style recovery patterns
- Adding automated assertions for version differences
- Validating retention settings
- Documenting VACUUM behavior and retention considerations
- Registering tables in Unity Catalog and querying versions by table name

### 23. Time Travel Summary

The time travel validation demonstrates that Delta Lake can read previous table versions after transactional changes.

It proves that:

- The product dimension existed before MERGE.
- Delta MERGE updated `PROD-002`.
- Delta MERGE inserted `PROD-005`.
- The table can be queried before and after the MERGE.
- Delta history records table operations.
- Versioned reads support validation and debugging.

This capability helps make the Lakehouse more reliable, explainable, and auditable.