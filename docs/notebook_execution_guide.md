# Notebook Execution Guide

This document explains how to execute the notebooks for the `azure-databricks-delta-lakehouse` project.

The goal is to provide a clear, reproducible execution sequence for the Lakehouse pipeline built with Azure Databricks, PySpark, ADLS Gen2, and Delta Lake.

### 1. Purpose

This project demonstrates a portfolio-oriented Azure Databricks Delta Lakehouse implementation.

The pipeline processes sample retail order data through the following layers:

```text
Landing → Bronze → Silver → Gold
```
The implementation demonstrates:

* Azure Databricks workspace usage
* PySpark notebooks
* ADLS Gen2 storage access
* Delta Lake path-based tables
* Bronze, Silver, and Gold Lakehouse architecture
* Data quality validation
* Rejected records handling
* SCD Type 2 dimensional history
* Fact and aggregate tables
* Delta MERGE / upsert
* Delta time travel
* Final validation reporting

### 2. Execution Environment

The notebooks were developed and validated using:

|Component | Value |
|----------|-------|
|Cloud provider | Microsoft Azure |
|Compute platform | Azure Databricks |
|Storage | Azure Data Lake Storage Gen2 |
|Storage access pattern | ABFSS paths |
|Processing engine | Apache Spark / PySpark |
|Table format | Delta Lake |
|Development mode | Databricks Git Folder |
|Repository | `azure-databricks-delta-lakehouse` |

### 3. Azure Resources Used

The project uses a clean Azure resource setup dedicated to this Databricks Lakehouse implementation.

| Resource | Purpose |
|----------|---------|
| Azure Databricks Workspace | Notebook execution and Spark processing |
| ADLS Gen2 Storage Account | Lakehouse storage |
| `landing` container | Source files |
| `bronze` container | Raw Delta tables |
| `silver` container | Clean and rejected Delta tables |
| `gold` container | Analytical model Delta tables |
| `metadata` container | Validation reports |
| Databricks Secret Scope | Secure storage credential reference |
| Databricks Personal Compute | Interactive development and execution |

### 4. Secure ADLS Gen2 Access

Storage credentials are not committed to this repository.

ADLS Gen2 access was configured at the Databricks compute level using a Databricks-backed secret scope and Spark configuration.

The notebooks assume that the compute already has access to the storage account.

No storage account keys, connection strings, or secrets should be stored in the notebooks or committed to Git.

### 5. Compute Configuration

The project was validated using a small single-node Databricks compute.

Recommended development configuration:

| Setting | Recommended value |
|---------|-------------------|
| Compute type | Personal Compute |
| Mode | Single node |
| Runtime | Databricks Runtime LTS |
| Auto-termination | 20–30 minutes |
| Workload type | Interactive notebooks |
| Pools | Not used |
| SQL Warehouse | Not used |
| Jobs / scheduled workflows | Not used for MVP |

This setup favors cost control over startup speed.

## 6. Notebook Execution Order

Run the notebooks in the following order:

```text
00_environment_setup
01_generate_sample_data
02_bronze_ingestion
03_silver_transformations
04_gold_dimensions
05_gold_facts_and_aggregates
06_delta_merge_upsert_demo
07_time_travel_validation
08_data_quality_validation
```

Each notebook depends on outputs from previous notebooks.

### 7. Notebook Summary

| Order| Notebook | Purpose |
|------|----------|---------|
| 00 | `00_environment_setup` | Validates environment paths and ADLS Gen2 access |
| 01 | `01_generate_sample_data`	 | Generates controlled sample source data in the landing container |
| 02 | `02_bronze_ingestion` | Reads source CSV files and writes Bronze Delta tables |
| 03 | `03_silver_transformations` | Cleans, validates, types, and separates rejected records |
| 04 | `04_gold_dimensions` | Builds Gold dimensions, including SCD Type 2 customer history |
| 05 | `05_gold_facts_and_aggregates` | Builds fact and analytical summary tables |
| 06 | `06_delta_merge_upsert_demo` | Demonstrates Delta MERGE / upsert on product dimension |
| 07 | `07_time_travel_validation` | Validates Delta time travel before and after MERGE |
| 08 | `08_data_quality_validation` | Runs final validation checks and persists a validation report |

### 8. Expected Lakehouse Outputs

After successful execution, the following Delta tables should exist.

#### Bronze
```text
bronze/delta_lakehouse/bronze_customers
bronze/delta_lakehouse/bronze_products
bronze/delta_lakehouse/bronze_orders
bronze/delta_lakehouse/bronze_order_items
```

#### Silver
```
silver/delta_lakehouse/silver_customers_clean
silver/delta_lakehouse/silver_products_clean
silver/delta_lakehouse/silver_orders_clean
silver/delta_lakehouse/silver_order_items_clean
silver/delta_lakehouse/silver_rejected_records
```

#### Gold
```text
gold/delta_lakehouse/gold_dim_customer_scd2
gold/delta_lakehouse/gold_dim_product
gold/delta_lakehouse/gold_fact_orders
gold/delta_lakehouse/gold_daily_sales_summary
gold/delta_lakehouse/gold_customer_sales_summary
```

#### Metadata
```text
metadata/delta_lakehouse/validation_reports/
```

### 9. Expected Row Counts

The final validation notebook checks expected table row counts.

| Table | Expected row count |
|-------|--------------------|
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

### 10. Important Execution Notes
#### Re-running the pipeline

The notebooks are designed for MVP demonstration and can be re-run during development.

Some notebooks overwrite their target Delta paths to keep the environment clean and reproducible.

#### MERGE notebook

The `06_delta_merge_upsert_demo` notebook demonstrates an update and insert using Delta MERGE:

* `PROD-002` is updated.
* `PROD-005` is inserted.

Re-running this notebook should not create duplicate product IDs if the MERGE logic is preserved.

### Time travel notebook

The `07_time_travel_validation` notebook validates Delta table versions before and after the MERGE operation.

The exact latest version number may change if the MERGE notebook is executed multiple times.

The important validation is:

```text
Before MERGE: 4 products
After MERGE: 5 products
```

### Final validation notebook

The `08_data_quality_validation` notebook consolidates validation checks into a Delta-based validation report.

The expected result is:

```text
PASS for all validations
```

### 11. Cost Control Notes

This project uses interactive Databricks compute only when needed.

Recommended cost-control practices:

* Start compute only during active work.
* Use auto-termination.
* Manually terminate compute after each work session.
* Avoid unnecessary SQL Warehouses.
* Avoid scheduled jobs during MVP development.
* Avoid pools unless startup latency becomes a justified concern.
* Keep sample datasets small.

### 12. Troubleshooting Notes

#### ADLS access fails

If the notebook cannot list or read ADLS Gen2 paths, check:

* The Databricks compute is running.
* The storage account name is correct.
* The secret scope exists.
* The secret value is current.
* The Spark configuration is set at the compute level.
* The compute was restarted after Spark config or secret changes.

#### Git folder does not show latest changes

If Databricks does not show the latest repository files:

```text
Databricks Git Folder → Pull
```

If local VS Code does not show latest Databricks changes:

```bash
git pull
```
#### Push is blocked by GitHub secret scanning

If GitHub blocks a push due to detected secrets:

* Do not bypass the warning.
* Remove the secret from files.
* Remove the secret from local commit history if needed.
* Rotate the exposed secret.
* Push only sanitized commits.

### 13. Execution Completion Criteria

The notebook execution phase is considered successful when:

All notebooks run from the Databricks Git Folder.
ADLS Gen2 paths are accessible without notebook-embedded secrets.
Bronze Delta tables are created.
Silver clean and rejected tables are created.
Gold dimensions, facts, and summaries are created.
Delta MERGE completes successfully.
Time travel validation confirms before/after table states.
Final validation report shows all checks as PASS.