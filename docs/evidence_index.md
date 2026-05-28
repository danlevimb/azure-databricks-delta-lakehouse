# Evidence Index

This document defines the evidence package for the `azure-databricks-delta-lakehouse` project.

The purpose of this evidence index is to organize screenshots and validation artifacts that demonstrate the project was implemented, executed, validated, and documented in Azure Databricks.

### Evidence Strategy

The main objective is to support the main technical claims made in the repository.

This evidence demonstrate:

- Azure Databricks workspace and compute setup
- Secure ADLS Gen2 access without notebook-embedded secrets
- Source data generation
- Bronze Delta table creation
- Silver data quality validation
- Rejected records handling
- Gold dimensional model
- SCD Type 2 customer history
- Gold fact and aggregate tables
- Delta MERGE / upsert
- Delta time travel validation
- Final data quality validation report
- Cost-control configuration

### Folder Structure

Evidence is organized under the `evidence/` directory with the followign structure:

| Folder | Purpose |
|--------|---------|
| `01_environment_setup/` | Workspace, compute, Git Folder, and ADLS access validation |
| `02_source_data_generation/` | Source files generated in the landing container |
| `03_bronze_layer/` | Bronze Delta tables and Delta history |
| `04_silver_layer/` | Silver clean tables and rejected records |
| `05_gold_dimensions/` | Customer SCD2 and product dimension |
| `06_gold_facts/` | Fact table and analytical summaries |
| `07_delta_merge/` | Delta MERGE / upsert validation |
| `08_time_travel/` | Versioned reads and before/after comparison |
| `09_data_quality_validation/` | Final validation report |
| `10_cost_controls/` | Compute auto-termination and cost-control settings |

---

### 01. Environment Setup

This evidence demonstrate that the project environment was created and that the notebooks can run from the Databricks Git Folder.

| File | Description |
|---|---|
| `01_databricks_workspace_created.png` | Azure Databricks workspace overview or confirmation | 
| `02_compute_configuration.png` | Databricks compute configuration showing runtime, single-node mode, and auto-termination |
| `03_git_folder_connected.png` | Databricks Git Folder connected to the GitHub repository |
| `04_environment_setup_success.png` | `00_environment_setup` notebook successfully listing ADLS source data |
| `05_adls_containers.png` | ADLS Gen2 containers used by the project |

#### This evidence proves:

- The project uses Azure Databricks.
- The notebooks are executed from a Git-integrated Databricks folder.
- The compute can access ADLS Gen2 without storage keys inside notebooks.
- The environment is ready to execute the Lakehouse pipeline.

### 02. Source Data Generation

This evidence demonstrate that the controlled source files were generated in the `landing` container.

| File | Description |
|------|-------------|
| `01_source_data_folders.png` | ADLS or Databricks listing showing `batch_001`, `batch_002`, and `batch_003_schema_evolution` |
| `02_batch_001_files.png` | Source files generated for `batch_001` |
| `03_generate_sample_data_success.png` | Successful output of `01_generate_sample_data` notebook |
| `04_sample_customers_preview.png` | Preview of generated customer source data |

#### This evidence proves:

- The sample dataset was generated successfully.
- The landing zone contains multiple source batches.
- The project has controlled input data for Bronze, Silver, and Gold processing.

### 03. Bronze Layer

This evidence demonstrate that raw source files were converted into Bronze Delta tables with technical metadata.

| File | Description |
|------|-------------|
| `01_bronze_ingestion_summary.png` | Bronze ingestion summary by entity and batch |
| `02_bronze_customers_schema_evolution.png` | Bronze customers showing `loyalty_tier` preserved |
| `03_bronze_table_folders.png` | ADLS or Databricks listing of Bronze Delta table folders |
| `04_bronze_delta_history.png` | Delta history for at least one Bronze table |

#### This evidence proves:

This evidence proves:

- Source CSV files were ingested into Delta tables.
- Bronze preserves source data with traceability.
- Technical metadata was added.
- Schema evolution was preserved in Bronze.
- Delta transaction logs exist for Bronze outputs.

### 04. Silver Layer

This evidence demonstrate that Silver transformations created clean records and rejected invalid records.

| File | Description |
|------|-------------|
| `01_silver_summary.png` | Silver table row-count summary |
| `02_rejected_records_summary.png` | Rejected records grouped by entity and reason |
| `03_silver_orders_clean.png` | Valid Silver orders preview |
| `04_silver_order_items_clean.png` | Valid Silver order items preview |
| `05_silver_delta_history.png` | Delta history for Silver outputs |

#### This evidence proves:

- Bronze data was transformed into typed and validated Silver tables.
- Invalid records were separated into `silver_rejected_records`.
- Referential validation worked.
- Business validation rules were applied.

### 05. Gold Dimensions

This evidence demonstrate that the project created analytical dimensions, including SCD Type 2 customer history.

| File | Description |
|------|-------------|
| `01_gold_customer_scd2_versions.png` | Customer SCD2 versions for customers with historical changes |
| `02_gold_customer_current_validation.png` | Validation showing one current record per customer |
| `03_gold_product_dimension.png` | Product dimension before or after MERGE |
| `04_gold_dimensions_summary.png` | Gold dimensions row-count summary |
| `05_gold_dimensions_delta_history.png` | Delta history for Gold dimension tables |

#### This evidence proves:

This evidence proves:

- The project created a Gold customer dimension.
- Customer history is preserved using SCD Type 2.
- Each customer has exactly one current version.
- A product dimension was created for analytical use and MERGE demonstration.

### 06. Gold Facts

This evidence demonstrate that the project created fact and analytical summary tables.

| File | Description |
|------|-------------|
| `01_gold_fact_orders.png` | Fact table preview showing order-level metrics |
| `02_fact_orders_scd2_join.png` | Fact orders joined to historical customer SCD2 version |
| `03_gold_daily_sales_summary.png` | Daily sales summary by date and currency |
| `04_gold_customer_sales_summary.png` | Customer sales summary |
| `05_gold_facts_summary.png` | Gold fact and summary row counts |

#### This evidence proves:

This evidence proves:

- The project created an order-level fact table.
- Order updates were handled using latest valid order state.
- The fact table joins to the correct historical customer version.
- Analytical summaries were created for daily sales and customer sales.
- Gold tables are ready for analytical consumption.

### 07. Delta MERGE

This evidence demonstrate Delta MERGE / upsert behavior.

| File | Description |
|------|-------------|
| `01_product_dimension_before_merge.png` | Product dimension before MERGE |
| `02_merge_source_batch.png` | Source update batch containing `PROD-002` and `PROD-005` |
| `03_product_dimension_after_merge.png` | Product dimension after MERGE |
| `04_merge_row_count_validation.png` | Validation showing 5 total and 5 distinct products |
| `05_delta_history_merge_operation.png` | Delta history showing `MERGE` operation |

#### This evidence proves:

- `PROD-002` was updated.
- `PROD-005` was inserted.
- No duplicate product IDs were created.
- Delta Lake recorded the operation as a `MERGE`.

### 08. Time Travel

This evidence demonstrate that Delta versions can be queried before and after MERGE.

| File | Description |
|------|-------------|
| `01_delta_history_versions.png` | Delta history showing table versions |
| `02_version_count_comparison.png` | Before/after product count comparison |
| `03_affected_products_comparison.png` | Before/after comparison for `PROD-002` and `PROD-005` |
| `04_product_insert_validation.png` | Validation that `PROD-005` did not exist before and exists after MERGE |

#### This evidence proves:

This evidence proves:

- Delta Lake supports versioned reads.
- The product table can be read before and after MERGE.
- `PROD-002` changed from `35.00` to `38.00`.
- `PROD-005` appears after MERGE.
- Delta time travel supports validation and debugging.

### 09. Data Quality Validation

This evidence demonstrate the final project-wide validation report.

| File | Description |
|---|---|
| `01_validation_report.png` | Final validation report showing validation names and statuses |
| `02_validation_status_summary.png` | PASS / FAIL summary |
| `03_validation_report_saved_to_metadata.png` | Confirmation that validation report was written to metadata container |
| `04_metadata_validation_report_folder.png` | ADLS listing of validation report folder |

#### This evidence proves:

- The project was validated after execution.
- Row counts were checked.
- Rejected records were checked.
- SCD2 rules were checked.
- Duplicate prevention was checked.
- MERGE results were checked.
- Delta history was checked.
- Revenue consistency was checked.
- Validation output was persisted.

### 10. Cost Controls

Folder:

`evidence/10_cost_controls/`

This evidence should demonstrate cost-aware development decisions.

#### Recommended Screenshots

| File | Description | Required |
|---|---|---|
| `01_compute_auto_termination.png` | Compute configuration showing auto-termination | Required |
| `02_single_node_compute.png` | Compute configuration showing single-node Personal Compute | Required |
| `03_resource_group_scope.png` | Project-specific Resource Group | Optional |
| `04_cost_budget_or_alerts.png` | Budget or cost alert configuration if created | Optional |
| `05_compute_terminated_after_session.png` | Compute terminated after work session | Optional |

#### What this evidence proves:

This evidence proves:

- The project was built with cost control in mind.
- Interactive compute was not left running unnecessarily.
- No pools or unnecessary SQL Warehouses were used for the MVP.
- The Databricks compute was configured with auto-termination.

### 13. Evidence Capture Checklist

Use this checklist before declaring the evidence package complete.

| Evidence area | Required screenshots complete |
|---|---|
| Environment setup | Pending |
| Source data generation | Pending |
| Bronze layer | Pending |
| Silver layer | Pending |
| Gold dimensions | Pending |
| Gold facts | Pending |
| Delta MERGE | Pending |
| Time travel | Pending |
| Data quality validation | Pending |
| Cost controls | Pending |

### 15. Recommended Evidence Commit Strategy

Evidence should be committed in small logical batches.

Recommended commits:

| Commit | Scope |
|---|---|
| `Add environment setup evidence` | `evidence/01_environment_setup/` |
| `Add source data and Bronze evidence` | `evidence/02_source_data_generation/`, `evidence/03_bronze_layer/` |
| `Add Silver and rejected records evidence` | `evidence/04_silver_layer/` |
| `Add Gold model evidence` | `evidence/05_gold_dimensions/`, `evidence/06_gold_facts/` |
| `Add Delta MERGE and time travel evidence` | `evidence/07_delta_merge/`, `evidence/08_time_travel/` |
| `Add validation and cost control evidence` | `evidence/09_data_quality_validation/`, `evidence/10_cost_controls/` |

### 16. Evidence Completion Criteria

The evidence package is considered complete when:

- All required screenshots are captured.
- Evidence file names match this index.
- Screenshots are stored in the correct folders.
- Screenshots are readable.
- No sensitive data is exposed.
- Documentation links point to the correct evidence files.
- The final validation report shows all checks as `PASS`.
- The repository can be reviewed by a recruiter or technical interviewer without requiring access to the Azure subscription.