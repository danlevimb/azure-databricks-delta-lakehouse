# Future Improvements

This document describes possible future improvements for the `azure-databricks-delta-lakehouse` project.

The current implementation is a portfolio-ready MVP focused on Azure Databricks, PySpark, Delta Lake, Bronze/Silver/Gold processing, SCD Type 2, Delta MERGE, time travel, and data quality validation.

The improvements listed here are not required for the MVP, but they represent realistic ways to evolve the project into a more production-like Lakehouse platform.

### 1. Future Improvements Objective

The objective of this document is to define a clear roadmap for possible enhancements beyond the current MVP.

The current project demonstrates core Lakehouse engineering patterns.

Future improvements could add:

- Production orchestration
- Governance
- CI/CD
- Automated testing
- More advanced data quality
- Larger-scale processing
- Streaming ingestion
- Operational monitoring
- Cost optimization
- BI consumption

This document helps separate current scope from future evolution.

### 2. Improvement Categories

Potential improvements are grouped into the following categories:

| Category | Description |
|---|---|
| Orchestration | Automating notebook execution |
| Governance | Adding Unity Catalog and access controls |
| Security | Improving secret and identity management |
| CI/CD | Automating deployment and validation |
| Data quality | Making validation configurable and reusable |
| Incremental processing | Extending MERGE and change handling |
| Performance | Optimizing Delta tables and Spark execution |
| Monitoring | Adding operational visibility |
| BI consumption | Connecting Gold outputs to analytical tools |
| Cost management | Improving cloud cost visibility and cleanup |

### 3. Databricks Jobs Orchestration

The current MVP executes notebooks manually from the Databricks Git Folder.

A natural future improvement is to orchestrate the notebooks using Databricks Jobs.

#### Proposed Enhancement

Create a Databricks Job with task dependencies:

```text
01_generate_sample_data
      ↓
02_bronze_ingestion
      ↓
03_silver_transformations
      ↓
04_gold_dimensions
      ↓
05_gold_facts_and_aggregates
      ↓
06_delta_merge_upsert_demo
      ↓
07_time_travel_validation
      ↓
08_data_quality_validation
```

#### Value

This would demonstrate:

- Workflow orchestration
- Task dependencies
- Job clusters
- Retry configuration
- Scheduled execution
- Production-style pipeline management

#### Portfolio Benefit

This would show that the project can move from interactive development to automated execution.

### 4. Job Cluster Strategy

The current MVP uses interactive Personal Compute.

A production-style workflow could use job clusters instead.

#### Proposed Enhancement

Use ephemeral job clusters for workflow execution.

```text
Job starts → cluster starts → notebooks run → cluster terminates
```

#### Value

This can improve cost control by ensuring compute exists only during job execution.

#### Consideration

Job clusters may add configuration complexity and should be introduced only after the notebook pipeline is stable.

### 5. Unity Catalog Integration

The current MVP uses path-based Delta tables.

A future version could register tables in Unity Catalog.

#### Proposed Enhancement

Introduce a Unity Catalog structure such as:

```text
catalog: portfolio_dev
schema: databricks_delta_lakehouse
tables:
  bronze_customers
  silver_customers_clean
  gold_fact_orders
```

#### Value

Unity Catalog would add:

- Table registration
- Centralized governance
- Access control
- Data lineage capabilities
- Catalog-based SQL access
- Better production alignment

#### Consideration

Unity Catalog would expand the project into governance architecture, so it is intentionally not part of the MVP.

### 6. External Locations and Storage Credentials

The current MVP configures ADLS access through compute-level Spark configuration and a Databricks-backed secret scope.

A more production-oriented approach would use Unity Catalog external locations.

#### Proposed Enhancement

Create:

```text
Storage credential
External location
Catalog/schema/table grants
```

#### Value

This would improve:

- Governance
- Security
- Access management
- Serverless compatibility
- Production-readiness

#### Consideration

This should be added together with a broader Unity Catalog design.

### 7. Azure Key Vault-Backed Secret Scope

The current MVP uses a Databricks-backed secret scope.

A future version could use Azure Key Vault-backed secrets.

#### Proposed Enhancement

Store sensitive values in Azure Key Vault and reference them from Databricks.

#### Value

This would improve:

- Secret centralization
- Rotation management
- Azure-native security posture
- Separation between Databricks and secret storage

#### Consideration

For the MVP, the current Databricks-backed secret scope is acceptable because it keeps secrets out of notebooks and GitHub.

### 8. Managed Identity or Service Principal Access

The current MVP uses storage key-based access configured securely outside notebooks.

A future version could use identity-based authentication.

#### Proposed Enhancement

Use one of the following patterns:

```text
Managed identity
Service principal
Unity Catalog storage credential
External location
```

#### Value

This would reduce reliance on storage account keys and align better with production security practices.

#### Consideration

Identity-based access should be implemented together with governance and access control design.

### 9. CI/CD with Databricks Asset Bundles

The current project uses GitHub and Databricks Git Folder integration.

A future version could add CI/CD.

#### Proposed Enhancement

Use Databricks Asset Bundles or another deployment approach to package and deploy notebooks, jobs, and configuration.

#### Value

This would demonstrate:

- Deployment automation
- Environment configuration
- Repeatable job deployment
- Dev/test/prod promotion patterns
- Better engineering maturity

#### Consideration

CI/CD should be added after the project structure, notebooks, and documentation are stable.

### 10. GitHub Actions Validation

The current project does not run automated checks on pull requests.

A future version could add GitHub Actions.

#### Proposed Enhancement

Add workflows for:

```text
Markdown linting
Secret scanning checks
Python formatting
Notebook syntax validation
Basic repository structure validation
```

#### Value

This would improve repository quality and reduce manual review.

#### Consideration

Because Databricks notebooks depend on cloud execution, full pipeline validation would still require Databricks-side execution.

### 11. Automated Testing

The current project uses a final validation notebook but does not include a formal automated test suite.

#### Proposed Enhancement

Add automated tests for:

- Transformation helpers
- Data quality rules
- SCD2 logic
- MERGE logic
- Expected row counts
- Duplicate prevention
- Revenue consistency

Possible tools:

```text
pytest
Databricks notebooks as tests
Databricks Jobs test workflow
```

#### Value

This would make the project stronger from a software engineering perspective.

#### Consideration

Testing should be introduced gradually to avoid overcomplicating the MVP.

### 12. Config-Driven Data Quality Rules

The current MVP defines validation rules directly in PySpark code.

A future version could externalize rules into configuration.

#### Proposed Enhancement

Create a configuration table or file defining rules such as:

```text
entity_name
column_name
rule_type
allowed_values
is_required
severity
```

#### Value

This would make quality rules easier to maintain and extend.

#### Example

Instead of hardcoding allowed order statuses, the rule could come from a configuration table.

```text
orders.order_status allowed values: CREATED, PAID, COMPLETED, CANCELLED, REFUNDED
```

#### Consideration

This would make the project more flexible but also more complex.

### 13. Multi-Reason Rejected Records

The current MVP captures the first rejection reason per invalid record.

A future version could capture multiple validation failures per record.

#### Proposed Enhancement

Store rejected records with either:

```text
Array of rejection reasons
```

or one row per failed rule:

```text
record_id
entity_name
reject_reason
rule_name
```

#### Value

This would improve diagnostic detail.

#### Example

An order item could fail because:

```text
parent order is invalid
quantity is negative
line total is invalid
```

Instead of recording only the first reason, all reasons could be captured.

### 14. Rejected Records Remediation Workflow

The current MVP records rejected records but does not implement remediation.

#### Proposed Enhancement

Add a remediation process such as:

```text
Rejected records review
Corrected records landing area
Reprocessing notebook
Rejected-to-clean recovery flow
```

#### Value

This would demonstrate a more complete data quality lifecycle.

#### Consideration

This is useful for production-style pipelines but is beyond the MVP scope.

### 15. Incremental Bronze Ingestion

The current Bronze process reads controlled batches and overwrites outputs during development reruns.

A future version could implement incremental file ingestion.

#### Proposed Enhancement

Track processed files using metadata.

Example metadata fields:

```text
source_file_path
file_modification_time
processed_at
record_count
processing_status
```

#### Value

This would prevent reprocessing files unnecessarily and make Bronze more production-like.

### 16. Auto Loader

The current project does not use Auto Loader.

A future version could use Auto Loader for file discovery and ingestion.

#### Proposed Enhancement

Use Auto Loader to incrementally process files arriving in the landing zone.

#### Value

Auto Loader would demonstrate:

- Incremental file discovery
- Scalable ingestion
- Schema inference
- Schema evolution handling
- Checkpointing

#### Consideration

Auto Loader is valuable, but it would shift the project toward ingestion architecture. The current MVP focuses on transformation and Lakehouse modeling.

### 17. Streaming Ingestion

The current project is batch-oriented.

A future version could add streaming ingestion.

#### Proposed Enhancement

Ingest events from:

```text
Event Hubs
Kafka
Streaming file source
```

Then process streaming Bronze data into Silver and Gold.

#### Value

This would demonstrate real-time or near-real-time data engineering.

#### Consideration

Streaming introduces operational complexity and cost. It should be added only if it supports a specific learning or portfolio goal.

### 18. Product SCD Type 2

The current project models customers as SCD Type 2 and products as current-state.

A future version could implement product SCD Type 2.

#### Proposed Enhancement

Track product changes over time, such as:

```text
Product price changes
Product category changes
Product active status changes
```

#### Value

This would demonstrate multiple dimensional history patterns.

#### Consideration

The MVP intentionally keeps product history simple so that products can be used for the MERGE demonstration.

### 19. Hybrid SCD Type 1 and Type 2

The current customer dimension implements Type 2 behavior only.

A future version could support hybrid SCD behavior.

#### Proposed Enhancement

Define some attributes as Type 1 and others as Type 2.

Example:

```text
email → Type 1 overwrite
city → Type 2 history
customer_segment → Type 2 history
```

#### Value

Hybrid SCD behavior is common in production data warehouses.

#### Consideration

This would require a more sophisticated dimension framework.

### 20. Late-Arriving Dimension Handling

The current MVP assumes customer changes arrive in effective timestamp order.

A future version could support late-arriving dimension updates.

#### Proposed Enhancement

Handle records that arrive after later versions already exist.

This would require recalculating effective date windows.

#### Value

Late-arriving data is a realistic production challenge.

#### Consideration

This would significantly increase SCD2 complexity.

### 21. Change Data Capture Patterns

The current project does not implement CDC.

A future version could add CDC-style source changes.

#### Proposed Enhancement

Introduce change operation fields such as:

```text
insert
update
delete
```

or:

```text
operation_type
operation_timestamp
```

#### Value

CDC patterns would make the pipeline more realistic for operational source systems.

#### Consideration

CDC would require new source design and additional merge/delete logic.

### 22. Delete and Soft Delete Handling

The current MERGE example handles updates and inserts only.

A future version could handle deletes.

#### Proposed Enhancement

Support:

```text
Hard delete
Soft delete
Deactivate record
Mark as deleted
```

#### Value

This would make the Delta MERGE example more complete.

#### Consideration

Delete behavior should be carefully documented because it affects history and analytical consistency.

### 23. Table Optimization

The current MVP does not optimize Delta table layout.

A future version could add table optimization patterns.

#### Proposed Enhancement

Evaluate:

```text
OPTIMIZE
ZORDER
Liquid clustering
Compaction
Partition strategy review
```

#### Value

This would demonstrate performance tuning and storage layout awareness.

#### Consideration

The current dataset is too small to require meaningful optimization. This improvement would be more useful with larger data.

### 24. Larger Dataset Generation

The current dataset is intentionally small.

A future version could generate larger synthetic datasets.

#### Proposed Enhancement

Add configurable row counts for:

```text
customers
products
orders
order_items
```

#### Value

A larger dataset would allow performance and partitioning experiments.

#### Consideration

This would increase compute and storage cost.

### 25. Performance Benchmarking

The current project does not benchmark performance.

A future version could measure runtime and table sizes.

#### Proposed Enhancement

Capture metrics such as:

```text
Notebook runtime
Input row counts
Output row counts
File counts
Table sizes
Partition counts
Shuffle behavior
```

#### Value

This would demonstrate performance awareness.

#### Consideration

Benchmarking should be cost-controlled.

### 26. Operational Metadata Model

The current project stores final validation reports but does not implement a full operational metadata model.

#### Proposed Enhancement

Create metadata tables such as:

```text
pipeline_run
notebook_run
table_load_summary
data_quality_summary
processed_files
error_log
```

#### Value

This would make the pipeline more observable and production-like.

### 27. Monitoring and Alerting

The current project does not implement production monitoring.

#### Proposed Enhancement

Add monitoring using:

```text
Databricks job alerts
Azure Monitor
Log Analytics
Email or Teams notifications
Cost alerts
Data quality failure alerts
```

#### Value

This would support production operations.

#### Consideration

Monitoring becomes most valuable after jobs and orchestration are added.

### 28. BI Consumption Layer

The current project produces Gold tables but does not connect to BI.

#### Proposed Enhancement

Add a BI consumption layer using:

```text
Power BI
Databricks SQL
Dashboards
Semantic model
```

#### Value

This would demonstrate end-to-end analytical consumption.

#### Consideration

The project is currently focused on Data Engineering. BI can be added as a separate phase.

### 29. Databricks SQL Warehouse

The current MVP does not use SQL Warehouse.

A future version could add Databricks SQL for querying Gold tables.

#### Proposed Enhancement

Expose Gold tables through Databricks SQL.

Potential use cases:

```text
Daily sales dashboard
Customer sales analysis
Revenue by currency
Order status breakdown
```

#### Value

This would show the serving layer from an analytics perspective.

#### Consideration

SQL Warehouse introduces additional cost and should be enabled only when needed.

### 30. Power BI Dashboard

The project could be extended with a Power BI dashboard.

#### Proposed Enhancement

Create dashboard pages such as:

```text
Daily revenue
Orders by status
Customer sales
Revenue by currency
Product sales
Rejected records overview
```

#### Value

This would make the Gold outputs easier to demonstrate visually.

#### Consideration

Power BI is not required for the current Data Engineering MVP.

### 31. Documentation Improvements

The repository can continue improving its documentation.

Possible additions:

```text
docs/code_walkthrough_public/
docs/faq.md
docs/troubleshooting.md
docs/design_decisions.md
docs/interview_defense_private_reference.md
```

#### Public Documentation

Public documentation should remain technical and recruiter-friendly.

#### Private Study Documentation

Deep interview notes and notebook-by-notebook explanations can live in the private roadmap repo.

### 32. Code Deep Dive Phase

A planned improvement is a code understanding and interview defense phase.

#### Proposed Enhancement

Create private deep-dive notes for each notebook.

Each note can explain:

- What problem the notebook solves
- What inputs it reads
- What outputs it writes
- What is Python logic
- What is PySpark logic
- What is Delta Lake logic
- What patterns it demonstrates
- How to explain it in an interview
- What could be improved

#### Value

This converts the project from something that only runs into something that can be confidently explained and defended.

### 33. Repository Quality Improvements

Future repository improvements could include:

- Consistent markdown linting
- Link checks
- Image compression
- Evidence naming validation
- Mermaid diagrams
- Architecture diagrams with Azure icons
- Better README navigation
- Documentation badges
- Table of contents improvements

These are polish items that can improve recruiter readability.

### 34. Cost Management Improvements

Future cost improvements could include:

- Azure budget alert for the project resource group
- Cost tags on all resources
- Cleanup script for generated ADLS paths
- Compute usage checklist
- Optional teardown guide
- Monthly cost review note
- Documentation of minimal resources needed to rerun the project

These improvements would make the project more operationally mature.

### 35. Final Cleanup and Teardown Guide

After evidence and documentation are complete, a teardown guide could be created.

#### Proposed Enhancement

Create:

```text
docs/teardown_guide.md
```

The guide could explain:

- What can be deleted
- What should not be deleted before evidence capture
- How to delete the project resource group
- How to avoid deleting unrelated resources
- How to preserve GitHub evidence
- How to verify no active compute remains

#### Value

This would reinforce cost-aware Azure usage.

### 36. Future Improvement Priority

Not all improvements should be implemented immediately.

Recommended priority:

| Priority | Improvement |
|---|---|
| High | Databricks Jobs orchestration |
| High | Private code deep dive and interview defense |
| High | Evidence capture and README polish |
| Medium | Unity Catalog design |
| Medium | Databricks Asset Bundles |
| Medium | Azure Key Vault-backed secrets |
| Medium | Config-driven data quality |
| Medium | Operational metadata model |
| Low | Streaming ingestion |
| Low | Large-scale performance benchmarking |
| Low | Power BI dashboard |
| Low | Product SCD Type 2 |

### 37. Future Improvements Summary

The current MVP already demonstrates the main technical patterns required for a strong Azure Databricks Delta Lakehouse portfolio project.

Future improvements could evolve the project toward:

- Production orchestration
- Stronger governance
- Better automation
- Advanced data quality
- Incremental ingestion
- More robust security
- Larger-scale performance testing
- Monitoring and alerting
- BI consumption
- Interview-ready code explanation

These improvements are intentionally separated from the MVP so the current project remains focused, cost-aware, and technically defensible.