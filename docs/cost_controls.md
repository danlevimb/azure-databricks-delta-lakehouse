# Cost Controls

This document describes the cost-control decisions used in the `azure-databricks-delta-lakehouse` project.

The project was designed as a portfolio-ready MVP using Azure Databricks and ADLS Gen2 while keeping cloud costs controlled and explainable.

### 1. Cost Control Objective

The objective of cost control in this project is to demonstrate Azure Databricks Lakehouse capabilities without creating unnecessary or always-on infrastructure.

The project follows a pay-as-you-go learning model with clear boundaries.

Cost-control priorities:

- Use only the infrastructure needed for the MVP.
- Avoid always-on services.
- Use short-lived interactive compute.
- Keep datasets small.
- Avoid unnecessary SQL Warehouses.
- Avoid scheduled jobs during MVP development.
- Avoid pools during MVP development.
- Keep previous project infrastructure separated.
- Delete or disable unused resources when no longer needed.

This project favors controlled execution over convenience.

### 2. Cost-Aware Architecture

The project uses a dedicated Azure setup.

Main resources:

```text
Project Resource Group
  ├── Azure Databricks Workspace
  └── ADLS Gen2 Storage Account

Databricks Managed Resource Group
  └── Resources managed by Azure Databricks
```

The project intentionally avoids reusing previous Azure Data Factory or Event Hub resources.

#### Resource Separation

A clean resource setup helps with:

- Tracking project-specific costs
- Avoiding accidental changes to previous projects
- Simplifying cleanup
- Producing cleaner documentation
- Making cost ownership easier to explain

### 3. Main Cost Drivers

The main cost drivers for this project are:

| Resource | Cost behavior |
|---|---|
| Azure Databricks compute | Generates cost while running |
| Databricks DBUs | Charged while compute is active |
| Underlying Azure VM | Charged while compute is active |
| ADLS Gen2 storage | Charged based on stored data and operations |
| Managed Databricks networking resources | May exist in the managed resource group |
| SQL Warehouse | Not used in this MVP |
| Jobs / scheduled workflows | Not used in this MVP |
| Event Hubs | Not used in this project |

The most important controllable cost driver is Databricks compute runtime.

### 4. Databricks Compute Strategy

The project uses a small interactive Databricks compute for notebook execution.

Recommended configuration:

| Setting | Value |
|---|---|
| Compute type | Personal Compute |
| Mode | Single node |
| Runtime | Databricks Runtime LTS |
| Workload | Interactive notebook development |
| Auto-termination | 20–30 minutes |
| Pools | Not used |
| SQL Warehouse | Not used |
| Jobs | Not used for MVP |

This configuration is sufficient for the small controlled datasets used in the project.

### 5. Auto-Termination Strategy

Auto-termination is enabled to reduce idle compute cost.

Recommended setting:

```text
20–30 minutes
```

During active development, 30 minutes can be used to avoid frequent restarts.

During lighter usage or after the MVP is complete, 20 minutes is preferred.

#### Recommended Usage

| Situation | Recommended auto-termination |
|---|---:|
| Active notebook development | 30 minutes |
| Light validation work | 20 minutes |
| Documentation-only work | Compute should be terminated |
| After project completion | 20 minutes or delete compute if no longer needed |

Auto-termination is a safety net.

Manual termination is still recommended after each work session.

### 6. Manual Termination Rule

The operating rule for this project is:

```text
If the compute is not actively being used, terminate it manually.
```

This is especially important because Databricks compute can continue generating charges while running or idle before auto-termination occurs.

#### Recommended Session Ending Checklist

Before ending a work session:

```text
1. Confirm notebooks finished running.
2. Save or commit required notebook changes.
3. Push changes to GitHub if needed.
4. Terminate Databricks compute manually.
5. Confirm compute state changed to Terminated.
```

### 7. Compute Startup Trade-Off

The compute can take several minutes to start.

This is expected because Databricks must provision the underlying compute resources and initialize the runtime.

The project accepts this startup latency as a cost-control trade-off.

```text
Lower idle cost → More startup waiting time
Higher convenience → More idle cost risk
```

For this MVP, lower idle cost is preferred.

### 8. Why Pools Are Not Used

Databricks pools can reduce startup time by keeping instances ready for use.

However, pools are not used in this MVP because the project prioritizes cost control over startup speed.

Pools may be useful in production or frequent-development scenarios, but they are unnecessary for this small portfolio project.

#### Decision

```text
No Databricks pools are used during the MVP.
```

#### Rationale

Avoiding pools helps prevent charges from resources kept available only for convenience.

### 9. Why SQL Warehouse Is Not Used

The project does not use a Databricks SQL Warehouse.

The MVP focuses on:

- PySpark notebooks
- Delta Lake path-based tables
- Bronze/Silver/Gold processing
- MERGE
- Time travel
- Data quality validation

A SQL Warehouse is not required for these objectives.

#### Decision

```text
No SQL Warehouse is used during the MVP.
```

#### Rationale

Avoiding SQL Warehouses reduces unnecessary cost and keeps the project focused on Databricks engineering patterns.

### 10. Why Scheduled Jobs Are Not Used

The project does not use scheduled Databricks Jobs.

All notebooks are executed manually during the MVP.

#### Decision

```text
No scheduled workflows are used during the MVP.
```

#### Rationale

The current objective is to demonstrate implementation patterns, not production orchestration.

Scheduled jobs can be added in a future iteration if the project evolves into a production-style workflow.

### 11. Why Serverless Is Not Used in the MVP

Serverless compute can reduce cluster management overhead and startup friction.

However, this MVP continues using Personal Compute because the access pattern is simpler and the project intentionally avoids adding Unity Catalog external locations or a more advanced governance setup.

#### Decision

```text
Use Personal Compute for MVP execution.
```

#### Rationale

Personal Compute keeps the project focused on:

- PySpark
- Delta Lake
- Bronze/Silver/Gold
- SCD Type 2
- MERGE
- Time travel
- Data quality validation

Serverless can be evaluated later as a future improvement.

### 12. ADLS Gen2 Cost Strategy

The project uses small controlled datasets.

ADLS Gen2 costs are expected to remain low because:

- Source CSV files are small.
- Delta tables are small.
- The dataset is generated for demonstration.
- No high-volume ingestion is used.
- No long-term large-scale storage is required.

#### Storage Containers

The project uses these containers:

```text
landing
bronze
silver
gold
metadata
evidence
rejected
```

The main data volume is stored in:

```text
landing
bronze
silver
gold
metadata
```

The `evidence` container is optional and can be avoided if evidence is stored only in the GitHub repository.

### 13. Storage Cleanup Strategy

During development, notebooks overwrite project-specific paths to keep outputs reproducible.

Main project paths:

```text
landing/source_data/
bronze/delta_lakehouse/
silver/delta_lakehouse/
gold/delta_lakehouse/
metadata/delta_lakehouse/
```

If cost cleanup is needed, nonessential development outputs can be deleted after evidence is captured.

#### Do Not Delete Before Evidence

Do not delete outputs until required screenshots and validation artifacts have been captured.

Recommended order:

```text
1. Run full pipeline.
2. Capture required evidence.
3. Commit evidence to GitHub.
4. Review documentation links.
5. Then clean up optional cloud outputs if desired.
```

### 14. Previous Project Resource Cleanup

This Databricks project is separate from previous Azure projects.

Previous project resources such as Event Hubs, old Function App resources, or unused storage accounts should be reviewed independently.

#### Cleanup Principle

```text
Delete only resources that are no longer needed and are not part of the current Databricks project.
```

Before deleting a resource group, verify its contents.

Recommended check:

```text
Resource Group → Review all resources → Confirm ownership → Delete only if safe
```

#### Resources Not to Delete During This Project

Do not delete:

```text
Current Databricks workspace
Current project storage account
Current project resource group
Databricks managed resource group
```

The Databricks managed resource group contains resources required by the workspace.

Deleting individual resources from it can break Databricks.

### 15. Event Hubs Cost Decision

Event Hubs are not used in this project.

Previous Event Hub resources from earlier projects should be disabled or deleted if no longer needed.

#### Decision

```text
No Event Hubs are used in the Databricks Delta Lakehouse MVP.
```

#### Rationale

The project focuses on batch-style Lakehouse processing using generated CSV source data and Delta tables.

Event streaming is outside the MVP scope.

### 16. Databricks Managed Resource Group

Azure Databricks creates a managed resource group for workspace infrastructure.

This group may contain resources such as:

```text
Managed identity
Storage resources
Networking resources
Public IP or NAT-related resources
```

These resources are managed by Databricks.

#### Important Rule

```text
Do not manually delete individual resources inside the Databricks managed resource group.
```

If the Databricks workspace is deleted, Azure handles the associated managed resources according to the workspace deletion process.

### 17. Budget and Alert Recommendation

A budget or cost alert is recommended for the project.

Recommended budget strategy:

| Scope | Recommendation |
|---|---|
| Subscription | Useful for broad safety |
| Resource Group | Useful for project-specific tracking |
| Databricks project RG | Preferred for this project |

Recommended alert thresholds:

```text
50%
80%
100%
```

A budget alert does not automatically stop resources.

It provides notification visibility when spending reaches defined thresholds.

### 18. Recommended Monthly Budget Range

For a small learning and portfolio project, a conservative budget can be used.

Suggested initial range:

```text
$500 MXN to $1,000 MXN
```

The exact value depends on learning pace, compute usage, and how often notebooks are executed.

This budget is not a technical requirement.

It is a financial guardrail.

### 19. Cost Evidence

Recommended cost-control evidence is defined in:

```text
docs/evidence_index.md
```

Relevant evidence folder:

```text
evidence/10_cost_controls/
```

Recommended screenshots:

| Evidence file | Purpose |
|---|---|
| `01_compute_auto_termination.png` | Shows auto-termination configuration |
| `02_single_node_compute.png` | Shows single-node Personal Compute |
| `03_resource_group_scope.png` | Shows project-specific resource group |
| `04_cost_budget_or_alerts.png` | Shows budget or alert if configured |
| `05_compute_terminated_after_session.png` | Shows compute terminated after use |

### 20. Cost-Safe Development Workflow

Recommended workflow:

```text
Start compute only when needed.
Run notebooks.
Validate outputs.
Commit changes.
Terminate compute.
Continue documentation in VS Code without compute running.
```

This separates compute-heavy work from documentation work.

#### Documentation Work

Markdown documentation does not require Databricks compute.

Recommended documentation workflow:

```text
Edit Markdown in VS Code.
Preview locally.
Commit changes.
Push to GitHub.
Pull in Databricks only if notebook context is needed.
```

### 21. Resource Cleanup Checklist

Use this checklist before deleting Azure resources.

| Check | Status |
|---|---|
| Is this resource part of the current Databricks project? | Pending |
| Is this resource required by the Databricks workspace? | Pending |
| Is this resource in the managed Databricks resource group? | Pending |
| Has evidence already been captured? | Pending |
| Has the resource been documented? | Pending |
| Is there any data that needs to be preserved? | Pending |
| Will deleting this break a notebook or validation? | Pending |
| Is this from a previous project and no longer needed? | Pending |

Only delete resources after confirming they are not needed.

### 22. Safe Cleanup Candidates

Potential cleanup candidates include:

```text
Old Event Hub namespaces from previous projects
Old Function App resource groups no longer used
Old storage accounts from previous projects
Temporary test files
Unneeded validation rerun folders
Unused screenshots
```

These should be reviewed before deletion.

### 23. Resources to Keep Until Project Closeout

Keep these resources until documentation and evidence are complete:

```text
Databricks workspace
Databricks compute
Project storage account
ADLS containers
Bronze Delta outputs
Silver Delta outputs
Gold Delta outputs
Metadata validation reports
GitHub repository
Local repository
Databricks Git Folder
```

After final evidence and documentation are complete, the Azure resources can be deleted if the project no longer needs live execution.

### 24. Final Project Cleanup Option

After the project is fully documented and evidence is captured, there are two options.

#### Option A: Keep Minimal Infrastructure

Keep:

```text
Databricks workspace
Storage account
Databricks Git Folder
```

Terminate compute when not in use.

This allows future demos or additional validation runs.

#### Option B: Delete Azure Infrastructure

Delete the project Azure resources after capturing evidence.

Keep:

```text
GitHub repo
Local repo
Documentation
Screenshots
Exported notebooks
Evidence files
```

This minimizes ongoing cloud cost.

The decision depends on whether the project needs to remain executable live.

### 25. Cost Control Decisions

| Decision | Rationale |
|---|---|
| Use a dedicated resource group | Easier cost tracking and cleanup |
| Use small sample data | Reduces storage and compute time |
| Use Personal Compute | Simpler for interactive development |
| Use single-node compute | Enough for MVP dataset |
| Enable auto-termination | Reduces idle compute cost |
| Terminate compute manually | Prevents unnecessary runtime |
| Avoid pools | Avoids idle capacity cost |
| Avoid SQL Warehouse | Not required for MVP |
| Avoid scheduled jobs | Prevents unexpected recurring runs |
| Avoid Event Hubs | Streaming is out of scope |
| Avoid Serverless for MVP | Keeps access model simple |
| Keep docs work local | Avoids running compute for Markdown editing |

### 26. Cost Control Limitations

This document describes project-level cost controls, but it does not guarantee a fixed Azure bill.

Actual cost depends on:

- Compute runtime
- Region
- VM availability
- Databricks pricing tier
- Storage usage
- Number of notebook executions
- Network and managed resource behavior
- Whether resources are left running

The safest practice is to monitor Azure Cost Management regularly during active development.

### 27. Future Cost Improvements

Possible future improvements include:

- Creating project-specific budget alerts
- Automating compute shutdown reminders
- Adding a cleanup script for test output paths
- Moving to job clusters for scheduled execution if needed
- Evaluating Serverless with Unity Catalog external locations
- Reviewing storage lifecycle policies
- Adding cost tags consistently across resources
- Adding a cost summary section to the project README
- Deleting Azure infrastructure after final portfolio evidence is captured

### 28. Cost Control Summary

The project is designed to demonstrate Azure Databricks Lakehouse capabilities while keeping costs controlled.

Main cost-control practices:

- Use only required resources.
- Keep compute small.
- Use auto-termination.
- Manually terminate compute.
- Avoid always-on services.
- Avoid scheduled workloads.
- Avoid unnecessary SQL Warehouses.
- Avoid pools during MVP development.
- Keep datasets small.
- Clean up unused previous project resources.
- Monitor Azure cost regularly.

This approach keeps the MVP technically useful while reducing the risk of unexpected cloud spending.