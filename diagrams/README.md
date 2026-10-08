# Visual Package

This directory contains the visual layer for the Azure Databricks Delta Lakehouse project.

The visuals support the main README by separating three engineering stories:

```text
banner.png
01_lakehouse_architecture.png
02_bronze_silver_gold_flow.png
03_delta_capabilities_flow.png
```

## Assets

| Asset | Role |
|---|---|
| `banner.png` | README hero / first visual impression |
| `01_lakehouse_architecture.png` | Azure Databricks + ADLS Gen2 architecture overview |
| `02_bronze_silver_gold_flow.png` | Landing → Bronze → Silver → Gold processing flow |
| `03_delta_capabilities_flow.png` | Delta MERGE, transaction history, versioning, and Time Travel |

## Visual discipline

- Conceptual visuals explain architecture and engineering intent.
- Execution screenshots and validation proof remain under `evidence/`.
- Visuals must not imply capabilities outside the implemented MVP.
- Unity Catalog, Databricks Jobs, CI/CD deployment, streaming, and production monitoring remain documented future improvements rather than implemented claims.

## Portfolio consistency

The visual package is intentionally aligned with the broader Azure portfolio while preserving the Lakehouse identity of this repository:

- dark Azure-oriented visual language;
- clear technical hierarchy;
- one primary engineering idea per diagram;
- minimal decorative noise;
- strong readability at GitHub README width.

The visual package should support the technical documentation, not replace it.
