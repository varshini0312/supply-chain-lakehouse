# Architecture

## Azure resources
| Resource | Purpose |
|---|---|
| Azure SQL Database | ERP/WMS OLTP source, CDC enabled |
| Azure Blob Storage | Vendor inbound files (ASN, invoices) |
| ADLS Gen2 (HNS) | Containers: `landing`, `lakehouse` (bronze/silver/gold managed by Unity Catalog) |
| Azure Data Factory | Orchestration + ingestion (Copy, Lookup, ForEach, Databricks Notebook) |
| Azure Databricks | PySpark processing, Delta Lake, Lakeflow, Unity Catalog |
| Azure Key Vault | Secrets (SQL connection strings, JDBC) |
| Azure DevOps | Repos (Git), Pipelines (CI/CD), Boards |

## Data flow
1. **Ingestion** – ADF pulls (a) dimension tables incrementally via watermark, (b) fact tables via SQL Server CDC change tables, (c) vendor files from Blob.
2. **Bronze** – Auto Loader streams landed Parquet into append-only Delta tables (raw, schema evolution on).
3. **Silver** – CDC events merged (`MERGE` / `apply_changes`), deduplicated, typed; supplier uses SCD2.
4. **Gold** – integrated marts (OTIF, inventory position, supplier scorecard, fulfillment).
5. **Governance** – Unity Catalog permissions, masks, row filters, tags, lineage, DQ results table.

## Delta Lake features used
MERGE, Change Data Feed, Z-ORDER/OPTIMIZE, auto-optimize, schema evolution, time travel, constraints, VACUUM (retain 7+ days).

## Environments
`dev` / `prod` catalogs (`supply_chain_dev`, `supply_chain_prod`), promoted via Azure DevOps with approvals.

## Incremental strategy summary
| Source | Method | Reason |
|---|---|---|
| supplier, product, warehouse | Watermark on `updated_at` | Small, rarely deleted |
| inventory, orders, shipments, PO | SQL Server CDC | Captures deletes and every change |
| Vendor files | File arrival + Auto Loader | Event/file based |
| Legacy DW | One-time JDBC migration | Historical backfill |
