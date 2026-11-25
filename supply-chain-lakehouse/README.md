# Supply Chain Lakehouse (Azure)

End-to-end **data migration, ingestion, integration and governance** project for a retail/manufacturing
**supply chain** (orders, shipments, inventory, suppliers, products, warehouses).

Operational data lives in **Azure SQL Database** (with **CDC** enabled). It is ingested by **Azure Data Factory (ADF)**
into **ADLS Gen2 / Azure Blob Storage**, processed in **Azure Databricks** with **PySpark**, **Delta Lake**
and **Lakeflow (Declarative Pipelines)** following the medallion pattern, and governed by **Unity Catalog**.
Code is versioned in **Git** and released through **Azure DevOps** CI/CD.

## Skills coverage map

| Skill | Where it is used |
|---|---|
| Azure Blob Storage | `adf/linkedService/ls_blob_landing.json` – landing/archive of vendor CSV/JSON files |
| ADLS Gen2 | `adf/linkedService/ls_adls_gen2.json`, medallion containers (bronze/silver/gold) |
| Azure SQL Database | `infra/sql/source/*.sql` – OLTP source with CDC enabled |
| Azure Data Factory (ADF) | `adf/` – pipelines, datasets, linked services, triggers |
| Azure Databricks | `databricks/` notebooks and `databricks.yml` job definitions |
| PySpark | `databricks/notebooks/*.py`, `src/sc_lakehouse/` |
| Python | `src/`, `tests/`, `scripts/` |
| Delta Lake | MERGE upserts, Change Data Feed, OPTIMIZE/VACUUM, SCD2 |
| CDC | SQL Server CDC -> ADF -> bronze -> `MERGE` into silver (`03_silver_cdc_merge.py`) |
| Lakeflow | `databricks/lakeflow/supply_chain_pipeline.py` (Declarative Pipelines + expectations, `apply_changes`) |
| Unity Catalog | `infra/unity_catalog/*.sql` – catalogs, external locations, grants, masks, tags |
| Data Governance | `docs/data_governance.md`, UC tags, column masks, row filters, lineage, DQ rules |
| Data Migration | `databricks/migration/` – legacy warehouse -> Delta one-time historical load |
| Data Ingestion | ADF copy activities + Databricks Auto Loader (`01_bronze_autoloader.py`) |
| Data Integration | Joining ERP, WMS and supplier feeds into conformed gold models |
| Data Pipeline | ADF master pipeline -> Databricks jobs -> Lakeflow |
| ETL / ELT | ETL for vendor files (transform before load), ELT for SQL sources (load raw, transform in Databricks) – see `docs/etl_vs_elt.md` |
| GIT | `.gitignore`, branching strategy (`docs/git_workflow.md`), PR template |
| Azure DevOps | `azure-pipelines.yml` (CI + multi-stage CD), `.azure-devops/pull_request_template.md` |
| SQL | Source DDL, UC DDL, gold-layer SQL views |

## Architecture

```
 Azure SQL DB (ERP/WMS, CDC)      Vendor files (CSV/JSON)       Legacy DW (migration)
          |                                 |                             |
          v                                 v                             v
   ADF Copy (incremental, CDC)      Azure Blob Storage (landing)    Databricks migration job
          |                                 |                             |
          +---------------> ADLS Gen2 (raw / bronze) <--------------------+
                                     |  Auto Loader + Lakeflow
                                     v
                         SILVER (Delta, CDC MERGE, DQ, SCD2)
                                     |
                                     v
                  GOLD (inventory_position, shipment_otif, supplier_scorecard)
                                     |
                             Unity Catalog governance
                                     |
                             BI / Power BI / ML
```

## Repository layout

```
adf/                    ADF JSON artifacts (pipelines, datasets, linked services, triggers)
databricks/notebooks/   Bronze / Silver / Gold PySpark notebooks
databricks/lakeflow/    Lakeflow Declarative Pipeline definition
databricks/migration/   Historical data migration
infra/sql/source/       Azure SQL source schema, CDC enablement, seed data
infra/unity_catalog/    Unity Catalog setup + governance SQL
src/sc_lakehouse/       Reusable PySpark / Python library
tests/                  Pytest unit tests
data/sample/            Small sample files
docs/                   Architecture, governance, Git, ETL vs ELT docs
azure-pipelines.yml     Azure DevOps CI/CD
databricks.yml          Databricks Asset Bundle (jobs & Lakeflow pipeline)
```

## Quick start

1. Provision resources (see `docs/architecture.md`): ADLS Gen2, Blob Storage, Azure SQL DB, Databricks (Unity Catalog enabled), ADF.
2. Run `infra/sql/source/01_schema.sql`, `02_enable_cdc.sql`, `03_seed.sql` on Azure SQL.
3. Run `infra/unity_catalog/01_catalog_setup.sql` then `02_governance.sql` in Databricks SQL.
4. Deploy ADF artifacts (`adf/`) via Azure DevOps or ADF Git integration.
5. Deploy Databricks bundle: `databricks bundle deploy -t dev`.
6. Trigger ADF pipeline `pl_master_supply_chain` (or wait for the scheduled trigger).
7. Run tests locally: `pip install -r requirements.txt && pytest`.

## Business outputs (gold)

- `gold.inventory_position` – on-hand, reserved, days-of-cover per product/warehouse
- `gold.shipment_otif` – On-Time-In-Full KPI per carrier/lane
- `gold.supplier_scorecard` – lead time, defect rate, fill rate per supplier
- `gold.order_fulfillment_fact` – order-to-delivery cycle time
