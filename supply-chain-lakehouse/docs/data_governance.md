# Data Governance

## Principles
- Single governance plane: **Unity Catalog** (catalog -> schema -> table, external locations, volumes).
- Least-privilege RBAC using Entra ID groups (`sc-data-engineers`, `sc-analysts`, `sc-data-scientists`, `sc-pii-readers`).
- Data owners and stewards tagged on every table (`data_owner`, `domain`).

## Controls implemented
| Control | Implementation |
|---|---|
| Access control | `GRANT` statements in `infra/unity_catalog/02_governance.sql` |
| PII protection | Column mask `governance.mask_email`, tag `classification=PII-email` |
| Regional restriction | Row filter `governance.warehouse_filter` |
| Data quality | `sc_lakehouse.dq`, Lakeflow expectations, `governance.dq_results` |
| Lineage | Unity Catalog lineage (`system.access.table_lineage`) |
| Audit | `system.access.audit`, `governance.pipeline_audit` |
| Retention | Delta VACUUM 7d+, bronze retained 13 months |
| Secrets | Azure Key Vault-backed secret scope, managed identity for storage |
| Change management | Git PRs + Azure DevOps approvals on prod |

## Classification
| Level | Examples | Handling |
|---|---|---|
| Public | Warehouse city | None |
| Internal | Inventory, PO | Role-based |
| Confidential/PII | Customer email | Masked by default |

## Stewardship RACI
Owner (business) approves access; Steward maintains definitions/tags; Engineers implement controls; Security audits quarterly.
