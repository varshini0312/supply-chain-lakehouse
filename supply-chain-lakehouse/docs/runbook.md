# Runbook
- **ADF failure**: Monitor -> Pipeline runs -> rerun failed activity. Watermark only advances on successful copy, so reruns are safe.
- **CDC gap**: if SQL CDC cleanup removed LSNs, run migration notebook for the affected table, then resume CDC.
- **DQ error**: check `governance.dq_results`; fix source or quarantine; rerun task `dq`.
- **Rollback a bad merge**: `RESTORE TABLE supply_chain_prod.silver.shipment TO VERSION AS OF <n>`.
