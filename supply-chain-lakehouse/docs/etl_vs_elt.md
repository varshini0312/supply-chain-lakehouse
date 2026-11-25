# ETL vs ELT in this project

**ELT (default)** – SQL Database sources are copied as-is to ADLS (ADF Copy), then transformed in Databricks using
PySpark and Delta Lake. This keeps raw history, allows reprocessing, and uses scalable compute.

**ETL (where needed)** – Vendor CSV files in Blob go through ADF with schema validation, skip-incompatible-rows and
type conversion *before* landing, because files arrive with inconsistent formats and must be rejected early.

| Aspect | ELT (SQL sources) | ETL (vendor files) |
|---|---|---|
| Transform location | Databricks | ADF Copy / mapping |
| Raw retained | Yes (bronze) | Validated copy + archive |
| Reprocessing | Easy | Re-drop file |
