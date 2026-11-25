-- Unity Catalog structure: one catalog per environment, schema per medallion layer
-- Replace <storage-account> with your ADLS Gen2 account name.

CREATE STORAGE CREDENTIAL IF NOT EXISTS sc_adls_cred
  COMMENT 'Managed identity for supply chain ADLS Gen2';
-- (create via UI/CLI with the access connector managed identity)

CREATE EXTERNAL LOCATION IF NOT EXISTS sc_lake_loc
  URL 'abfss://lakehouse@<storage-account>.dfs.core.windows.net/supply_chain'
  WITH (STORAGE CREDENTIAL sc_adls_cred)
  COMMENT 'Supply chain lakehouse root on ADLS Gen2';

CREATE EXTERNAL LOCATION IF NOT EXISTS sc_landing_loc
  URL 'abfss://landing@<storage-account>.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL sc_adls_cred)
  COMMENT 'Raw landing zone (ADF + vendor files)';

CREATE CATALOG IF NOT EXISTS supply_chain_dev
  MANAGED LOCATION 'abfss://lakehouse@<storage-account>.dfs.core.windows.net/supply_chain/dev'
  COMMENT 'Supply chain DEV catalog';

USE CATALOG supply_chain_dev;
CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Raw, append-only, CDC records as ingested';
CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Cleansed, deduplicated, conformed Delta tables';
CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Business aggregates and KPIs';
CREATE SCHEMA IF NOT EXISTS governance COMMENT 'DQ results, audit, control tables';

CREATE VOLUME IF NOT EXISTS bronze.landing_files
  LOCATION 'abfss://landing@<storage-account>.dfs.core.windows.net/vendor_files';
