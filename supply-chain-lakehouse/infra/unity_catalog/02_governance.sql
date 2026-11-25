USE CATALOG supply_chain_dev;

-- 1) Groups & RBAC (least privilege)
GRANT USE CATALOG ON CATALOG supply_chain_dev TO `sc-data-engineers`, `sc-analysts`, `sc-data-scientists`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA bronze TO `sc-data-engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA silver TO `sc-data-engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA gold   TO `sc-data-engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `sc-data-scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `sc-analysts`, `sc-data-scientists`;

-- 2) Tags for classification / discoverability
ALTER TABLE silver.customer_order SET TAGS ('domain'='supply_chain','pii'='true','data_owner'='order-mgmt');
ALTER TABLE silver.supplier       SET TAGS ('domain'='supply_chain','pii'='false','data_owner'='procurement');
ALTER TABLE silver.customer_order ALTER COLUMN customer_email SET TAGS ('classification'='PII-email');

-- 3) Column mask on PII
CREATE OR REPLACE FUNCTION governance.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('sc-pii-readers') THEN email
            ELSE regexp_replace(email, '^[^@]+', '*****') END;

ALTER TABLE silver.customer_order ALTER COLUMN customer_email SET MASK governance.mask_email;

-- 4) Row filter: regional access to warehouses
CREATE OR REPLACE FUNCTION governance.warehouse_filter(country STRING)
RETURN is_account_group_member('sc-global-access')
    OR (is_account_group_member('sc-india-team') AND country = 'India')
    OR (is_account_group_member('sc-eu-team') AND country IN ('Netherlands','Sweden'));

ALTER TABLE silver.warehouse SET ROW FILTER governance.warehouse_filter ON (country);

-- 5) Documentation & constraints
COMMENT ON TABLE gold.shipment_otif IS 'On-Time-In-Full KPI per carrier. Owner: logistics-analytics';
ALTER TABLE silver.shipment ADD CONSTRAINT qty_non_negative CHECK (qty_shipped >= 0);

-- 6) Audit & lineage queries (system tables)
-- SELECT * FROM system.access.table_lineage WHERE target_table_full_name LIKE 'supply_chain_dev.gold.%';
-- SELECT * FROM system.access.audit WHERE service_name = 'unityCatalog' ORDER BY event_time DESC;
