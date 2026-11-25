USE CATALOG supply_chain_dev;

CREATE OR REPLACE VIEW gold.v_low_stock_alert AS
SELECT * FROM gold.inventory_position WHERE days_of_cover < 7;
