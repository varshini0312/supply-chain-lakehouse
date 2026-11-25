-- Azure SQL Database: supply chain OLTP source
CREATE SCHEMA scm;
GO

CREATE TABLE scm.supplier (
    supplier_id      INT           NOT NULL PRIMARY KEY,
    supplier_name    NVARCHAR(200) NOT NULL,
    country          NVARCHAR(60),
    contact_email    NVARCHAR(200),
    rating           DECIMAL(3,2),
    updated_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE scm.product (
    product_id       INT           NOT NULL PRIMARY KEY,
    sku              NVARCHAR(50)  NOT NULL UNIQUE,
    product_name     NVARCHAR(200) NOT NULL,
    category         NVARCHAR(100),
    unit_cost        DECIMAL(12,2),
    supplier_id      INT           REFERENCES scm.supplier(supplier_id),
    updated_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE scm.warehouse (
    warehouse_id     INT           NOT NULL PRIMARY KEY,
    warehouse_name   NVARCHAR(100) NOT NULL,
    city             NVARCHAR(100),
    country          NVARCHAR(60),
    updated_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE scm.inventory (
    inventory_id     BIGINT        NOT NULL PRIMARY KEY,
    product_id       INT           NOT NULL REFERENCES scm.product(product_id),
    warehouse_id     INT           NOT NULL REFERENCES scm.warehouse(warehouse_id),
    qty_on_hand      INT           NOT NULL,
    qty_reserved     INT           NOT NULL DEFAULT 0,
    updated_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE scm.purchase_order (
    po_id            BIGINT        NOT NULL PRIMARY KEY,
    supplier_id      INT           NOT NULL REFERENCES scm.supplier(supplier_id),
    po_date          DATE          NOT NULL,
    expected_date    DATE,
    status           NVARCHAR(30)  NOT NULL,
    updated_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE scm.customer_order (
    order_id         BIGINT        NOT NULL PRIMARY KEY,
    customer_id      INT           NOT NULL,
    customer_email   NVARCHAR(200),
    product_id       INT           NOT NULL REFERENCES scm.product(product_id),
    warehouse_id     INT           NOT NULL REFERENCES scm.warehouse(warehouse_id),
    qty_ordered      INT           NOT NULL,
    order_ts         DATETIME2     NOT NULL,
    promised_date    DATE,
    status           NVARCHAR(30)  NOT NULL,
    updated_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE scm.shipment (
    shipment_id      BIGINT        NOT NULL PRIMARY KEY,
    order_id         BIGINT        NOT NULL REFERENCES scm.customer_order(order_id),
    carrier          NVARCHAR(80),
    ship_date        DATE,
    delivery_date    DATE,
    qty_shipped      INT,
    status           NVARCHAR(30),
    updated_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
-- Watermark control table used by ADF for incremental (non-CDC) loads
CREATE TABLE dbo.watermark (
    table_name       NVARCHAR(100) NOT NULL PRIMARY KEY,
    watermark_value  DATETIME2     NOT NULL
);
GO
CREATE PROCEDURE dbo.usp_update_watermark @table_name NVARCHAR(100), @value DATETIME2
AS
BEGIN
    MERGE dbo.watermark AS t
    USING (SELECT @table_name AS table_name, @value AS watermark_value) s
    ON t.table_name = s.table_name
    WHEN MATCHED THEN UPDATE SET watermark_value = s.watermark_value
    WHEN NOT MATCHED THEN INSERT VALUES (s.table_name, s.watermark_value);
END
GO
