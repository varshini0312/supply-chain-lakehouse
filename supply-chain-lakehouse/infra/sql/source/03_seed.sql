INSERT INTO scm.supplier (supplier_id, supplier_name, country, contact_email, rating) VALUES
(1,'Apex Components','India','sales@apex.example',4.5),
(2,'Nordic Metals','Sweden','info@nordic.example',4.2),
(3,'Pacific Plastics','Vietnam','hello@pacific.example',3.9);

INSERT INTO scm.product (product_id, sku, product_name, category, unit_cost, supplier_id) VALUES
(101,'SKU-BOLT-01','Steel Bolt M8','Hardware',0.25,1),
(102,'SKU-GEAR-07','Drive Gear','Mechanical',12.80,2),
(103,'SKU-CASE-03','Plastic Casing','Enclosures',3.10,3);

INSERT INTO scm.warehouse (warehouse_id, warehouse_name, city, country) VALUES
(1,'BLR-DC1','Bengaluru','India'),(2,'ROT-DC2','Rotterdam','Netherlands');

INSERT INTO scm.inventory (inventory_id, product_id, warehouse_id, qty_on_hand, qty_reserved) VALUES
(1,101,1,5000,300),(2,102,1,420,60),(3,103,2,1200,100);

INSERT INTO scm.customer_order (order_id, customer_id, customer_email, product_id, warehouse_id, qty_ordered, order_ts, promised_date, status) VALUES
(9001,501,'ravi@example.com',101,1,200,'2026-11-01T09:00:00','2026-11-05','SHIPPED'),
(9002,502,'anna@example.com',102,1,50,'2026-11-02T11:30:00','2026-11-06','SHIPPED');

INSERT INTO scm.shipment (shipment_id, order_id, carrier, ship_date, delivery_date, qty_shipped, status) VALUES
(7001,9001,'BlueDart','2026-11-02','2026-11-04',200,'DELIVERED'),
(7002,9002,'DHL','2026-11-03','2026-11-08',40,'DELIVERED');
