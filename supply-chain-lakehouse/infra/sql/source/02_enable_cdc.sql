-- Enable Change Data Capture on the database and key tables
EXEC sys.sp_cdc_enable_db;
GO
DECLARE @tables TABLE (name SYSNAME);
INSERT INTO @tables VALUES ('supplier'),('product'),('warehouse'),('inventory'),
                           ('purchase_order'),('customer_order'),('shipment');
DECLARE @t SYSNAME;
DECLARE c CURSOR FOR SELECT name FROM @tables;
OPEN c; FETCH NEXT FROM c INTO @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC sys.sp_cdc_enable_table
         @source_schema = N'scm', @source_name = @t,
         @role_name = NULL, @supports_net_changes = 1;
    FETCH NEXT FROM c INTO @t;
END
CLOSE c; DEALLOCATE c;
GO
-- CDC capture instances: cdc.scm_<table>_CT
-- __$operation: 1=delete, 2=insert, 3=update(before), 4=update(after)
