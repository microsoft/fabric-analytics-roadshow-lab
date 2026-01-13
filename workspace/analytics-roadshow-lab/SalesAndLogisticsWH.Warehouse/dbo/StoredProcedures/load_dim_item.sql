CREATE PROCEDURE dbo.load_dim_item (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'dim.item')
    
    /* Handle the unknown member */
    IF NOT EXISTS (SELECT * FROM dim.item WHERE item_ak = 'Unknown')
    INSERT INTO dim.item VALUES ('Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0, 'Unknown', 0, 0, 0, 0, 0, 0, 0, '1900-01-01', '12-31-2099')

    /* UPSERT the data from the lakehouse bronze tables */
    MERGE dim.item AS t
    USING 
        (
            SELECT
                item_id AS item_ak,
                sku,
                description AS item_description,
                brand,
                category,
                subcategory,
                material,
                nominal_size,
                end_connection,
                pressure_class,
                weight,
                cost,
                list_price,
                is_sdofcertified,
                structural_index,
                span_rating,
                '1900-01-01' AS start_date,
                '12-31-2099' AS end_date
            FROM SalesAndLogisticsLH.silver.item
            WHERE
                _processing_timestamp > @last_load_datetime
                AND _processing_timestamp <= @new_load_datetime
        ) AS s
        ON t.item_ak = s.item_ak
    WHEN MATCHED THEN
        UPDATE
        SET
            item_ak             = s.item_ak,
            sku                 = s.sku,
            item_description    = s.item_description,
            brand               = s.brand,
            category            = s.category,
            subcategory         = s.subcategory,
            material            = s.material
    WHEN NOT MATCHED THEN 
        INSERT (item_ak, sku, item_description, brand, category, subcategory, material, nominal_size, end_connection, pressure_class, weight, cost, list_price, is_sdofcertified, structural_index, span_rating, start_date, end_date)
        VALUES(s.item_ak, s.sku, s.item_description, s.brand, s.category, s.subcategory, s.material, s.nominal_size, s.end_connection, s.pressure_class, s.weight, s.cost, s.list_price, s.is_sdofcertified, s.structural_index, s.span_rating, s.start_date, s.end_date);

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'dim.item'
END