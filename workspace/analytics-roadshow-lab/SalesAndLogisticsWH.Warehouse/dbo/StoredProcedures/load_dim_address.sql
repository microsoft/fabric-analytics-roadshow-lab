CREATE PROCEDURE dbo.load_dim_address (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'dim.address')
    
    /* Handle the unknown member */
    IF NOT EXISTS (SELECT * FROM dim.address WHERE address_line_1 = 'Unknown' AND address_line_2 = 'Unknown' AND state_abbreviation = 'Unknown' AND zip_code = 'Unknown' AND country = 'Unknown')
    INSERT INTO dim.address VALUES ('Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0.0, 0.0)

    /* UPSERT the data from the lakehouse bronze tables */
    MERGE dim.address AS t
    USING 
        (
            SELECT
                destination_address AS address_line_1, 
                '' AS address_line_2,
                destination_city AS city,
                destination_state AS state_abbreviation,
                destination_zip_code AS zip_code,
                destination_country AS country,
                destination_latitude AS latitude,
                destination_longitude AS longitude
            FROM SalesAndLogisticsLH.silver.shipment
            WHERE
                _processing_timestamp > @last_load_datetime
                AND _processing_timestamp <= @new_load_datetime

            UNION

            SELECT
                origin_address AS address_line_1, 
                '' AS address_line_2,
                origin_city AS city,
                origin_state AS state_abbreviation,
                origin_zip_code AS zip_code,
                origin_country AS country,
                origin_latitude AS latitude,
                origin_longitude AS longitude
            FROM SalesAndLogisticsLH.silver.shipment
            WHERE
                _processing_timestamp > @last_load_datetime
                AND _processing_timestamp <= @new_load_datetime
        ) AS s
        ON t.address_line_1 = s.address_line_1
        AND t.city = s.city
        AND t.state_abbreviation = s.state_abbreviation
        AND t.zip_code = s.zip_code
    WHEN MATCHED THEN
        UPDATE
        SET
            address_line_1 = s.address_line_1,
            address_line_2 = s.address_line_2,
            city = s.city,
            state_abbreviation = s.state_abbreviation,
            zip_code = s.zip_code,
            country = s.country,
            latitude = s.latitude,
            longitude = s.longitude
    WHEN NOT MATCHED THEN 
        INSERT (address_line_1, address_line_2, city, state_abbreviation, zip_code, country, latitude, longitude)
        VALUES(s.address_line_1, s.address_line_2, s.city, s.state_abbreviation, s.zip_code, s.country, s.latitude, s.longitude);

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'dim.address'
END