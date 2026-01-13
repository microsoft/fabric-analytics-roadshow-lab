CREATE PROCEDURE dbo.load_dim_facility (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
     /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'dim.facility')
    
    /* Handle the unknown member */
    IF NOT EXISTS (SELECT * FROM dim.facility WHERE facility_ak = 'Unknown')
    INSERT INTO dim.facility VALUES ('Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0.0, 0.0, '1900-01-01', '12-31-2099')

    /* UPSERT the data from the lakehouse bronze tables */
    MERGE dim.facility AS t
    USING 
        (
            SELECT
                facility_id AS facility_ak,
                facility_name,
                facility_type,
                address,
                city,
                [state] AS state_abbreviation,
                zip_code,
                country,
                latitude,
                longitude,
                '1900-01-01' AS start_date,
                '12-31-2099' AS end_date
            FROM SalesAndLogisticsLH.silver.facility
            WHERE 
                _processing_timestamp > @last_load_datetime
                AND _processing_timestamp <= @new_load_datetime
        ) AS s
        ON t.facility_ak = s.facility_ak
    WHEN MATCHED THEN
        UPDATE
        SET
            facility_ak         = s.facility_ak,
            facility_name       = s.facility_name,
            facility_type       = s.facility_type,
            address             = s.address,
            city                = s.city,
            state_abbreviation  = s.state_abbreviation,
            zip_code            = s.zip_code
    WHEN NOT MATCHED THEN 
        INSERT (facility_ak, facility_name, facility_type, address, city, state_abbreviation, zip_code, country, latitude, longitude, start_date, end_date)
        VALUES(s.facility_ak, s.facility_name, s.facility_type, s.address, s.city, s.state_abbreviation, s.zip_code, s.country, s.latitude, s.longitude, s.start_date, s.end_date);

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'dim.facility'
END