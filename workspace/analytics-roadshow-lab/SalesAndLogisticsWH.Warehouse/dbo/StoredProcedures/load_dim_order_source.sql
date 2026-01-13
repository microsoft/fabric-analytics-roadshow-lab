CREATE PROCEDURE dbo.load_dim_order_source (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'dim.order_source')
    
    /* Handle the unknown member */
    IF NOT EXISTS (SELECT * FROM dim.order_source WHERE order_source_ak = 'Unknown')
    INSERT INTO dim.order_source VALUES ('Unknown', 'Unknown', '1900-01-01', '12-31-2099')

    /* UPSERT the data from the lakehouse bronze tables */
    INSERT INTO dim.order_source
    SELECT DISTINCT
        source AS order_source_ak,
        source AS order_source_name,
        '1900-01-01' AS start_date,
        '12-31-2099' AS end_date
    FROM SalesAndLogisticsLH.silver.[order] AS o
    LEFT JOIN dim.order_source AS os
        ON o.source = os.order_source_ak
    WHERE
        os.order_source_ak IS NULL
        AND _processing_timestamp > @last_load_datetime
        AND _processing_timestamp <= @new_load_datetime

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'dim.order_source'
END