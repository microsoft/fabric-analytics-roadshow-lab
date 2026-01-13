CREATE PROCEDURE dbo.load_fact_shipment (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'fact.shipment')
    
    /* Insert new records from the lakehouse bronze tables */
    INSERT INTO fact.shipment
    SELECT DISTINCT
        s.tracking_number
        ,o.order_number
        ,ISNULL(sd.date_sk, 19000101) AS ship_date_sk
        ,ISNULL(cdd.date_sk, 19000101) AS committed_delivery_date_sk
        ,ISNULL(dd.date_sk, 19000101) AS delivery_date_sk
        ,ISNULL(c.[customer_sk], -1) AS customer_sk
        ,ISNULL(oa.address_sk, -1) AS origin_address_sk
        ,ISNULL(da.address_sk, -1) AS destination_address_sk
        ,s.service_level
        ,GREATEST(DATEDIFF(DAY, CONVERT(DATE, s.committed_delivery_date), CONVERT(DATE, delivery.delivery_date)), 0) AS delivery_days_late
        ,s.[late_delivery_penalty_per_day]
        ,GREATEST(DATEDIFF(DAY, CONVERT(DATE, s.committed_delivery_date), CONVERT(DATE, delivery.delivery_date)), 0) * s.late_delivery_penalty_per_day AS late_delivery_penalty
        ,s.distance AS shipment_distance
        ,s.declared_value
        ,s.height
        ,s.width
        ,s.length
        ,s.volume
        ,s.weight
        ,s.is_fragile
        ,s.is_hazardous
        ,s.requires_refrigeration
    FROM SalesAndLogisticsLH.silver.shipment AS s
    LEFT JOIN SalesAndLogisticsLH.silver.[order] AS o
        ON s.order_id = o.order_id
    LEFT JOIN dim.customer AS c
        ON s.customer_id = c.customer_ak
    LEFT JOIN dim.address AS da
        ON s.destination_address = da.address_line_1
        AND s.destination_city = da.city
        AND s.destination_state = da.state_abbreviation
        AND s.destination_zip_code = da.zip_code
    LEFT JOIN dim.address AS oa
        ON  s.origin_address = oa.address_line_1
        AND s.origin_city = oa.city
        AND s.origin_state = oa.state_abbreviation
        AND s.origin_zip_code = oa.zip_code
    LEFT JOIN dim.date AS sd
        ON CONVERT(DATE, s.ship_date) = sd.date
    LEFT JOIN dim.date AS cdd
        ON CONVERT(DATE, s.committed_delivery_date) = cdd.date
    LEFT JOIN (
        SELECT 
            shipment_id,
            CONVERT(DATE, event_timestamp) AS delivery_date
        FROM SalesAndLogisticsLH.silver.shipment_scan_event
        WHERE event_type = 'Delivered'
    ) AS delivery
        ON s.shipment_id = delivery.shipment_id
    LEFT JOIN dim.date AS dd
        ON delivery.delivery_date = dd.date
    WHERE
        NOT EXISTS
            (
                SELECT
                    1
                FROM fact.shipment AS fs
                WHERE
                    s.tracking_number = fs.tracking_number
            )
        AND s._processing_timestamp > @last_load_datetime
        AND s._processing_timestamp <= @new_load_datetime

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'fact.shipment'
END