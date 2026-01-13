CREATE PROCEDURE dbo.load_fact_order (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'fact.order')
    
    /* Insert new records from the lakehouse bronze tables */
    INSERT INTO fact.[order]
    SELECT
        o.order_number,
        o.line_number AS order_line_number,
        ISNULL(d.date_sk, 19000101) AS order_date_sk,
        ISNULL(os.order_source_sk, -1) AS order_source_sk,
        ISNULL(c.customer_sk, -1) AS customer_sk,
        ISNULL(i.item_sk, -1) AS item_sk,
        o.quantity,
        o.unit_price,
        o.extended_price,
        o.net_weight,
        o.warranty_included
    FROM SalesAndLogisticsLH.silver.[order] AS o
    LEFT JOIN dim.order_source AS os
        ON o.source = os.order_source_ak
    LEFT JOIN dim.customer AS c
        ON o.customer_id = c.customer_ak
    LEFT JOIN dim.item AS i
        ON o.item_id = i.item_ak
    LEFT JOIN dim.[date] AS d
        ON CONVERT(DATE, o.order_date) = d.[date]
    WHERE
        NOT EXISTS
            (
                SELECT
                    1
                FROM fact.[order] AS fo
                WHERE
                    o.order_number = fo.order_number
                    AND o.line_number = fo.order_line_number
            )
        AND _processing_timestamp > @last_load_datetime
        AND _processing_timestamp <= @new_load_datetime
    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'fact.order'
END